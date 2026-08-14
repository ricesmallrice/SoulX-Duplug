import re
import sys
import time
import soxr
import queue
import base64
import socket
import struct
import select
import logging
import threading
import uuid
import json
import asyncio
import numpy as np
from typing import Dict, Optional
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.staticfiles import StaticFiles
from starlette.websockets import WebSocketState

from clients.tts_client import IndexTTS_VLLM, Cosyvoice_Streaming_VLLM
from clients.llm_client import QwenLLM_stream
from clients.vad_client import TurnTaking
from modules.utils.backchannel_utils import check_backchannel

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

app = FastAPI()


# Headers required for SharedArrayBuffer
@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    response = await call_next(request)
    response.headers["Cross-Origin-Opener-Policy"] = "same-origin"
    response.headers["Cross-Origin-Embedder-Policy"] = "require-corp"
    return response


# Global event loop reference for thread-safe websocket sending
main_loop = None

# Remote relay subscribers: forward audio stream & control events to other machines
relay_connections = set()
relay_lock = threading.Lock()

# TCP 音频发送端（--tcp_target）：主动连接远端播放器并推送音频流与控制事件。
tcp_sender = None

# TCP frame format: [type:1B][len:4B][payload]
#   type=0 audio (payload = raw int16 PCM), type=1 control (payload = control code)
CTRL_CODE = {"stop_audio": 0, "pause_audio": 1, "resume_audio": 2}


class TcpAudioSender:
    """TCP 音频发送端：主动连接远端播放器（remote_player.py），可靠推送音频流与控制事件。

    帧格式: [type:1B][len:4B big-endian][payload]
      type=0 音频（payload = int16 PCM，整段一帧）
      type=1 控制（payload = 控制码，0=stop/1=pause/2=resume）
    断线后由后台线程自动重连；stop 控制帧可实现即时打断。
    """

    def __init__(self, target: str):
        host, port = target.rsplit(":", 1)
        self.addr = (host, int(port))
        self.sock = None
        self._lock = threading.Lock()
        # TCP 发送统计（低频日志）：累计帧数/字节数，约每 2 秒打印一次
        self._sent_frames = 0
        self._sent_bytes = 0
        self._log_ts = time.time()
        threading.Thread(target=self._maintain, daemon=True).start()
        # 启动时打印一次：--tcp_target 指定后 TCP 播放通道初始化完成
        logger.info(f"TCP channel enabled -> {target}")

    def _maintain(self):
        """保证连接存活：无连接则重连；对端关闭则丢弃并等待下次重连。"""
        while True:
            with self._lock:
                sock = self.sock
            if sock is None:
                try:
                    s = socket.create_connection(self.addr, timeout=5)
                    s.settimeout(3.0)  # 发送超时：半开连接时避免 sendall 永久阻塞卡死事件循环
                    with self._lock:
                        self.sock = s
                    # _maintain 线程重连成功时打印
                    logger.info(f"TCP player connected: {self.addr}")
                except OSError as e:
                    # 连接失败时打印，2 秒后由 _maintain 循环重试
                    logger.warning(f"TCP connect to {self.addr} failed: {e}, retry")
                    time.sleep(2)
                continue
            # 探测对端是否已关闭（可读则 recv 返回空）。
            # select 可能因 _send 并发 close 同一 socket 抛 OSError(EBADF)，此时按"对端关闭"处理走重连
            try:
                r, _, _ = select.select([sock], [], [], 1.0)
            except OSError:
                r = [sock]
            if r:
                try:
                    if not sock.recv(1):
                        raise OSError("player closed")
                except OSError:
                    with self._lock:
                        self.sock = None
                    try:
                        sock.close()
                    except OSError:
                        pass
                    # _maintain 探测到对端已关闭（recv 返回空）时打印
                    logger.info(f"TCP player disconnected: {self.addr}")

    def _send(self, ptype: int, payload: bytes) -> bool:
        """写入 TCP 播放连接；返回 True=已交给内核发送队列，False=未连接或发送失败被丢弃。"""
        pkt = struct.pack(">BI", ptype, len(payload)) + payload
        with self._lock:
            sock = self.sock
        if sock is None:
            return False  # 未连接：直接丢弃（音频断流 / stop 无需送达）
        try:
            sock.sendall(pkt)
            return True
        except OSError as e:
            with self._lock:
                self.sock = None
            try:
                sock.close()
            except OSError:
                pass
            # 发送音频/控制帧失败时打印；连接置空，后续由 _maintain 重连
            logger.info(f"TCP player send failed: {e}, will reconnect")
            return False

    def send_audio(self, data: bytes):
        self._send(0, data)
        # 低频日志：约每 2 秒打印一次发送统计，确认 TTS 音频确实推送到远端播放器
        self._sent_frames += 1
        self._sent_bytes += len(data)
        now = time.time()
        if now - self._log_ts >= 2.0:
            logger.info(
                f"[tcp] audio sent: {self._sent_frames} frames / "
                f"{self._sent_bytes} bytes in {now - self._log_ts:.1f}s"
            )
            self._sent_frames = 0
            self._sent_bytes = 0
            self._log_ts = now

    def send_event(self, event: str, data, reason: str = ""):
        code = CTRL_CODE.get(event)
        if code is None:
            return  # UI-only events are not forwarded
        sent = self._send(1, bytes([code]))
        suffix = f" (reason: {reason})" if reason else ""
        # 每次发送控制帧（stop/pause/resume）时打印；按真实发送结果打 sent / dropped，
        # reason 标明触发来源（如 new_utterance / barge_in(mic_tcp) / barge_in(browser)）
        logger.info(
            f"[tcp] control frame {'sent' if sent else 'dropped'}: "
            f"{event} (code={code}){suffix}"
        )

    def close(self):
        """主动断开与播放端的连接；播放端收到断连即清空缓冲，实现打断静音（不依赖 stop 控制帧）。"""
        with self._lock:
            sock, self.sock = self.sock, None
        if sock is not None:
            try:
                sock.close()
            except OSError:
                pass
            # interrupt() 主动断开播放连接时打印（播放端收到断连即清缓冲静音）
            logger.info("TCP player connection dropped (interrupt)")


@app.on_event("startup")
async def startup_event():
    global main_loop
    main_loop = asyncio.get_running_loop()


class Config:
    """Global configuration constants."""

    SAMPLE_RATE = 16000
    VAD_POOL_SIZE = 10
    PORT = 55556


class ChatSession:
    """Manages the full lifecycle of a single user session."""

    def __init__(self, client_id, vad_instance, websocket: Optional[WebSocket]):
        # websocket 在 headless 模式下为 None（无浏览器连接，音频走 TCP 远端播放器）
        self.client_id = client_id
        self.vad = vad_instance
        self.websocket = websocket
        self.lock = threading.Lock()
        self.vad_lock = threading.Lock()  # 序列化 VAD 调用（浏览器麦克风 / TCP 远端麦克风）
        self._stop_event = threading.Event()  # Internal event to signal interruption
        self.is_active = True
        self.uses_remote_mic = False  # 是否使用 TCP 转发来的远端麦克风

        # Audio config
        self.input_sample_rate = Config.SAMPLE_RATE

        # State for pending message commitment (handling interruption)
        self.pending_message = None
        self.pending_audio_duration = 0.0
        self.pending_start_time = None
        self.interruption_time = None

    @property
    def stop_event(self):
        return self._stop_event

    def interrupt(self, reason: str = ""):
        """Interrupts current inference or audio playback."""
        self._stop_event.set()
        # 中断正在进行的 TTS 请求：关闭连接让 6006 服务端检测到断开，
        # 在句级检查点停止剩余合成，释放 GPU slot，避免新一轮 TTS 排队
        if tts is not None:
            try:
                tts.abort()
            except Exception:
                pass
        emit_to_room(self.client_id, "stop_audio", {"message": "interrupt"})
        emit_to_room(self.client_id, "circle_status", {"status": "LISTENING"})
        # 向远端播放器发送 stop 控制帧（type=1, code=0），播放端清空缓冲立即静音。
        # 不断开 TCP 连接：打断后下一轮 TTS 可直接复用连接推送，避免断连/重连时序问题
        if tcp_sender is not None:
            tcp_sender.send_event("stop_audio", None, reason=reason)

    def pause(self):
        """Pauses audio playback."""
        emit_to_room(self.client_id, "pause_audio", {"message": "pause"})
        emit_to_room(self.client_id, "circle_status", {"status": "LISTENING"})

    def reset_interrupt(self):
        """Creates a new stop event for the next processing cycle, leaving the old one set."""
        self._stop_event = threading.Event()


class SessionManager:
    """Thread-safe manager for active chat sessions."""

    def __init__(self):
        self.sessions: Dict[str, ChatSession] = {}
        self._lock = threading.Lock()

    def create_session(self, client_id, vad_instance, websocket: WebSocket):
        with self._lock:
            session = ChatSession(client_id, vad_instance, websocket)
            self.sessions[client_id] = session
            return session

    def get_session(self, client_id) -> Optional[ChatSession]:
        return self.sessions.get(client_id)

    def remove_session(self, client_id):
        with self._lock:
            return self.sessions.pop(client_id, None)


class VADModelPool:
    """Object pool for VAD instances to optimize memory and startup time."""

    def __init__(self, model_cls, size=Config.VAD_POOL_SIZE):
        self.pool = queue.Queue(maxsize=size)
        # 进程启动时打印一次：开始加载 VAD 模型池（模型加载慢，此后可能长时间无输出）
        logger.info(f"Initializing VAD Pool with {size} instances...")
        for _ in range(size):
            # Initialize instances without specific callbacks (bound during acquisition)
            instance = model_cls(
                status_callback=None, transcription_callback=None, circle_callback=None
            )
            self.pool.put(instance)

    def acquire(self):
        """Retrieve a VAD instance from the pool."""
        return self.pool.get(block=True)

    def release(self, instance):
        """Reset and return the instance back to the pool."""
        if hasattr(instance, "reset"):
            instance.reset()
        # Clear callbacks to prevent memory leaks or stale context
        instance.status_callback = None
        instance.transcription_callback = None
        instance.circle_callback = None
        self.pool.put(instance)


# ==== Global Singleton Initialization ====
vad_pool = VADModelPool(TurnTaking)
session_manager = SessionManager()
llm = QwenLLM_stream()
# tts = Cosyvoice_Streaming_VLLM()
tts = IndexTTS_VLLM()
asr = None  # Placeholder for ASR client if transcription isn't handled within VAD
# 全局单例创建完成（VAD 池 / 会话管理 / LLM / TTS）；此打印后服务才可用
print("System initialized: VAD Pool, LLM client, TTS client ready.")


def emit_to_room(client_id, event, data):
    """Helper function to safely emit WebSocket messages to a specific client."""
    session = session_manager.get_session(client_id)
    if not session or not main_loop:
        return

    ws = session.websocket

    # Helper wrapper to run async send in the main loop
    async def _send():
        # 1. Send to the local session websocket（headless 模式下 websocket 为 None，无浏览器可发）
        if ws is not None and ws.client_state == WebSocketState.CONNECTED:
            try:
                if event == "audio_chunk":
                    # Audio data: send raw bytes
                    await ws.send_bytes(data)
                else:
                    # Text/JSON data
                    message = json.dumps({"event": event, "data": data})
                    await ws.send_text(message)
            except Exception as e:
                # 向浏览器 WS 发送消息失败时打印（音频或 JSON 事件）
                logger.error(f"Failed to send to {client_id}: {e}")

        # 2. Forward to remote relay subscribers (raw bytes for audio, JSON for events)
        await _relay_forward(event, data)

    asyncio.run_coroutine_threadsafe(_send(), main_loop)


async def _relay_forward(event, data):
    """Forward audio chunks / control events to the remote (TCP or WebSocket relay)."""
    global tcp_sender

    # TCP 通道：可靠推送音频；打断不发 stop 控制帧，由 interrupt() 断开连接触发播放端清队列
    if tcp_sender is not None:
        if event == "audio_chunk":
            tcp_sender.send_audio(data)
        return

    if not relay_connections:
        return

    with relay_lock:
        targets = list(relay_connections)

    failed = []
    for rws in targets:
        try:
            if rws.client_state != WebSocketState.CONNECTED:
                failed.append(rws)
                continue
            if event == "audio_chunk":
                await rws.send_bytes(data)
            else:
                message = json.dumps({"event": event, "data": data})
                await rws.send_text(message)
        except Exception as e:
            # 旁听转发（/ws/relay 订阅端）发送失败时打印，并将该连接移出订阅集合
            logger.error(f"Relay send failed: {e}")
            failed.append(rws)

    if failed:
        with relay_lock:
            for rws in failed:
                relay_connections.discard(rws)


def pipeline_worker(client_id, audio_segment, sample_rate):
    """
    Main processing pipeline: ASR -> LLM -> TTS.
    Runs in a background thread for each detected utterance.
    """
    session = session_manager.get_session(client_id)
    if not session:
        return

    try:
        # ============================================================
        # [E2E 计时起点] 用户说完、utterance 到达 55556 的时刻。
        # 之后依次执行（耗时性质）：
        #   1. ASR 文本确认（即时，识别已在 8000 VAD 侧完成）
        #   2. 上轮 pending 消息结算（毫秒级）
        #   3. interrupt(new_utterance) 打断上轮 + reset（毫秒级）
        #   4. ASR result 日志 + user 消息入队
        #   5. LLM 请求 → 首个 token（TTFT，大耗时①）
        #   6. 流式循环：LLM chunk → TTS 合成 → 推音频
        #      首个音频帧产出 = E2E First Audio（大耗时②）
        #   7. 收尾：LLM/TTS Total → 被打断截断提交 / 未打断存 pending → E2E Complete
        # 两个 E2E 指标均以 t_pipeline_start 为基准：
        #   E2E First Audio = 首帧音频 - t_pipeline_start（用户感知"首声"等待）
        #   E2E Complete    = 处理完成 - t_pipeline_start（完整一轮）
        # ============================================================
        t_pipeline_start = time.time()  # [Timing] Pipeline start for E2E latency
        # 1. ASR Phase (Automatic Speech Recognition)
        if asr is None:
            # Fallback if ASR is handled by internal TurnTaking module
            asr_text = (
                audio_segment if isinstance(audio_segment, str) else "Voice Detected"
            )
        else:
            asr_text = asr.recognize(audio_segment, sample_rate)

        if not asr_text.strip():
            return

        # # 2. Backchannel Detection (vad already handles this, but double-check here)
        # # Checks for filler words or short backchannels that shouldn't trigger a full response
        # if check_backchannel(asr_text):
        #     emit_to_room(client_id, "resume_audio", {"message": "backchannel detected"})
        #     return

        # Process Pending Message from previous turn if any (before adding new user message)
        with session.lock:
            if session.pending_message:
                final_msg = session.pending_message
                # Check if an interruption occurred during the playback of the previous complete message
                if (
                    session.interruption_time
                    and session.pending_start_time
                    and session.pending_audio_duration > 0
                ):
                    elapsed = max(
                        session.interruption_time - session.pending_start_time, 0
                    )
                    ratio = min(elapsed / session.pending_audio_duration, 1.0)
                    cutoff = int(len(final_msg) * ratio)
                    final_msg = final_msg[:cutoff]
                    # 每轮新 utterance 开始、提交上轮 pending 消息时打印；
                    # 上轮回复播放中途被打断 → 按已播比例截断后补进 LLM 历史
                    logger.info(
                        f"[{client_id}] Previous turn, Ratio: {ratio:.2f}, Truncated: {final_msg}"
                    )
                else:
                    # 上轮回复完整播放（未被打断），整段补进 LLM 历史
                    logger.info(f"[{client_id}] Previous turn completed fully.")

                llm.add_message(client_id, "assistant", final_msg)

            # Clear pending state
            session.pending_message = None
            session.pending_audio_duration = 0.0
            session.pending_start_time = None
            session.interruption_time = None

        # 3. Preparation for Response
        session.interrupt(reason="new_utterance")  # Signal previous threads to stop
        session.reset_interrupt()  # Create a fresh event for this new thread

        # Capture the specific stop_event for this execution cycle
        current_stop_event = session.stop_event

        if current_stop_event.is_set():
            return

        # 每轮 utterance 的最终文本确定后打印（源头：8000 云端/本地 ASR 结果），随后进入 LLM
        logger.info(f"[{client_id}] ASR result: {asr_text}")
        emit_to_room(client_id, "user_transcription", {"text": asr_text})

        llm.add_message(client_id, "user", asr_text)
        t_llm_start = time.time()           # [Timing] LLM generation start (for TTFT)
        t_llm_first_chunk = None            # [Timing] Will be set on first LLM chunk
        t_tts_first_audio = None            # [Timing] Will be set on first TTS audio chunk
        llm_reply_gen = llm.generate_with_history(
            client_id, stop_event=current_stop_event
        )
        # logger.info(llm.get_session(client_id))

        # 4. LLM & TTS Streaming Processing
        # Iterate over LLM response chunks and synthesize audio on the fly
        message_to_add = ""
        interrupted = False
        first_emit_time = None
        total_audio_duration = 0.0

        for i, chunk in enumerate(
            llm_reply_gen
            if hasattr(llm_reply_gen, "__iter__") and not isinstance(llm_reply_gen, str)
            else [llm_reply_gen]
        ):
            if current_stop_event.is_set():
                interrupted = True
                if session.interruption_time:
                    # 打断发生时打印：从 barge-in 到达（interruption_time 记录）到 LLM 流式循环检测到的延迟
                    logger.info(  # [Timing] From barge-in arrival to LLM loop detection
                        f"[{client_id}] Interrupt Latency (LLM loop): {time.time() - session.interruption_time:.3f}s"
                    )
                break

            # 每收到一个 LLM 流式文本块时打印（TTFT 之后持续输出）
            logger.info(f"[{client_id}] LLM Chunk: {chunk}")
            if t_llm_first_chunk is None:
                t_llm_first_chunk = time.time()  # [Timing] First LLM token received
                # 第一个 LLM 块到达时打印一次：从 LLM 请求发出（t_llm_start）到首个 token 的耗时
                logger.info(  # [Timing] Time To First Token from LLM
                    f"[{client_id}] LLM TTFT: {t_llm_first_chunk - t_llm_start:.3f}s"
                )

            # Send LLM text chunk immediately
            emit_to_room(client_id, "text_response", {"text": chunk})

            # Accumulate text before TTS loop to ensure current chunk is considered
            message_to_add += chunk

            # Synthesize text chunk to speech (Iterate over int16 pcm chunks)
            for wav_chunk in tts.synthesize(chunk, streaming=True):
                if current_stop_event.is_set():
                    interrupted = True
                    if session.interruption_time:
                        # 打断发生时打印：从 barge-in 到 TTS 合成循环检测到的延迟
                        logger.info(  # [Timing] From barge-in arrival to TTS loop detection
                            f"[{client_id}] Interrupt Latency (TTS loop): {time.time() - session.interruption_time:.3f}s"
                        )
                    break

                if first_emit_time is None:
                    first_emit_time = time.time()
                if t_tts_first_audio is None:
                    t_tts_first_audio = time.time()  # [Timing] First TTS audio emitted
                    # 第一帧 TTS 音频产出时打印一次：TTS 首音频耗时（相对首个 LLM token）+ E2E 首音频耗时（相对本轮开始）
                    logger.info(  # [Timing] TTS first chunk latency + E2E first audio
                        f"[{client_id}] TTS First Audio: {t_tts_first_audio - t_llm_first_chunk:.3f}s"
                        f" | E2E First Audio: {t_tts_first_audio - t_pipeline_start:.3f}s"
                    )

                # Calculate audio duration: bytes / (sample_rate * channels * bytes_per_sample)
                # Assuming 24k sample rate, 1 channel, 16-bit (2 bytes) = 48000 bytes/sec
                total_audio_duration += len(wav_chunk) / 48000.0

                emit_to_room(client_id, "audio_chunk", wav_chunk)

            if current_stop_event.is_set():
                interrupted = True
                if session.interruption_time:
                    # 打断发生时打印：一个 LLM chunk 的 TTS 合成完后才检测到 stop（post-TTS）的延迟
                    logger.info(  # [Timing] From barge-in arrival to post-TTS detection
                        f"[{client_id}] Interrupt Latency (post-TTS): {time.time() - session.interruption_time:.3f}s"
                    )
                break

        # 本轮所有 LLM 流式块接收完时打印：LLM 总耗时（从首个 token 到结束）
        if t_llm_first_chunk is not None:
            logger.info(
                f"[{client_id}] LLM Total: {time.time() - t_llm_first_chunk:.3f}s"
            )
        # 本轮所有 TTS 音频合成完时打印：TTS 总耗时（从首音频到结束）
        if t_tts_first_audio is not None:
            logger.info(
                f"[{client_id}] TTS Total: {time.time() - t_tts_first_audio:.3f}s"
            )

        if interrupted:
            # If interrupted mid-stream, calculate truncation immediately using current time
            if first_emit_time and total_audio_duration > 0:
                elapsed_time = max(session.interruption_time - first_emit_time, 0)
                ratio = min(elapsed_time / total_audio_duration, 1.0)
                cutoff_length = int(len(message_to_add) * ratio)
                message_to_add = message_to_add[:cutoff_length]
                # 本轮被用户打断时打印：按已播放比例截断回复文本，立即提交进 LLM 历史
                logger.info(
                    f"[{client_id}] Interrupted mid-stream. Ratio: {ratio:.2f}. Truncated message: {message_to_add}"
                )
            else:
                message_to_add = ""

            # Commit immediately as this pipeline execution is dead
            llm.add_message(client_id, "assistant", message_to_add)

            # Ensure no pending state is left over
            with session.lock:
                session.pending_message = None
                session.pending_audio_duration = 0.0
                session.pending_start_time = None
                session.interruption_time = None

        else:
            # Pipeline finished successfully, but user might interrupt later while audio is playing.
            # Do NOT commit to LLM yet. Save to session pending state.
            with session.lock:
                session.pending_message = message_to_add
                session.pending_audio_duration = total_audio_duration
                session.pending_start_time = first_emit_time
                session.interruption_time = None

        # 每轮 pipeline 结束时打印：从 utterance 到达（t_pipeline_start）到处理完成的端到端总耗时
        logger.info(  # [Timing] Total E2E pipeline time (start → finish)
            f"[{client_id}] E2E Complete: {time.time() - t_pipeline_start:.3f}s"
        )

    except Exception as e:
        # pipeline 任一步骤抛异常时打印（含 traceback），用于定位 ASR/LLM/TTS 环节故障
        logger.error(f"Error in pipeline for {client_id}: {e}", exc_info=True)


# ==== Remote Mic Receiver (TCP) ====
# 远端麦克风转发过来（见 remote_mic.py），与浏览器麦克风走完全相同的 VAD -> 打断/发言 处理路径。
# TCP 帧格式: [type:1B][len:4B][payload]，type=0 音频（推荐，可靠不丢块）
MIC_TCP_HEADER = 5  # [type:1B][len:4B]
mic_audio_queue = queue.Queue(maxsize=64)
# 麦克风模式：browser=浏览器本机麦克风（默认）；remote=使用 TCP 转发来的远端麦克风
# 由启动参数 --mic_tcp_port 决定，随 connect_ack 后的 mic_mode 事件告知前端
MIC_MODE = "browser"

# headless 模式：无浏览器，自动创建常驻会话（见 start_headless_session）
HEADLESS = False


def start_headless_session():
    """headless 模式：创建常驻 system 会话（无 WebSocket），
    常驻持续对话（LLM 上下文累积），音频输入走远端麦克风、输出走 TCP 远端播放器。"""
    client_id = "system"
    vad_instance = vad_pool.acquire()
    session = session_manager.create_session(client_id, vad_instance, None)
    session.is_active = True
    session.uses_remote_mic = True
    # headless 模式启动时打印一次：常驻 system 会话创建完成，持续对话并接收远端麦克风
    logger.info(
        f"[{client_id}] Headless session initialized (persistent, remote mic)"
    )


def pick_mic_session():
    """选择接收远端麦克风音频的会话：headless 模式固定用常驻 system 会话；否则优先显式启用远端麦克风的会话。"""
    if HEADLESS:
        s = session_manager.get_session("system")
        if s is not None and s.is_active:
            return s
    with session_manager._lock:
        candidates = list(session_manager.sessions.values())
    for s in reversed(candidates):
        if s.is_active and s.uses_remote_mic:
            return s
    for s in reversed(candidates):
        if s.is_active:
            return s
    return None


def emit_remote_mic_waveform(client_id, audio):
    """把远端麦克风音频块转发给浏览器用于画波形（确认远端音频链路/可作调试）。"""
    session = session_manager.get_session(client_id)
    if not session or not main_loop:
        return
    ws = session.websocket
    if ws is None:
        return  # headless：无浏览器，无需画波形

    async def _send():
        if ws.client_state == WebSocketState.CONNECTED:
            try:
                data = base64.b64encode(
                    (np.clip(audio, -1.0, 1.0) * 32768.0).astype(np.int16).tobytes()
                ).decode()
                await ws.send_text(
                    json.dumps(
                        {"event": "remote_mic_waveform", "data": data}
                    )
                )
            except Exception:
                pass

    asyncio.run_coroutine_threadsafe(_send(), main_loop)


def mic_vad_worker():
    """从队列取远端麦克风音频送入 VAD，处理结果与浏览器麦克风一致。"""
    dropped = 0
    wave_cnt = 0
    while True:
        chunk = mic_audio_queue.get()
        session = pick_mic_session()
        if session is None:
            dropped += 1
            # 远端麦克风音频没有归属会话时打印（每丢 50 块一次）：
            # 典型原因 = 未带 --headless 且无浏览器会话
            if dropped % 50 == 1:
                logger.warning(
                    f"[mic-tcp] no active session, dropping mic audio "
                    f"({dropped} dropped so far)"
                )
            continue
        dropped = 0
        # 每 2 帧转发一次波形给浏览器（约 25Hz），页面可实时看到远端麦克风输入
        wave_cnt += 1
        if wave_cnt % 2 == 0:
            emit_remote_mic_waveform(session.client_id, chunk)
        try:
            with session.vad_lock:
                segment = session.vad.process(chunk)
            if segment is not None:
                if isinstance(segment, list) and segment[0] is None:
                    # Barge-in（打断当前回复）
                    # 远端麦克风在 AI 说话期间检测到用户插话（VAD 判 nonidle）时打印，触发打断
                    logger.info(
                        f"[{session.client_id}] Barge-in detected at VAD nonidle (mic-tcp)"
                    )
                    if session.interruption_time is None:
                        session.interruption_time = time.time()
                    session.interrupt(reason="barge_in(mic_tcp)")
                else:
                    # 远端麦克风说完一句（VAD 返回 speak 文本）时打印，随后开 pipeline_worker 处理
                    logger.info(
                        f"[{session.client_id}] VAD Process (mic-tcp)"
                        f" | Utterance: {segment}"
                    )
                    threading.Thread(
                        target=pipeline_worker,
                        args=(session.client_id, segment, Config.SAMPLE_RATE),
                        daemon=True,
                    ).start()
        except Exception as e:
            # 远端麦克风音频送 VAD 异常时打印（含 traceback）
            logger.error(f"[mic-tcp] VAD process failed: {e}", exc_info=True)


def _feed_mic_audio(payload: bytes):
    """把一段 int16 PCM 压成 float32 放入队列；处理不过来时丢弃最旧数据保持实时。"""
    if not payload or len(payload) % 2 != 0:
        return
    audio = np.frombuffer(payload, dtype=np.int16).astype(np.float32) / 32768.0
    try:
        mic_audio_queue.put_nowait(audio)
    except queue.Full:
        # VAD 处理不过来时丢弃最旧的数据，保持实时
        try:
            mic_audio_queue.get_nowait()
            mic_audio_queue.put_nowait(audio)
        except queue.Empty:
            pass


def mic_tcp_receiver(port):
    """接收远端麦克风 TCP 音频流，帧格式 [type:1B][len:4B][payload]。"""
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("0.0.0.0", port))
    srv.listen(4)
    # 启动时打印一次：mic_tcp_receiver 线程开始监听远端麦克风端口
    logger.info(f"[mic-tcp] listening on 0.0.0.0:{port} for remote microphone")
    while True:
        try:
            conn, addr = srv.accept()
        except OSError:
            continue
        # 远端麦克风（remote_mic.py）建立 TCP 连接时打印
        logger.info(f"[mic-tcp] mic client connected: {addr}")
        threading.Thread(target=_mic_tcp_client, args=(conn,), daemon=True).start()


def _mic_tcp_client(conn):
    """读取单个 TCP 连接上的麦克风音频帧；连接关闭/异常即退出。"""
    try:
        conn.settimeout(30.0)  # 30s 无数据视为连接异常
        buf = b""
        while True:
            while len(buf) < MIC_TCP_HEADER:
                data = conn.recv(4096)
                if not data:
                    return
                buf += data
            ptype = buf[0]
            plen = struct.unpack(">I", buf[1:5])[0]
            buf = buf[5:]
            while len(buf) < plen:
                data = conn.recv(4096)
                if not data:
                    return
                buf += data
            payload = buf[:plen]
            buf = buf[plen:]
            if ptype == 0:
                _feed_mic_audio(payload)
    except (socket.timeout, OSError):
        pass
    finally:
        try:
            conn.close()
        except OSError:
            pass
        # 远端麦克风连接断开/超时退出时打印
        logger.info("[mic-tcp] mic client disconnected")


# ==== WebSocket Endpoint & Processing ====
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    client_id = str(uuid.uuid4())
    # 浏览器打开页面建立 WebSocket 时打印（client_id 为该会话唯一标识）
    logger.info(f"New connection: {client_id}")

    session = None

    try:
        # Initial Handshake / Setup
        emit_to_room(
            client_id,
            "vad_loading",
            {"state": "loading", "message": "Acquiring model resources..."},
        )

        # We need a session object to put in session_manager before emit_to_room works fully.
        loop = asyncio.get_running_loop()
        vad_instance = await loop.run_in_executor(None, vad_pool.acquire)

        # Bind Callbacks
        vad_instance.status_callback = lambda s, m: emit_to_room(
            client_id, "vad_status", {"state": s, "message": m}
        )
        vad_instance.transcription_callback = lambda t: emit_to_room(
            client_id, "user_transcription", {"text": t}
        )
        vad_instance.circle_callback = lambda s: emit_to_room(
            client_id, "circle_status", {"status": s}
        )

        session = session_manager.create_session(client_id, vad_instance, websocket)
        # 远端麦克风模式下，本会话的 VAD 输入来自 TCP 转发，而非浏览器麦克风
        session.uses_remote_mic = MIC_MODE == "remote"

        # Now emission works via session_manager lookups
        emit_to_room(client_id, "connect_ack", {"client_id": client_id})
        # 告知前端麦克风模式：带 --mic_tcp_port 时用远端麦克风，否则用浏览器麦克风
        emit_to_room(client_id, "mic_mode", {"mode": MIC_MODE})
        # 会话初始化时打印：告知前端当前麦克风模式（browser=浏览器麦克风 / remote=远端麦克风）
        logger.info(f"[{client_id}] mic_mode sent: {MIC_MODE}")
        emit_to_room(
            client_id,
            "vad_loading",
            {"state": "ready", "message": "Model loaded, ready to experience"},
        )
        # 会话资源就绪时打印（VAD 实例获取成功、回调绑定完成）
        logger.info(f"Session initialized for {client_id}")

        while True:
            # Receive Message
            try:
                message = await websocket.receive()
            except (WebSocketDisconnect, RuntimeError):
                # RuntimeError: Starlette 在收到 disconnect 后再调 receive 会抛
                # "Cannot call receive once a disconnect message has been received"
                # 属客户端断开时的正常竞态，按断开处理即可
                # 浏览器断开连接时打印（含 Starlette receive 竞态导致的伪断开）
                logger.info(f"Client disconnected: {client_id}")
                break

            if "bytes" in message and message["bytes"]:
                # 远端麦克风模式（--mic_tcp_port）下忽略浏览器采集的音频，
                # 只用 TCP 转发的远端麦克风，避免双路输入
                if MIC_MODE == "remote":
                    continue
                # Binary Audio Data
                _t_vad_recv = time.time()  # [Timing] Audio bytes received
                data = message["bytes"]
                # Convert buffer to float32
                audio_chunk = (
                    np.frombuffer(data, dtype=np.int16).astype(np.float32) / 32768.0
                )

                current_sr = session.input_sample_rate
                if current_sr != Config.SAMPLE_RATE:
                    audio_chunk = soxr.resample(
                        audio_chunk, current_sr, Config.SAMPLE_RATE, quality="VHQ"
                    )

                with session.vad_lock:
                    segment = session.vad.process(audio_chunk)
                _t_vad_done = time.time()  # [Timing] VAD processing completed

                if segment is not None:
                    if isinstance(segment, list) and segment[0] is None:
                        # Barge-in
                        # 浏览器麦克风在 AI 说话期间检测到用户插话时打印，触发打断
                        logger.info(f"[{client_id}] Barge-in detected at VAD nonidle")
                        if session.interruption_time is None:
                            session.interruption_time = time.time()
                        session.interrupt(reason="barge_in(browser)")
                    else:
                        # 浏览器麦克风说完一句时打印：VAD 往返耗时（音频进→文本出）+ 完整语句
                        logger.info(  # [Timing] VAD roundtrip: audio in → utterance out
                            f"[{client_id}] VAD Process: {_t_vad_done - _t_vad_recv:.3f}s"
                            f" | Utterance: {segment}"
                        )
                        # Complete Utterance
                        threading.Thread(
                            target=pipeline_worker,
                            args=(client_id, segment, Config.SAMPLE_RATE),
                            daemon=True,
                        ).start()

            elif "text" in message and message["text"]:
                # JSON Control Message
                try:
                    payload = json.loads(message["text"])
                    event = payload.get("event")

                    if event == "duplex_stop":
                        if session.interruption_time is None:
                            session.interruption_time = time.time()
                        session.stop_event.set()
                        session.reset_interrupt()
                        # 前端发送 duplex_stop 手动停止时打印
                        logger.info(f"Session manually stopped by client: {client_id}")

                    elif event == "config_audio":
                        # Client sending sample rate configuration
                        sr_data = payload.get("data", {})
                        # 前端上报音频配置（采样率/麦克风来源）时打印
                        logger.info(f"[{client_id}] config_audio received: {sr_data}")
                        sr = sr_data.get("sample_rate")
                        if sr:
                            session.input_sample_rate = int(sr)
                            # 采样率配置生效时打印
                            logger.info(f"[{client_id}] Sample rate set to {sr}")
                        # 标记使用 TCP 转发的远端麦克风，浏览器自身不再采集麦克风
                        if sr_data.get("source") == "remote_mic":
                            session.uses_remote_mic = True
                            # 前端声明使用远端麦克风（source=remote_mic）时打印
                            logger.info(f"[{client_id}] Remote mic enabled")

                except json.JSONDecodeError:
                    # 前端文本消息不是合法 JSON 时打印（可忽略的干扰消息）
                    logger.warning(f"[{client_id}] Received invalid JSON")

    except WebSocketDisconnect:
        # 会话正常断开时打印（浏览器关闭页面/断网）
        logger.info(f"Client disconnected: {client_id}")
    except Exception as e:
        # WS 处理循环抛未捕获异常时打印（含 traceback）
        logger.error(f"WebSocket error: {e}", exc_info=True)
    finally:
        if session:
            session.is_active = False
            session.stop_event.set()
            vad_pool.release(session.vad)
            session_manager.remove_session(client_id)
        # 会话清理完成时打印：标记非活跃、释放 VAD 实例回池、移除会话
        logger.info(f"Session cleaned up: {client_id}")


# ==== Remote Relay Endpoint ====
@app.websocket("/ws/relay")
async def relay_endpoint(websocket: WebSocket):
    """
    远端旁听端点：订阅当前会话的音频流与控制事件。

    远端只收不发：
      - 二进制帧 = int16 PCM（24000 Hz / 单声道），直接送扬声器
      - JSON 帧   = {"event": "...", "data": {...}}，如 stop_audio / pause_audio ...
    连接后即可实时收到本机网页相同的 audio_chunk 与 stop_audio。
    """
    await websocket.accept()
    with relay_lock:
        relay_connections.add(websocket)
    # 旁听端（/ws/relay）连接时打印
    logger.info(f"Relay subscriber connected: {websocket.client.host}")

    try:
        # 远端一般只收不发；这里持续接收以感知断开（收到任意内容即忽略）
        while True:
            message = await websocket.receive()
            if "text" in message and message["text"]:
                # 可选支持 ping/pong 或自定义控制
                logger.debug(f"Relay message ignored: {message['text']}")
    except WebSocketDisconnect:
        # 旁听端断开时打印
        logger.info("Relay subscriber disconnected")
    except Exception as e:
        # 旁听转发异常时打印
        logger.error(f"Relay subscriber error: {e}")
    finally:
        with relay_lock:
            relay_connections.discard(websocket)


# ==== Static Resource Routing ====
app.mount("/", StaticFiles(directory="frontend", html=True), name="frontend")


if __name__ == "__main__":
    import argparse
    import uvicorn

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--tcp_target",
        type=str,
        default=None,
        help="远端播放器地址 ip:port（对应 remote_player.py 的 --port），"
        "app 主动连接并推送音频流，支持即时打断",
    )
    parser.add_argument(
        "--mic_tcp_port",
        type=int,
        default=None,
        help="接收远端麦克风 TCP 音频的端口（配合 remote_mic.py --target），"
        "可靠不丢块，设置后浏览器可勾选「远程麦克风」而不采集本机麦克风",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="无浏览器模式：自动创建常驻会话，音频输入走远端麦克风"
        "（需 --mic_tcp_port）、输出走远端播放器（需 --tcp_target）",
    )
    args = parser.parse_args()

    if args.headless:
        HEADLESS = True
        # 启动参数校验：headless 必须有远端麦克风输入，否则拒绝启动
        if not args.mic_tcp_port:
            logger.error("--headless 需要 --mic_tcp_port（远端麦克风输入）")
            sys.exit(1)
        # 启动参数校验：headless 缺远端播放器仅告警（可后补）
        if not args.tcp_target:
            logger.warning(
                "--headless 未带 --tcp_target，语音将无处播放"
                "（请配合 remote_player.py --port 启动播放端）"
            )

    if args.tcp_target:
        tcp_sender = TcpAudioSender(args.tcp_target)

    if args.mic_tcp_port:
        MIC_MODE = "remote"
        threading.Thread(target=mic_vad_worker, daemon=True).start()
        threading.Thread(
            target=mic_tcp_receiver, args=(args.mic_tcp_port,), daemon=True
        ).start()

    if args.headless:
        # 常驻会话在后台线程创建（vad_pool.acquire 阻塞）
        threading.Thread(target=start_headless_session, daemon=True).start()

    # 服务正式启动前打印监听地址
    logger.info(f"Server starting on http://localhost:{Config.PORT}")
    uvicorn.run(app, host="0.0.0.0", port=Config.PORT)
