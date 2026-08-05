import re
import time
import soxr
import queue
import base64
import socket
import struct
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

# UDP channel (--udp_target). When enabled, audio goes via UDP only (one channel at a time).
udp_sender = None

# UDP packet format: [type:1B][seq:4B][payload_len:2B][payload]
#   type=0 audio (payload = raw int16 PCM), type=1 control (payload = control code)
UDP_MAX_PAYLOAD = 1200  # keep below MTU to avoid IP fragmentation
UDP_CTRL = {"stop_audio": 0, "pause_audio": 1, "resume_audio": 2}


class UdpSender:
    """Best-effort UDP sender for the audio stream / control events."""

    def __init__(self, target: str):
        host, port = target.rsplit(":", 1)
        self.addr = (host, int(port))
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.seq = 0
        logger.info(f"UDP channel enabled -> {target}")

    def send_audio(self, data: bytes):
        for i in range(0, len(data), UDP_MAX_PAYLOAD):
            chunk = data[i : i + UDP_MAX_PAYLOAD]
            pkt = struct.pack(">BIH", 0, self.seq, len(chunk)) + chunk
            self.sock.sendto(pkt, self.addr)
            self.seq += 1

    def send_event(self, event: str, data):
        code = UDP_CTRL.get(event)
        if code is None:
            return  # UI-only events are not forwarded over UDP
        pkt = struct.pack(">BIH", 1, self.seq, 1) + bytes([code])
        self.sock.sendto(pkt, self.addr)
        self.seq += 1


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

    def __init__(self, client_id, vad_instance, websocket: WebSocket):
        self.client_id = client_id
        self.vad = vad_instance
        self.websocket = websocket
        self.lock = threading.Lock()
        self.vad_lock = threading.Lock()  # 序列化 VAD 调用（浏览器麦克风 / UDP 远端麦克风）
        self._stop_event = threading.Event()  # Internal event to signal interruption
        self.is_active = True
        self.uses_remote_mic = False  # 是否使用 UDP 转发来的远端麦克风

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

    def interrupt(self):
        """Interrupts current inference or audio playback."""
        self._stop_event.set()
        emit_to_room(self.client_id, "stop_audio", {"message": "interrupt"})
        emit_to_room(self.client_id, "circle_status", {"status": "LISTENING"})

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
print("System initialized: VAD Pool, LLM client, TTS client ready.")


def emit_to_room(client_id, event, data):
    """Helper function to safely emit WebSocket messages to a specific client."""
    session = session_manager.get_session(client_id)
    if not session or not main_loop:
        return

    ws = session.websocket

    # Helper wrapper to run async send in the main loop
    async def _send():
        # 1. Send to the local session websocket
        if ws.client_state == WebSocketState.CONNECTED:
            try:
                if event == "audio_chunk":
                    # Audio data: send raw bytes
                    await ws.send_bytes(data)
                else:
                    # Text/JSON data
                    message = json.dumps({"event": event, "data": data})
                    await ws.send_text(message)
            except Exception as e:
                logger.error(f"Failed to send to {client_id}: {e}")

        # 2. Forward to remote relay subscribers (raw bytes for audio, JSON for events)
        await _relay_forward(event, data)

    asyncio.run_coroutine_threadsafe(_send(), main_loop)


async def _relay_forward(event, data):
    """Forward audio chunks / control events to the remote (UDP or WebSocket relay)."""
    global udp_sender

    # One channel at a time: if UDP is enabled, WS relay is not used
    if udp_sender is not None:
        if event == "audio_chunk":
            udp_sender.send_audio(data)
        # 不再发送控制事件（如 stop_audio），远端依赖数据流中断自然停播
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
                    logger.info(
                        f"[{client_id}] Previous turn, Ratio: {ratio:.2f}, Truncated: {final_msg}"
                    )
                else:
                    logger.info(f"[{client_id}] Previous turn completed fully.")

                llm.add_message(client_id, "assistant", final_msg)

            # Clear pending state
            session.pending_message = None
            session.pending_audio_duration = 0.0
            session.pending_start_time = None
            session.interruption_time = None

        # 3. Preparation for Response
        session.interrupt()  # Signal previous threads to stop
        session.reset_interrupt()  # Create a fresh event for this new thread

        # Capture the specific stop_event for this execution cycle
        current_stop_event = session.stop_event

        if current_stop_event.is_set():
            return

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
                    logger.info(  # [Timing] From barge-in arrival to LLM loop detection
                        f"[{client_id}] Interrupt Latency (LLM loop): {time.time() - session.interruption_time:.3f}s"
                    )
                break

            logger.info(f"[{client_id}] LLM Chunk: {chunk}")
            if t_llm_first_chunk is None:
                t_llm_first_chunk = time.time()  # [Timing] First LLM token received
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
                        logger.info(  # [Timing] From barge-in arrival to TTS loop detection
                            f"[{client_id}] Interrupt Latency (TTS loop): {time.time() - session.interruption_time:.3f}s"
                        )
                    break

                if first_emit_time is None:
                    first_emit_time = time.time()
                if t_tts_first_audio is None:
                    t_tts_first_audio = time.time()  # [Timing] First TTS audio emitted
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
                    logger.info(  # [Timing] From barge-in arrival to post-TTS detection
                        f"[{client_id}] Interrupt Latency (post-TTS): {time.time() - session.interruption_time:.3f}s"
                    )
                break

        # [Timing] LLM & TTS total generation duration
        if t_llm_first_chunk is not None:
            logger.info(
                f"[{client_id}] LLM Total: {time.time() - t_llm_first_chunk:.3f}s"
            )
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

        logger.info(  # [Timing] Total E2E pipeline time (start → finish)
            f"[{client_id}] E2E Complete: {time.time() - t_pipeline_start:.3f}s"
        )

    except Exception as e:
        logger.error(f"Error in pipeline for {client_id}: {e}", exc_info=True)


# ==== Remote Mic UDP Receiver ====
# 远端麦克风通过 UDP 转发过来（见 remote_mic_udp.py），
# 与浏览器麦克风走完全相同的 VAD -> 打断/发言 处理路径。
# 数据包格式与 UdpSender 一致: [type:1B][seq:4B][payload_len:2B][payload]
MIC_UDP_PKT_HEADER = 7
mic_audio_queue = queue.Queue(maxsize=64)


def pick_mic_session():
    """选择接收远端麦克风音频的会话：优先显式启用远端麦克风的会话，否则取最近活跃的会话。"""
    with session_manager._lock:
        candidates = list(session_manager.sessions.values())
    for s in reversed(candidates):
        if s.is_active and s.uses_remote_mic:
            return s
    for s in reversed(candidates):
        if s.is_active:
            return s
    return None


def mic_vad_worker():
    """从队列取远端麦克风音频送入 VAD，处理结果与浏览器麦克风一致。"""
    while True:
        chunk = mic_audio_queue.get()
        session = pick_mic_session()
        if session is None:
            continue
        try:
            with session.vad_lock:
                segment = session.vad.process(chunk)
            if segment is not None:
                if isinstance(segment, list) and segment[0] is None:
                    # Barge-in（打断当前回复）
                    if session.interruption_time is None:
                        session.interruption_time = time.time()
                    session.interrupt()
                else:
                    threading.Thread(
                        target=pipeline_worker,
                        args=(session.client_id, segment, Config.SAMPLE_RATE),
                        daemon=True,
                    ).start()
        except Exception as e:
            logger.error(f"[mic-udp] VAD process failed: {e}", exc_info=True)


def mic_udp_receiver(port):
    """接收远端麦克风 UDP 音频包，放入队列供 mic_vad_worker 处理。"""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("0.0.0.0", port))
    sock.settimeout(0.5)
    logger.info(f"[mic-udp] listening on 0.0.0.0:{port} for remote microphone")
    pkt_count = 0
    last_log = time.time()
    while True:
        try:
            pkt, _ = sock.recvfrom(2048)
        except socket.timeout:
            continue
        # 周期性打印接收统计，便于确认 UDP 麦克风链路是否打通
        pkt_count += 1
        now = time.time()
        if now - last_log >= 5.0:
            logger.info(
                f"[mic-udp] received {pkt_count} packets in last {now - last_log:.1f}s"
            )
            pkt_count = 0
            last_log = now
        if len(pkt) < MIC_UDP_PKT_HEADER:
            continue
        ptype, seq, plen = struct.unpack(">BIH", pkt[:MIC_UDP_PKT_HEADER])
        if ptype != 0:
            continue
        payload = pkt[MIC_UDP_PKT_HEADER : MIC_UDP_PKT_HEADER + plen]
        if not payload or len(payload) % 2 != 0:
            continue
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


# ==== WebSocket Endpoint & Processing ====
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    client_id = str(uuid.uuid4())
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

        # Now emission works via session_manager lookups
        emit_to_room(client_id, "connect_ack", {"client_id": client_id})
        emit_to_room(
            client_id,
            "vad_loading",
            {"state": "ready", "message": "Model loaded, ready to experience"},
        )
        logger.info(f"Session initialized for {client_id}")

        while True:
            # Receive Message
            try:
                message = await websocket.receive()
            except WebSocketDisconnect:
                logger.info(f"Client disconnected: {client_id}")
                break

            if "bytes" in message and message["bytes"]:
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
                        if session.interruption_time is None:
                            session.interruption_time = time.time()
                        session.interrupt()
                    else:
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
                        logger.info(f"Session manually stopped by client: {client_id}")

                    elif event == "config_audio":
                        # Client sending sample rate configuration
                        sr_data = payload.get("data", {})
                        sr = sr_data.get("sample_rate")
                        if sr:
                            session.input_sample_rate = int(sr)
                            logger.info(f"[{client_id}] Sample rate set to {sr}")
                        # 标记使用 UDP 转发的远端麦克风，浏览器自身不再采集麦克风
                        if sr_data.get("source") == "remote_mic":
                            session.uses_remote_mic = True
                            logger.info(f"[{client_id}] Remote mic (UDP) enabled")

                except json.JSONDecodeError:
                    logger.warning(f"[{client_id}] Received invalid JSON")

    except WebSocketDisconnect:
        logger.info(f"Client disconnected: {client_id}")
    except Exception as e:
        logger.error(f"WebSocket error: {e}", exc_info=True)
    finally:
        if session:
            session.is_active = False
            session.stop_event.set()
            vad_pool.release(session.vad)
            session_manager.remove_session(client_id)
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
    logger.info(f"Relay subscriber connected: {websocket.client.host}")

    try:
        # 远端一般只收不发；这里持续接收以感知断开（收到任意内容即忽略）
        while True:
            message = await websocket.receive()
            if "text" in message and message["text"]:
                # 可选支持 ping/pong 或自定义控制
                logger.debug(f"Relay message ignored: {message['text']}")
    except WebSocketDisconnect:
        logger.info("Relay subscriber disconnected")
    except Exception as e:
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
        "--udp_target",
        type=str,
        default=None,
        help="远端 UDP 地址 ip:port，设置后音频走 UDP 通道（与 WebSocket relay 二选一）",
    )
    parser.add_argument(
        "--mic_udp_port",
        type=int,
        default=None,
        help="接收远端麦克风 UDP 音频的端口（配合 remote_mic_udp.py），"
        "设置后浏览器可勾选「远程麦克风」而不采集本机麦克风",
    )
    args = parser.parse_args()

    if args.udp_target:
        udp_sender = UdpSender(args.udp_target)

    if args.mic_udp_port:
        threading.Thread(target=mic_vad_worker, daemon=True).start()
        threading.Thread(
            target=mic_udp_receiver, args=(args.mic_udp_port,), daemon=True
        ).start()

    logger.info(f"Server starting on http://localhost:{Config.PORT}")
    uvicorn.run(app, host="0.0.0.0", port=Config.PORT)
