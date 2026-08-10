#!/usr/bin/env python3
import asyncio
import base64
import hashlib
import json
import sys
import time
import uuid
from urllib.parse import quote, urlencode, urlsplit, urlunsplit

# ==================== 请在这里填写 ====================

BASE_URL = "wss://aichain-sh.xfyun.cn"

APP_ID = "Lvj2CrW2"
APP_KEY = "3070c4c9788d4a4994458789d3e5f796"
SN = "test_sn"

SCENE = "main"
STT_ENGINE_ID = "3"

# None 表示使用系统默认麦克风，也可以填写设备编号，例如 1
MIC_DEVICE = None

SAMPLE_RATE = 16000
CHANNELS = 1
BLOCK_DURATION_MS = 40

CONNECT_TIMEOUT_SECONDS = 10
RESULT_TIMEOUT_SECONDS = 15

# =====================================================

try:
    import websockets
except ImportError as exc:
    print(
        f"缺少依赖 {exc.name}，请执行：\n"
        "  python -m pip install websockets",
        file=sys.stderr,
    )
    raise SystemExit(2) from exc

# sounddevice 仅交互式录音脚本（__main__）使用；
# 被其他模块（如 service/model.py）import 时允许缺失。
try:
    import sounddevice as sd
except Exception:
    sd = None


class Transcript:
    """累积 AIChain append/replace 类型的识别结果。"""

    def __init__(self):
        self.text = ""
        self.language = "unknown"

    def apply(self, event):
        data = event.get("data")
        if not isinstance(data, dict):
            return False

        language = data.get("language")
        if isinstance(language, str) and language.strip():
            self.language = language.strip()

        text = data.get("text")
        if not isinstance(text, str):
            return False

        action = data.get("action", "append")

        if action == "append":
            self.text += text
            return True

        if action != "replace":
            return False

        position = data.get("position")
        if not isinstance(position, dict):
            return False

        start = position.get("start")
        end = position.get("end")

        if type(start) is not int or type(end) is not int:
            return False

        if start < 0 or start > end:
            # 服务端用 end=-1 表示空区间（清空/占位帧），
            # 携带空文本时视为清空当前文本，静默处理
            if start == 0 and end == -1 and not text:
                self.text = ""
                return True

            return False

        # 空文本 replace 帧（静音/占位）：只更新语言，不改文本
        if not text:
            return True

        # 部分引擎第一帧会直接发送 replace(0..N)。
        if not self.text:
            if start != 0:
                return False

            self.text = text
            return True

        # 服务端常用整句全量 replace(0..len)，end 可以等于当前文本长度。
        # 若 end 超前（流式增量滞后，服务端文本比本地累积长），
        # 该帧携带服务端当前完整句，整句直接采用。
        if end > len(self.text):
            if start != 0:
                return False

            self.text = text
            return True

        self.text = (
            self.text[:start]
            + text
            + self.text[end:]
        )
        return True


def validate_settings():
    values = {
        "APP_ID": APP_ID,
        "APP_KEY": APP_KEY,
        "SN": SN,
    }

    for name, value in values.items():
        if not value or str(value).startswith("请填写"):
            raise ValueError(f"请先在脚本开头填写 {name}")

    if SAMPLE_RATE <= 0:
        raise ValueError("SAMPLE_RATE 必须大于零")

    if CHANNELS != 1:
        raise ValueError("当前脚本要求 CHANNELS = 1")

    if sd is None:
        raise RuntimeError(
            "缺少 sounddevice 依赖，请执行："
            "python -m pip install sounddevice"
        )


def build_auth_url():
    timestamp = int(time.time())

    checksum = hashlib.sha256(
        f"{APP_KEY}{timestamp}".encode("utf-8")
    ).hexdigest()

    parsed = urlsplit(BASE_URL.rstrip("/"))

    if parsed.scheme not in ("ws", "wss") or not parsed.hostname:
        raise ValueError(
            "BASE_URL 必须是合法的 ws:// 或 wss:// 地址"
        )

    query = {
        "curtime": str(timestamp),
        "checksum": checksum,
        "sn": SN,
    }

    if SCENE:
        query["scene"] = SCENE

    path = f"/v1/chat/{quote(APP_ID, safe='')}"

    return urlunsplit(
        (
            parsed.scheme,
            parsed.netloc,
            path,
            urlencode(query),
            "",
        )
    )


def build_session_config():
    stt = {
        "enable": True,
        # 本地不指定语言类型，交由云端自动判断
        "audioConfig": {
            "audioEncoding": "raw",
            "format": "plain",
            "sampleRate": SAMPLE_RATE,
            "bitDepth": 16,
            "channels": CHANNELS,
        },
        # 本脚本通过按回车结束录音，因此关闭服务端 VAD。
        "vad": {
            "enable": False,
        },
        "turnDetection": {
            "enable": False,
        },
    }

    if STT_ENGINE_ID:
        stt["sttEngineId"] = STT_ENGINE_ID

    return {
        "mode": "half_duplex",
        "simplifiedResponse": True,
        "multiTurnEnabled": False,
        "stt": stt,
        "nlu": {
            "enable": False,
        },
        "tts": {
            "enable": False,
        },
    }


def format_server_error(message):
    error = message.get("error") or {}

    if isinstance(error, dict):
        return (
            f"code={error.get('code')}, "
            f"message={error.get('message')}"
        )

    return str(error)


async def receive_json(ws, timeout=None):
    if timeout is None:
        raw = await ws.recv()
    else:
        raw = await asyncio.wait_for(
            ws.recv(),
            timeout=timeout,
        )

    message = json.loads(raw)

    if not isinstance(message, dict):
        raise RuntimeError(
            f"服务端返回了非对象消息：{message!r}"
        )

    if message.get("type") in ("session.error", "error"):
        raise RuntimeError(
            f"{message.get('type')}: "
            f"{format_server_error(message)}"
        )

    return message


async def wait_for_event(ws, expected_type, timeout):
    deadline = (
        asyncio.get_running_loop().time()
        + timeout
    )

    while True:
        remaining = (
            deadline
            - asyncio.get_running_loop().time()
        )

        if remaining <= 0:
            raise TimeoutError(
                f"等待 {expected_type} 超时"
            )

        message = await receive_json(
            ws,
            timeout=remaining,
        )

        if message.get("type") == expected_type:
            return message

        print(
            f"[INFO] 等待 {expected_type} 时忽略："
            f"{message.get('type')}"
        )


def build_audio_event(
    sid,
    cid,
    pcm_bytes,
    end_flag,
):
    return {
        "type": "conversation.user.append",
        "sid": sid,
        "cid": cid,
        "items": [
            {
                "type": "audio",
                "data": base64.b64encode(
                    pcm_bytes
                ).decode("ascii"),
            }
        ],
        "endFlag": bool(end_flag),
    }


async def send_audio(
    ws,
    sid,
    cid,
    audio_queue,
):
    """
    实时发送音频。

    为了确保最后一个真实音频帧携带 endFlag=true，
    始终暂存一帧，收到结束信号后再发送最后一帧。
    """
    pending = None
    frame_count = 0
    byte_count = 0

    while True:
        data = await audio_queue.get()

        if data is None:
            if pending is None:
                raise RuntimeError(
                    "没有采集到麦克风音频"
                )

            await ws.send(json.dumps(
                build_audio_event(
                    sid,
                    cid,
                    pending,
                    True,
                )
            ))

            frame_count += 1
            byte_count += len(pending)

            print(
                f"[INFO] 音频发送完成："
                f"{frame_count} 帧，"
                f"{byte_count} 字节"
            )
            return

        if not data:
            continue

        if pending is not None:
            await ws.send(json.dumps(
                build_audio_event(
                    sid,
                    cid,
                    pending,
                    False,
                )
            ))

            frame_count += 1
            byte_count += len(pending)

        pending = data


async def receive_results(
    ws,
    sid,
    cid,
):
    transcript = Transcript()

    # [TIMING] 计时起点：首个识别结果到达耗时
    t0 = time.monotonic()
    first_result = True

    while True:
        message = await receive_json(ws)
        message_type = message.get("type")

        message_sid = message.get("sid")
        message_cid = message.get("cid")

        if message_sid not in (None, sid):
            continue

        if message_cid not in (None, cid):
            continue

        if message_type == "stt.result":
            if transcript.apply(message):
                if first_result:
                    print(
                        f"[TIMING] 首个识别结果到达: "
                        f"{time.monotonic() - t0:.3f}s"
                    )
                    first_result = False

                frame_kind = (
                    "FINAL-FRAME"
                    if message.get("last")
                    else "PARTIAL"
                )

                print(
                    f"[{frame_kind}] "
                    f"language={transcript.language} | "
                    f"{transcript.text}"
                )
            else:
                print(
                    "[WARN] 无法解析 stt.result，"
                    f"data={json.dumps(message.get('data'), ensure_ascii=False)}"
                )

            continue

        if message_type == "event.cid_end":
            return (
                transcript.text,
                transcript.language,
            )

        print(
            f"[INFO] 忽略服务端事件："
            f"{message_type}"
        )


async def recognize_pcm(
    pcm_bytes,
    timeout=RESULT_TIMEOUT_SECONDS,
):
    """
    整段 PCM 识别，返回 (text, language)。

    一次性把整段音频作为一条 conversation.user.append 发送，
    并携带 endFlag=true，然后等待最终识别结果。
    """
    url = build_auth_url()

    async with websockets.connect(
        url,
        open_timeout=CONNECT_TIMEOUT_SECONDS,
    ) as ws:
        created = await wait_for_event(
            ws,
            "session.created",
            CONNECT_TIMEOUT_SECONDS,
        )

        sid = str(
            created.get("sid") or ""
        ).strip()

        if not sid:
            raise RuntimeError(
                "session.created 中没有 sid"
            )

        await ws.send(json.dumps(
            {
                "type": "session.config",
                "sid": sid,
                "config": build_session_config(),
            },
            ensure_ascii=False,
        ))

        await wait_for_event(
            ws,
            "session.configed",
            CONNECT_TIMEOUT_SECONDS,
        )

        cid = (
            f"{APP_ID}@{int(time.time())}-"
            f"{uuid.uuid4().hex[:8]}"
        )

        await ws.send(json.dumps(
            build_audio_event(
                sid,
                cid,
                pcm_bytes,
                True,
            )
        ))

        try:
            return await asyncio.wait_for(
                receive_results(ws, sid, cid),
                timeout=timeout,
            )
        except asyncio.TimeoutError as exc:
            raise TimeoutError(
                "等待最终识别结果超时"
            ) from exc


async def record_one_utterance(
    ws,
    sid,
):
    cid = (
        f"{APP_ID}@{int(time.time())}-"
        f"{uuid.uuid4().hex[:8]}"
    )

    audio_queue = asyncio.Queue()
    loop = asyncio.get_running_loop()

    t_utterance = time.monotonic()  # [TIMING] 本次识别耗时计时起点

    blocksize = max(
        1,
        int(
            SAMPLE_RATE
            * BLOCK_DURATION_MS
            / 1000
        ),
    )

    def microphone_callback(
        indata,
        frames,
        time_info,
        status,
    ):
        del frames, time_info

        if status:
            print(
                f"\n[WARN] 麦克风状态：{status}",
                file=sys.stderr,
            )

        pcm = bytes(indata)

        loop.call_soon_threadsafe(
            audio_queue.put_nowait,
            pcm,
        )

    sender_task = asyncio.create_task(
        send_audio(
            ws,
            sid,
            cid,
            audio_queue,
        )
    )

    receiver_task = asyncio.create_task(
        receive_results(
            ws,
            sid,
            cid,
        )
    )

    try:
        device_info = sd.query_devices(
            MIC_DEVICE,
            "input",
        )

        print(
            f"[INFO] 麦克风："
            f"{device_info['name']}"
        )

        print(
            "[REC] 正在录音，请讲话；"
            "按回车结束录音……"
        )

        with sd.RawInputStream(
            samplerate=SAMPLE_RATE,
            blocksize=blocksize,
            device=MIC_DEVICE,
            channels=CHANNELS,
            dtype="int16",
            callback=microphone_callback,
        ):
            await asyncio.to_thread(input)

        # 麦克风停止后，通知发送协程发送最终帧。
        await audio_queue.put(None)

        await sender_task

        try:
            result = await asyncio.wait_for(
                receiver_task,
                timeout=RESULT_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError as exc:
            raise TimeoutError(
                "等待最终识别结果超时"
            ) from exc

        print(
            f"[TIMING] 最终识别结果返回耗时: "
            f"{time.monotonic() - t_utterance:.3f}s"
        )
        return result

    finally:
        for task in (
            sender_task,
            receiver_task,
        ):
            if not task.done():
                task.cancel()

        await asyncio.gather(
            sender_task,
            receiver_task,
            return_exceptions=True,
        )


async def main():
    validate_settings()

    url = build_auth_url()
    parsed = urlsplit(BASE_URL)

    masked_sn = (
        "****" + SN[-4:]
        if len(SN) > 4
        else "****"
    )

    print(
        f"[INFO] AIChain host={parsed.netloc}, "
        f"appid={APP_ID}, "
        f"scene={SCENE or '默认'}, "
        f"sn={masked_sn}"
    )

    started = time.monotonic()

    async with websockets.connect(
        url,
        open_timeout=CONNECT_TIMEOUT_SECONDS,
    ) as ws:
        print(
            f"[OK] WebSocket 已连接："
            f"{time.monotonic() - started:.3f}s"
        )

        created = await wait_for_event(
            ws,
            "session.created",
            CONNECT_TIMEOUT_SECONDS,
        )

        sid = str(
            created.get("sid") or ""
        ).strip()

        if not sid:
            raise RuntimeError(
                "session.created 中没有 sid"
            )

        await ws.send(json.dumps(
            {
                "type": "session.config",
                "sid": sid,
                "config": build_session_config(),
            },
            ensure_ascii=False,
        ))

        configured = await wait_for_event(
            ws,
            "session.configed",
            CONNECT_TIMEOUT_SECONDS,
        )

        configured_sid = str(
            configured.get("sid") or ""
        ).strip()

        if (
            configured_sid
            and configured_sid != sid
        ):
            raise RuntimeError(
                "session.configed 返回了不同的 sid"
            )

        print(
            f"[OK] AIChain 会话配置成功，"
            f"sid={sid}"
        )

        text, language = (
            await record_one_utterance(
                ws,
                sid,
            )
        )

    print("\n========== 最终结果 ==========")
    print(f"文本：{text or '<空>'}")
    print(f"语种：{language}")
    print("==============================")


if __name__ == "__main__":
    try:
        asyncio.run(main())

    except KeyboardInterrupt:
        print(
            "\n[FAIL] 用户取消",
            file=sys.stderr,
        )
        raise SystemExit(130)

    except Exception as exc:
        print(
            f"\n[FAIL] "
            f"{type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        raise SystemExit(1)