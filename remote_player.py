#!/usr/bin/env python3
"""
远端播放器：接收 4090 通过 TCP 推送的实时 TTS 音频流并在本机扬声器播放。

用法:
    pip install sounddevice

    python remote_player.py --port 1212 --device HECATE \           # 指定输出设备（耳机/声卡）
  --forward 127.0.0.1:1213 \                                        # 转发到 audio_face_stream
  --forward-gate                                                    # 严格闸门模式:等下游推理完口型并下发 ROS 后回执再播放，让口型和声音同时开始

app.py 用 --tcp_target <本机IP>:<port> 主动连上来并推送音频（本脚本只监听接收）。

TCP 帧格式（与 app.py 的 TcpAudioSender 对应）:
    [type:1B][len:4B big-endian][payload]
    type=0 音频: payload = 裸 int16 小端 PCM（24000 Hz 单声道，整段一帧）
    type=1 控制: payload = 控制码（0=stop, 1=pause, 2=resume）
    TCP 有序可靠，无需重排。

打断机制:
    - 主信号 : app 发送 stop 控制帧（type=1, payload=0）-> 播放端清空缓冲立即静音
      （连接保持）；同时 stop 帧随转发链路发给下游口型程序，口型同步停止
    - 兜底    : TCP 断连（app 崩溃/网络断/重启）时同样清空缓冲静音，避免残留音频

无 PCM 流时的行为:
    - 对话间隙 : 播放缓冲为空，静默等待，新数据到来自动恢复
    - 断流      : 播放队列自然耗尽后静音；app 断开后继续等待新连接
"""
import argparse
import queue
import socket
import struct
import sys
import threading
import time

import numpy as np
import sounddevice as sd

SAMPLE_RATE = 24000   # 网络侧音频采样率（app.py 推送的 TTS）
CHANNELS = 1
DTYPE = "int16"

# 输出流固定块大小：OUT_BLOCK_MS 为每次写入粒度，避免 ALSA underrun
OUT_BLOCK_MS = 20     # 每次写入 20ms
DEFAULT_LATENCY = 0.04  # 输出缓冲 40ms（越小延迟越低，过小易爆音，可用 --latency 调）

audio_q = queue.Queue()
mute_flag = threading.Event()  # 打断标志：置位后播放线程放弃当前段并清声卡缓冲
#------------------------------------------------转发队列------------------------------------------------
# 转发队列：--forward 指定下游（如 audio_face_stream）时，收到的帧原样再发一份。
# 与播放路径解耦，下游没连上/发不动都不影响本机出声；但打断时与播放同步清空。
forward_q = queue.Queue(maxsize=256)
_forward_enabled = False

# --forward-gate：先扣住音频，等下游（audio_face_stream）推理完并下发 ROS 后
# 回执 "ACK <bytes>"，再把对应字节放进播放队列，让口型和声音同步出现。
_forward_gated = False
_hold_lock = threading.Lock()
_hold_buf = bytearray()
_hold_since = None
# In gate mode, audio must never bypass Audio2Face.  This threshold only emits
# a diagnostic if ACKs stop arriving; it does not release held audio.
GATE_WARNING_SECONDS = 0.5
_interrupt_generation = 0


def _resample_linear(x, src_rate, dst_rate):
    """线性插值重采样，用于设备原生采样率不是 24kHz 的情况。"""
    if src_rate == dst_rate:
        return x
    src = np.asarray(x, dtype=np.float32)
    n_out = int(round(len(src) * dst_rate / src_rate))
    src_idx = np.linspace(0.0, len(src) - 1, n_out)
    lo = src_idx.astype(np.int32)
    hi = np.minimum(lo + 1, len(src) - 1)
    frac = src_idx - lo  # 1D 插值位置小数部分；用 [:, None] 会变成 N×N 广播导致 OOM
    return (src[lo] * (1.0 - frac) + src[hi] * frac).astype(np.int16)


def _resolve_device(arg):
    """把 --device 参数解析成输出设备索引（None=系统默认输出）。

    支持: 数字索引、名称关键字（如 HECATE）。
    """
    if arg is None:
        return None
    if arg.isdigit():
        return int(arg)
    for d in sd.query_devices():
        if d["max_output_channels"] > 0 and arg.lower() in d["name"].lower():
            print(f"[player] matched device {d['index']}: {d['name']}")
            return d["index"]
    print(f"[player] no output device matching '{arg}'", file=sys.stderr)
    sys.exit(1)


def player_worker(device_index, latency):
    """播放线程：从队列取 PCM 写入扬声器；输出流异常时自动重开设备。"""
    while True:
        try:
            _player_worker_once(device_index, latency)
        except sd.PortAudioError as exc:
            print(f"[player] output stream error, reopening: {exc}")
            with audio_q.mutex:
                audio_q.queue.clear()
            mute_flag.set()
            time.sleep(0.5)
        except Exception as exc:
            print(f"[player] unexpected output error, reopening: {exc}")
            with audio_q.mutex:
                audio_q.queue.clear()
            mute_flag.set()
            time.sleep(0.5)


def _player_worker_once(device_index, latency):
    """打开一次输出流并持续播放，直到设备/流异常。"""
    dev = sd.query_devices(device_index, kind="output")
    native_rate = int(dev["default_samplerate"])
    if native_rate != SAMPLE_RATE:
        print(f"[player] device {native_rate}Hz, will resample from {SAMPLE_RATE}Hz")
    blocksize = int(native_rate * OUT_BLOCK_MS / 1000)
    silence = b"\x00\x00" * blocksize

    with sd.RawOutputStream(
        samplerate=native_rate,
        channels=CHANNELS,
        dtype=DTYPE,
        blocksize=blocksize,
        latency=latency,
        device=device_index,
    ) as out:
        print(
            f"[player] opened output: {dev['name']} @ "
            f"{native_rate} Hz / {CHANNELS} ch / {DTYPE}"
        )
        while True:
            try:
                chunk = audio_q.get(timeout=0.02)
            except queue.Empty:
                # 无新数据：写静音块保持声卡数据流连续，避免 underrun 爆音
                if mute_flag.is_set():
                    mute_flag.clear()
                out.write(silence)
                continue
            if native_rate != SAMPLE_RATE:
                chunk = _resample_linear(
                    np.frombuffer(chunk, dtype=np.int16), SAMPLE_RATE, native_rate
                ).tobytes()
            # 按固定块大小分片写入，保持声卡写入节奏稳定
            for i in range(0, len(chunk), len(silence)):
                if mute_flag.is_set():
                    # 打断：放弃当前段剩余，abort 清掉声卡缓冲，随后 start 恢复流
                    # （abort 后必须 start 才能继续写，否则 write 会抛 Stream is stopped）
                    mute_flag.clear()
                    out.abort()
                    out.start()
                    break
                out.write(chunk[i : i + len(silence)])


def clear_all_buffers():
    """清空播放/转发队列并置位打断标志：正在写入声卡的音频段也会被丢弃，立即静音。

    转发与播放同节奏：打断时下游（audio_face_stream）同样不再收到被截断的尾部音频。
    """
    global _hold_since, _interrupt_generation
    _interrupt_generation += 1
    with audio_q.mutex:
        audio_q.queue.clear()
    with forward_q.mutex:
        forward_q.queue.clear()
    with _hold_lock:
        _hold_buf.clear()
        _hold_since = None
    mute_flag.set()

#------------------------------------------------转发程序------------------------------------------------
def _parse_forward_target(arg):
    """把 --forward host:port 解析成 (host, port)。"""
    host, _, port = arg.rpartition(":")
    if not host or not port.isdigit():
        print(f"[forward] invalid target '{arg}', expected host:port", file=sys.stderr)
        sys.exit(1)
    return host, int(port)


def _enqueue_forward(ptype, payload):
    """把帧放进转发队列。

    严格闸门下绝不能丢帧，否则扣留音频将永远等不到对应 ACK；因此队列满时
    通过上游 TCP 自然背压。非闸门模式维持原来的实时优先策略，满时丢最旧帧。
    """
    if not _forward_enabled:
        return
    if _forward_gated:
        forward_q.put((ptype, payload))
        return
    try:
        forward_q.put_nowait((ptype, payload))
    except queue.Full:
        try:
            forward_q.get_nowait()
            forward_q.put_nowait((ptype, payload))
        except (queue.Empty, queue.Full):
            pass


def _enqueue_forward_priority(ptype, payload):
    """打断控制帧优先转发：丢弃旧音频，让下游立刻收到 stop。"""
    if not _forward_enabled:
        return
    with forward_q.mutex:
        forward_q.queue.clear()
    forward_q.put((ptype, payload))


def _hold_audio(payload):
    """--forward-gate 模式：先把音频扣在这里，等下游回执再放行。"""
    global _hold_since
    with _hold_lock:
        _hold_buf.extend(payload)
        if _hold_since is None:
            _hold_since = time.monotonic()


def _release_audio(n_bytes):
    """把已完成推理的前 n_bytes 交给播放队列；n_bytes<=0 表示全放。"""
    global _hold_since
    with _hold_lock:
        if not _hold_buf:
            return 0
        if n_bytes <= 0 or n_bytes > len(_hold_buf):
            n_bytes = len(_hold_buf)
        chunk = bytes(_hold_buf[:n_bytes])
        del _hold_buf[:n_bytes]
        _hold_since = time.monotonic() if _hold_buf else None
    audio_q.put(chunk)
    return len(chunk)


def gate_watchdog():
    """严格闸门监控：ACK 停止时只告警，绝不绕过 A2F 放音。"""
    warned = False
    while True:
        time.sleep(0.2)
        with _hold_lock:
            stalled_for = (
                time.monotonic() - _hold_since
                if _hold_since is not None
                else 0.0
            )
            held_bytes = len(_hold_buf)
        stalled = held_bytes > 0 and stalled_for > GATE_WARNING_SECONDS
        if stalled and not warned:
            print(
                f"[gate] waiting {stalled_for:.1f}s for A2F ACK; "
                f"holding {held_bytes} bytes (not playing unsynced)"
            )
            warned = True
        elif not stalled:
            warned = False


def _drain_forward_acks(conn, ack_buf):
    """非阻塞读取下游 ACK，返回 (conn, ack_buf)。"""
    if conn is None or not _forward_gated:
        return conn, ack_buf
    try:
        conn.settimeout(0.01)
        while True:
            data = conn.recv(256)
            if not data:
                raise OSError("downstream closed")
            ack_buf += data
            while b"\n" in ack_buf:
                line, _, ack_buf = ack_buf.partition(b"\n")
                fields = line.decode("utf-8", "ignore").split()
                if len(fields) == 2 and fields[0] == "ACK":
                    try:
                        released = _release_audio(int(fields[1]))
                        if released:
                            print(f"[gate] ack released {released} bytes")
                    except ValueError:
                        pass
    except (socket.timeout, BlockingIOError):
        pass
    except OSError:
        try:
            conn.close()
        except OSError:
            pass
        conn = None
        ack_buf = b""
        print("[forward] downstream closed")
    return conn, ack_buf


def forward_worker(target):
    """转发音频并持续读取 A2F ACK。

    严格闸门下，同一帧会一直重试到成功，绝不丢帧或绕过 A2F 放音；
    非闸门模式仍以实时性优先，连接失败时允许丢当前帧。
    """
    conn = None
    ack_buf = b""
    while True:
        conn, ack_buf = _drain_forward_acks(conn, ack_buf)
        try:
            ptype, payload = forward_q.get(timeout=0.02)
        except queue.Empty:
            continue

        generation = _interrupt_generation
        frame = struct.pack(">BI", ptype, len(payload)) + payload
        while True:
            if generation != _interrupt_generation and ptype == 0:
                print("[forward] interrupt arrived, dropping stale audio frame")
                break
            if conn is None:
                try:
                    conn = socket.create_connection(target, timeout=2.0)
                    ack_buf = b""
                    print(f"[forward] connected to {target[0]}:{target[1]}")
                except OSError:
                    if not _forward_gated:
                        print("[forward] downstream unavailable, dropping frame")
                        break
                    time.sleep(0.2)
                    continue
            try:
                conn.settimeout(2.0)
                conn.sendall(frame)
                conn, ack_buf = _drain_forward_acks(conn, ack_buf)
                break
            except OSError:
                try:
                    conn.close()
                except OSError:
                    pass
                conn = None
                ack_buf = b""
                if not _forward_gated:
                    print("[forward] downstream unavailable, dropping frame")
                    break
                print("[forward] downstream unavailable, retrying held frame")
                time.sleep(0.2)
#------------------------------------------------以上为转发程序------------------------------------------------

def _handle_client(conn):
    """读取单个 TCP 连接上的音频帧；连接关闭/异常即退出。

    不设空闲超时：对话间隙没有音频是正常状态，连接应保持稳定；
    断线检测交给 app 侧（TcpAudioSender._maintain），新连接到来时由监听线程关闭旧连接。
    """
    try:
        buf = b""
        while True:
            # 读帧头 [type:1B][len:4B]
            while len(buf) < 5:
                data = conn.recv(4096)
                if not data:
                    return
                buf += data
            ptype = buf[0]
            plen = struct.unpack(">I", buf[1:5])[0]
            buf = buf[5:]
            # 读完整 payload
            while len(buf) < plen:
                data = conn.recv(4096)
                if not data:
                    return
                buf += data
            payload = buf[:plen]
            buf = buf[plen:]
            if ptype == 0:
                if _forward_gated:
                    # 等下游推理完并下发 ROS 后再放音，避免口型晚于声音
                    _hold_audio(payload)
                else:
                    audio_q.put(payload)
                _enqueue_forward(ptype, payload)
            elif ptype == 1 and payload:
                if payload[0] == 0:
                    print("[tcp] stop control -> clearing buffers (interrupt)")
                    clear_all_buffers()
                    _enqueue_forward_priority(ptype, payload)
                else:
                    _enqueue_forward(ptype, payload)
            elif ptype != 2:
                _enqueue_forward(ptype, payload) #------------------------------------------------转发程序------------------------------------------------
    except OSError:
        pass
    finally:
        try:
            conn.close()
        except OSError:
            pass
        # 断连兜底：打断主信号是 stop 控制帧（连接保持）；这里处理 app 崩溃/
        # 网络断/重启导致的连接断开，同样清空缓冲立即静音，避免残留音频
        print("[tcp] app disconnected -> buffers cleared")
        clear_all_buffers()


def tcp_listener(port):
    """监听 app.py（--tcp_target）的连接，按 [type:1B][len:4B][payload] 收帧播放。

    每个连接独立线程处理；新连接到来时关闭旧连接并清空播放缓冲，
    避免 app 断线重连/重启后旧连接残留、串话或阻塞监听。
    """
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("0.0.0.0", port))
    srv.listen(4)
    print(f"[tcp] listening on 0.0.0.0:{port}")
    current = [None]
    while True:
        conn, addr = srv.accept()
        print(f"[tcp] app connected: {addr}")
        # 关闭旧连接，避免重复连接导致双播
        old = current[0]
        if old is not None:
            try:
                old.close()
            except OSError:
                pass
        current[0] = conn
        # 新会话开始：清掉残留音频，避免串话
        clear_all_buffers()
        threading.Thread(target=_handle_client, args=(conn,), daemon=True).start()


def main():
    parser = argparse.ArgumentParser(description="Remote TTS stream player (TCP)")
    parser.add_argument(
        "--port",
        type=int,
        default=1212,
        help="监听端口（app.py 的 --tcp_target 连上来），默认 1212",
    )
    parser.add_argument(
        "--device",
        type=str,
        default=None,
        help="输出设备：索引号或名称关键字（如 HECATE），默认系统默认输出",
    )
    parser.add_argument(
        "--latency",
        type=float,
        default=DEFAULT_LATENCY,
        help="输出缓冲时长（秒），越小延迟越低，过小可能爆音，默认 0.04",
    )
    parser.add_argument("--list", action="store_true", help="列出所有音频设备")
    #------------------------------------------------转发程序------------------------------------------------
    parser.add_argument(
        "--forward",
        type=str,
        default=None,
        help=(
            "把收到的帧原样转发到 host:port（如 127.0.0.1:1213），"
            "供 audio_face_stream 做实时表情推理；不填则只播放"
        ),
    )
    parser.add_argument(
        "--forward-gate",
        action="store_true",
        help=(
            "先扣住音频，等下游推理完口型并下发 ROS 后回执再播放，"
            "让口型和声音同时开始；需要与 --forward 一起使用"
        ),
    )
    #------------------------------------------------以上为转发程序------------------------------------------------
    args = parser.parse_args()

    if args.list:
        print(sd.query_devices())
        sys.exit(0)

    device_index = _resolve_device(args.device)
    threading.Thread(
        target=player_worker, args=(device_index, args.latency), daemon=True
    ).start()

    #------------------------------------------------转发程序------------------------------------------------
    if args.forward:
        global _forward_enabled, _forward_gated
        _forward_enabled = True
        target = _parse_forward_target(args.forward)
        threading.Thread(target=forward_worker, args=(target,), daemon=True).start()
        print(f"[forward] enabled -> {target[0]}:{target[1]}")
        if args.forward_gate:
            _forward_gated = True
            threading.Thread(target=gate_watchdog, daemon=True).start()
            print(
                f"[gate] strict mode enabled, warning after "
                f"{GATE_WARNING_SECONDS}s without ACK"
            )
    elif args.forward_gate:
        print("[gate] --forward-gate 需要配合 --forward 使用", file=sys.stderr)
        sys.exit(1)
    #------------------------------------------------以上为转发程序------------------------------------------------
    tcp_listener(args.port)


if __name__ == "__main__":
    main()