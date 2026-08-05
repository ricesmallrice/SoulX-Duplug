#!/usr/bin/env python3
"""
远端麦克风发送端：在本机采集麦克风音频，通过 TCP 实时转发到运行 app.py 的机器（4090），
这样就不需要在那台机器上开浏览器麦克风了。

用法:
    pip install sounddevice numpy
    python remote_mic.py --target 192.168.1.100:55559
    python remote_mic.py --list            # 列出本机音频设备，找到要用的麦克风
    python remote_mic.py --target 192.168.1.100:55559 --device 2    # 按索引
    python remote_mic.py --target 192.168.1.100:55559 --device HECATE  # 按名称关键字

与 remote_player.py 对称：播放端负责「收 TTS 音频」，本脚本负责「发麦克风音频」。

TCP 帧格式（与 app.py 的 mic_tcp_receiver 对应）:
    [type:1B][len:4B big-endian][payload]
    type=0 音频: payload = 裸 int16 小端 PCM（16000 Hz 单声道）
"""
import argparse
import os
import queue
import socket
import struct
import sys
import threading
import time

import numpy as np
import sounddevice as sd

SAMPLE_RATE = 16000   # 与 app.py 的 Config.SAMPLE_RATE 一致
CHANNELS = 1
DTYPE = "int16"


def _resample_linear(x, src_rate, dst_rate):
    """线性插值重采样，用于设备原生采样率不是 16kHz 的情况。"""
    if src_rate == dst_rate:
        return x
    src = np.asarray(x, dtype=np.float32).reshape(-1)  # 强制 1D，避免广播爆炸
    n_out = int(round(len(src) * dst_rate / src_rate))
    src_idx = np.linspace(0.0, len(src) - 1, n_out)
    lo = src_idx.astype(np.int32)
    hi = np.minimum(lo + 1, len(src) - 1)
    frac = src_idx - lo  # 1D 插值位置小数部分；用 [:, None] 会变成 N×N 广播导致 OOM
    return (src[lo] * (1.0 - frac) + src[hi] * frac).astype(np.int16)


def _resolve_device(arg):
    """把 --device 参数解析成设备索引。

    支持: 数字索引、名称关键字（如 HECATE）、缺省时自动选择 USB 输入设备。
    """
    def _inputs():
        return [d for d in sd.query_devices() if d["max_input_channels"] > 0]

    if arg is None:
        for d in _inputs():
            if "usb" in d["name"].lower():
                print(f"[mic] auto-selected device {d['index']}: {d['name']}")
                return d["index"]
        for d in _inputs():
            print(f"[mic] auto-selected device {d['index']}: {d['name']}")
            return d["index"]
        print("[mic] no input device found", file=sys.stderr)
        sys.exit(1)
    if arg.isdigit():
        return int(arg)
    for d in _inputs():
        if arg.lower() in d["name"].lower():
            print(f"[mic] matched device {d['index']}: {d['name']}")
            return d["index"]
    print(f"[mic] no input device matching '{arg}'", file=sys.stderr)
    sys.exit(1)


class MicSender:
    """TCP 发送端，与 app.py 的 mic_tcp_receiver 对应。

    音频回调只负责入队（有界队列，满时丢最旧保持实时），
    独立线程负责连接 + 发送，断线自动重连。
    """

    def __init__(self, target: str):
        host, port = target.rsplit(":", 1)
        self.addr = (host, int(port))
        self.q = queue.Queue(maxsize=512)
        threading.Thread(target=self._run, daemon=True).start()
        print(f"[mic] target -> {target}")

    def send_audio(self, data: bytes):
        try:
            self.q.put_nowait(data)
        except queue.Full:
            # 连接断开堆积时丢弃最旧数据，保持实时性
            try:
                self.q.get_nowait()
                self.q.put_nowait(data)
            except queue.Empty:
                pass

    def _run(self):
        sock = None
        while True:
            data = self.q.get()
            if sock is None:
                try:
                    sock = socket.create_connection(self.addr, timeout=5)
                    print(f"[mic] connected to {self.addr[0]}:{self.addr[1]}")
                except OSError as e:
                    print(f"[mic] connect failed: {e}, retry")
                    time.sleep(2)
                    continue
            pkt = struct.pack(">BI", 0, len(data)) + data
            try:
                sock.sendall(pkt)
            except OSError:
                try:
                    sock.close()
                except OSError:
                    pass
                sock = None


def main():
    parser = argparse.ArgumentParser(
        description="Remote microphone sender (16kHz mono int16, TCP)"
    )
    parser.add_argument(
        "--target",
        type=str,
        default=None,
        help="运行 app.py 的机器地址:端口，对应 app.py 的 --mic_tcp_port，"
        "如 192.168.1.100:55559",
    )
    parser.add_argument(
        "--device",
        type=str,
        default=None,
        help="输入设备：索引号（--list 查看）或名称关键字（如 HECATE），默认自动选 USB 麦克风",
    )
    parser.add_argument("--list", action="store_true", help="列出所有音频设备")
    parser.add_argument(
        "--chunk_ms", type=int, default=20, help="每个包对应的音频时长（毫秒）"
    )
    args = parser.parse_args()

    if args.list:
        print(sd.query_devices())
        print("Default input device:", sd.default.device)
        sys.exit(0)

    if not args.target:
        parser.error("the following arguments are required: --target")

    sender = MicSender(args.target)

    device_index = _resolve_device(args.device)
    device_info = sd.query_devices(device_index, kind="input")
    native_rate = int(device_info["default_samplerate"])

    # 直接用设备原生采样率打开（USB 麦克风常不支持 16kHz，强行尝试会触发 ALSA 报错），在回调里重采样
    stream_rate = native_rate
    if native_rate != SAMPLE_RATE:
        print(f"[mic] device {native_rate}Hz, will resample to {SAMPLE_RATE}Hz")

    # 音频内容统计：每 2 秒打印一次已发送的帧/字节/电平，并追加到日志文件
    stats = {"frames": 0, "bytes": 0, "total": 0, "n": 0, "rms": 0.0, "peak": 0, "last": time.time()}
    log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "remote_mic_sent.log")

    def callback(indata, frames, time_info, status):
        if status:
            print(f"[mic] status: {status}", file=sys.stderr)
        if stream_rate != SAMPLE_RATE:
            data = _resample_linear(indata, stream_rate, SAMPLE_RATE).tobytes()
        else:
            data = indata.tobytes()
        sender.send_audio(data)
        a = np.frombuffer(data, dtype=np.int16).astype(np.float32)
        stats["frames"] += 1
        stats["bytes"] += len(data)
        stats["total"] += len(data)
        stats["n"] += 1
        stats["rms"] += float(np.sqrt(np.mean(a * a)))
        stats["peak"] = max(stats["peak"], float(np.abs(a).max()))
        now = time.time()
        if now - stats["last"] >= 2.0:
            rms_avg = stats["rms"] / max(stats["n"], 1)
            db = 20 * np.log10(rms_avg / 32768 + 1e-12)
            line = (
                f"[mic] sent {stats['frames']} frames / {stats['bytes']} bytes, "
                f"total={stats['total']} bytes, "
                f"peak={stats['peak']:.0f}, rms={rms_avg:.0f} ({db:+.0f} dB)"
            )
            print(line, flush=True)
            with open(log_path, "a") as f:
                f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} {line}\n")
            stats.update(frames=0, bytes=0, n=0, rms=0.0, peak=0, last=now)

    def _open_stream(rate):
        return sd.InputStream(
            samplerate=rate,
            channels=CHANNELS,
            dtype=DTYPE,
            device=device_index,
            blocksize=int(rate * args.chunk_ms / 1000),
            callback=callback,
        )

    stream_ctx = _open_stream(stream_rate)

    with stream_ctx:
        print(
            f"[mic] capturing {stream_rate}Hz -> {SAMPLE_RATE}Hz, "
            f"chunk={args.chunk_ms}ms, device={device_info['name']}"
        )
        print("Press Ctrl+C to stop")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("[mic] stopped")


if __name__ == "__main__":
    main()
