#!/usr/bin/env python3
"""
远端播放器：接收 4090 推送的实时 TTS 音频流并在本机扬声器播放。

用法:
    pip install websockets sounddevice
    python remote_player.py --host <4090机器IP> [--port 55556]

收到的数据格式（与 /ws 网页收到的完全一致）:
    - 二进制帧 : 裸 int16 小端 PCM，24000 Hz，单声道（无需解码，直接送扬声器）
    - JSON 帧  : {"event": "stop_audio", "data": {...}} 等控制事件

无 PCM 流时的行为:
    - 对话间隙（服务端没有输出）: 播放队列为空，静默等待，下一段音频到来自动恢复
    - 打断（收到 stop_audio）  : 清空播放队列，立即停止未播出的缓冲
    - 断线                      : 自动重连；重连前队列耗尽则自然停播
"""
import argparse
import asyncio
import json
import queue
import threading
import time

import sounddevice as sd
import websockets

SAMPLE_RATE = 24000  # 与 4090 侧 TTS 输出一致
CHANNELS = 1
DTYPE = "int16"

audio_q = queue.Queue()


def player_worker():
    """播放线程：从队列取 PCM 写入扬声器；队列空则静默等待。"""
    # 注意：若声卡不支持 24000 Hz，可把采样率改为设备支持的（如 48000），
    # 并用 soxr 把收到的 24k 数据重采样后再写入。
    with sd.RawOutputStream(
        samplerate=SAMPLE_RATE, channels=CHANNELS, dtype=DTYPE
    ) as out:
        print(f"[player] opened output: {SAMPLE_RATE} Hz / {CHANNELS} ch / {DTYPE}")
        while True:
            try:
                chunk = audio_q.get(timeout=0.5)
            except queue.Empty:
                # 没有新的 PCM 数据：不输出，保持静默
                continue
            out.write(chunk)


def clear_queue():
    with audio_q.mutex:
        audio_q.queue.clear()


async def run(ws_url):
    while True:
        try:
            async with websockets.connect(ws_url, max_size=None) as ws:
                print(f"[relay] connected: {ws_url}")
                async for msg in ws:
                    if isinstance(msg, bytes):
                        audio_q.put(msg)
                    else:
                        try:
                            payload = json.loads(msg)
                        except json.JSONDecodeError:
                            continue
                        event = payload.get("event")
                        if event == "stop_audio":
                            # 打断：丢弃已缓冲但未播放的音频，立即停止
                            clear_queue()
                            print("[relay] stop_audio -> buffer cleared")
                        else:
                            print(f"[relay] event: {event}")
        except Exception as e:
            print(f"[relay] connection lost: {e}")
        time.sleep(2)  # 断线重连


def main():
    parser = argparse.ArgumentParser(description="Remote TTS stream player")
    parser.add_argument("--host", required=True, help="4090 机器 IP")
    parser.add_argument("--port", type=int, default=55556)
    args = parser.parse_args()

    threading.Thread(target=player_worker, daemon=True).start()

    ws_url = f"ws://{args.host}:{args.port}/ws/relay"
    try:
        asyncio.run(run(ws_url))
    except KeyboardInterrupt:
        print("\n[player] stopped")


if __name__ == "__main__":
    main()
