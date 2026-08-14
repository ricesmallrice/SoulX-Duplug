import os, sys
import grpc
import requests
import io, wave
import time
import re
import threading

# Add path for CosyVoice to sys.path if not present
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
COSY_GRPC_PATH = os.path.abspath(
    os.path.join(CURRENT_DIR, "../modules/CosyVoice/async_cosyvoice/runtime/async_grpc")
)

if COSY_GRPC_PATH not in sys.path:
    sys.path.append(COSY_GRPC_PATH)

try:
    import cosyvoice_pb2
    import cosyvoice_pb2_grpc
except ImportError:
    print(
        f"Warning: Failed to import cosyvoice_pb2 from {COSY_GRPC_PATH}. CosyVoice client will not work."
    )

from modules.utils.MyTn.cn_tn import TextNorm
from modules.utils.text_utils import split_cn_en


class IndexTTS_VLLM:
    def __init__(self, speaker: str = "elva", api_url: str = "http://0.0.0.0:6006/tts"):
        self.speaker = speaker
        self.api_url = api_url
        self.normalizer = TextNorm()
        self.MORE_PUNCT = "'\",;:、，；：\n"
        # 打断支持：记录当前正在进行的 TTS 请求，abort() 时关闭连接
        self._active = None
        self._lock = threading.Lock()

        data = {"text": "This is for warm up!", "character": self.speaker}
        try:
            for _ in range(10):
                response = requests.post(self.api_url, json=data)
        except Exception:
            pass

    def synthesize(self, text: str, sample_rate: int = 24000, streaming=None):
        text = self.normalizer(text).replace("*", "").replace("-", " ").strip()
        if text[-1] in self.MORE_PUNCT:
            text = text[:-1]
        if not text:
            return

        data = {"text": text, "character": self.speaker}

        try:
            # stream=True 以便保留 response 句柄：打断时 abort() 可关闭连接，
            # 让服务端 is_disconnected 生效，停止剩余句子合成（行为与原一致，正常路径同阻塞等待）
            response = requests.post(self.api_url, json=data, stream=True)
            with self._lock:
                self._active = response
        except Exception as e:
            print(f"IndexTTS_VLLM inference failed: {e}")
            return

        # Convert wav bytes to int16 pcm
        try:
            try:
                with io.BytesIO(response.content) as wav_buffer:
                    with wave.open(wav_buffer, "rb") as wav_file:
                        yield wav_file.readframes(wav_file.getnframes())
            finally:
                with self._lock:
                    if self._active is response:
                        self._active = None
        except Exception as e:
            print(f"Failed to convert WAV to PCM: {e}")
            # Fallback or return empty bytes if conversion fails
            return

    def abort(self):
        """打断时调用：关闭正在进行的 TTS 请求连接，让服务端检测到断开并停止合成。"""
        with self._lock:
            resp = self._active
        if resp is not None:
            try:
                resp.close()
            except Exception:
                pass


class Cosyvoice_Streaming_VLLM:
    def __init__(self, host: str = "localhost", port: int = 6006):
        self.host = host
        self.port = port
        self.normalizer = TextNorm()
        self.MORE_PUNCT = "'\",!?;:、，！？；：\n"
        self.streaming_threshold = 10

        try:
            for i in range(10):
                response = self.synthesize("This is for warm up!")
                for _ in response:
                    pass
        except Exception:
            pass

    def detect_language_accent(self, text: str) -> str:
        if not text:
            return "elva-female-zh"

        # Count Chinese characters
        zh_chars = re.findall(r"[\u4e00-\u9fff]", text)
        num_zh = len(zh_chars)

        # Count English words
        en_words = re.findall(r"[a-zA-Z]+", text)
        num_en = len(en_words)

        if num_zh == 0:
            return "elva-female-en"

        if num_zh >= num_en:
            return "elva-female-zh"

        return "elva-female-en"

    def synthesize(self, text: str, sample_rate: int = 24000, streaming=None):
        text = self.normalizer(text).replace("*", "").replace("-", " ").strip()
        if text[-1] in self.MORE_PUNCT:
            text = text[:-1]
        if not text:
            return

        if streaming is None:
            streaming = False
            if len(split_cn_en(text)) >= self.streaming_threshold:
                streaming = True

        try:
            with grpc.insecure_channel(f"{self.host}:{self.port}") as channel:
                stub = cosyvoice_pb2_grpc.CosyVoiceStub(channel)

                req = cosyvoice_pb2.Request()
                req.tts_text = text
                req.stream = streaming
                req.speed = 1.0
                req.text_frontend = True
                req.format = "pcm"
                req.zero_shot_by_spk_id_request.spk_id = self.detect_language_accent(
                    text
                )

                for response in stub.Inference(req):
                    if response.tts_audio:
                        # int16 pcm
                        yield response.tts_audio

        except Exception as e:
            print(f"CosyVoice streaming inference failed: {e}")
            return


# test
if __name__ == "__main__":
    import time

    tts = IndexTTS_VLLM("elva", api_url="http://0.0.0.0:6006/tts")
    text = "Next time you can go earlier or choose a time with fewer people."
    start_time = time.time()
    wav = tts.synthesize(text)
    end_time = time.time()
    print(f"Response time: {end_time - start_time} seconds")
