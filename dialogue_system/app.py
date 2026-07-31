import re
import time
import soxr
import queue
import base64
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
        self._stop_event = threading.Event()  # Internal event to signal interruption
        self.is_active = True

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

    if ws.client_state != WebSocketState.CONNECTED:
        return

    # Helper wrapper to run async send in the main loop
    async def _send():
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

    asyncio.run_coroutine_threadsafe(_send(), main_loop)


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

                with session.lock:
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


# ==== Static Resource Routing ====
app.mount("/", StaticFiles(directory="frontend", html=True), name="frontend")


if __name__ == "__main__":
    import uvicorn

    logger.info(f"Server starting on http://localhost:{Config.PORT}")
    uvicorn.run(app, host="0.0.0.0", port=Config.PORT)
