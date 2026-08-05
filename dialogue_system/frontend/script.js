const startBtn = document.getElementById('startBtn');
const stopBtn = document.getElementById('stopBtn');
const voiceCircle = document.getElementById('voiceCircle');
const circleStatusEl = document.getElementById('circleStatus');
const waveformCanvas = document.getElementById('waveformCanvas');
const ctx = waveformCanvas.getContext('2d');

waveformCanvas.width = waveformCanvas.clientWidth;
waveformCanvas.height = 100;

let audioContext, analyser, dataArray, source, stream, processor;
let animationId = null;
let listening = false;
let remoteMicMode = false; // 服务端告知的麦克风模式：true = 使用 TCP 远端麦克风

// --- ASR Audio Context is separate from TTS Audio Context ---
let ttsContext = null;
let ttsNode = null;
let ttsSab = null;
let ttsFloat32Data = null;
let ttsStates = null; // Int32Array [writeIndex, readIndex]
// Buffer size: capacity for ~120 seconds at 24k
const TTS_BUFFER_SIZE = 24000 * 120;

// ==================== Native WebSocket ====================
// Determine protocol (ws or wss)
const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
const wsUrl = `${protocol}//${window.location.host}/ws`;

let socket = new WebSocket(wsUrl);
socket.binaryType = 'arraybuffer'; // Important for receiving audio

socket.onopen = () => {
  console.log('[WebSocket] connected');
};

socket.onclose = () => {
  console.log('[WebSocket] disconnected');
};

socket.onerror = (error) => {
  console.error('[WebSocket] error:', error);
};

// Handle incoming messages
socket.onmessage = (event) => {
  const data = event.data;

  // Check if data is binary (Audio Chunk)
  if (data instanceof ArrayBuffer) {
    handleAudioChunk(data);
    return;
  }

  // Otherwise handle JSON messages
  try {
    const msg = JSON.parse(data);
    const eventName = msg.event;
    const payload = msg.data;

    switch (eventName) {
      case 'connect_ack':
        console.log('Connection confirmed:', payload);
        break;
      case 'mic_mode':
        remoteMicMode = payload.mode === 'remote';
        console.log('[mic] mode =', payload.mode);
        break;
      case 'text_response':
        console.log('[LLM]', payload.text);
        break;
      case 'vad_loading':
        handleVadLoading(payload);
        break;
      case 'vad_status':
        if (vadStatusEl) {
          vadStatusEl.textContent = payload.message || payload.state || "Unknown Status";
          vadStatusEl.dataset.state = payload.state || "idle";
        }
        break;
      case 'circle_status':
        updateCircleState(payload.status);
        break;
      case 'user_transcription':
        handleUserTranscription(payload);
        break;
      case 'stop_audio':
        console.log('Received stop_audio signal');
        if (ttsNode) ttsNode.port.postMessage({ type: 'abort' });
        updateCircleState("LISTENING");
        break;
      case 'pause_audio':
        if (ttsNode) ttsNode.port.postMessage({ type: 'pause' });
        updateCircleState("LISTENING");
        break;
      case 'resume_audio':
        if (ttsNode) ttsNode.port.postMessage({ type: 'resume' });
        updateCircleState('SPEAKING');
        break;
      case 'remote_mic_waveform':
        // 远端麦克风音频（int16 base64）→ 画实时波形，确认远端音频链路
        drawRemoteMicWaveform(payload);
        break;
      default:
        console.log('Unknown event:', eventName);
    }
  } catch (e) {
    console.error('Failed to parse message:', e);
  }
};


// DOM elements
const vadStatusEl = document.getElementById("vadStatus");
const userTranscriptionEl = document.getElementById("userTranscription");

// Loading Overlay Elements
const loadingOverlay = document.getElementById("loadingOverlay");
const loadingMessage = document.getElementById("loadingMessage");
const loadingConfirmBtn = document.getElementById("loadingConfirmBtn");
const loadingSpinner = document.querySelector(".loading-spinner");
const loadingSuccess = document.querySelector(".loading-success");

function handleVadLoading({ state, message }) {
  loadingOverlay.classList.remove("hidden");
  loadingMessage.textContent = message;

  if (state === "loading") {
    loadingConfirmBtn.classList.add("hidden");
    loadingSpinner.classList.remove("hidden");
    loadingSuccess.classList.add("hidden");
  } else if (state === "ready") {
    loadingSpinner.classList.add("hidden");
    loadingSuccess.classList.remove("hidden");
    loadingConfirmBtn.classList.remove("hidden");
  }
}

// Close loading overlay
loadingConfirmBtn.addEventListener("click", async () => {
  loadingOverlay.classList.add("hidden");
  updateCircleState("READY");

  // Initialize TTS Audio Engine
  if (!ttsContext) {
    await initTTSAudioEngine();
  }
});

function handleUserTranscription({ text }) {
  if (!userTranscriptionEl) return;

  // Truncate logic: keep last N chars if too long
  const MAX_LEN = 20;
  let displayText = text || "No transcription yet";
  if (displayText.length > MAX_LEN) {
    let tail = displayText.slice(-MAX_LEN);

    // Ensure we don't cut off in the middle of a word
    const match = tail.match(/^[a-zA-Z]+/);
    if (match) {
      const extraLen = match[0].length;
      tail = displayText.slice(-(MAX_LEN + extraLen));
    }

    displayText = "..." + tail;
  }

  userTranscriptionEl.textContent = displayText;
  userTranscriptionEl.classList.add("updated");
  window.setTimeout(() => userTranscriptionEl.classList.remove("updated"), 400);
}

// ==================== TTS Engine (SharedArrayBuffer + AudioWorklet) ====================
async function initTTSAudioEngine() {
  try {
    // Create context with TTS specific sample rate (24k)
    ttsContext = new AudioContext({ sampleRate: 24000, latencyHint: 'interactive' });

    // Add AudioWorklet Module
    await ttsContext.audioWorklet.addModule('processor.js');

    // Initialize SharedArrayBuffer
    // Header (8 bytes) + Data (Float32 * SIZE)
    const sabSize = 8 + TTS_BUFFER_SIZE * 4;
    ttsSab = new SharedArrayBuffer(sabSize);

    // Views
    ttsStates = new Int32Array(ttsSab, 0, 2); // [0]: WriteIndex, [1]: ReadIndex
    ttsFloat32Data = new Float32Array(ttsSab, 8);

    // Init Indices
    Atomics.store(ttsStates, 0, 0);
    Atomics.store(ttsStates, 1, 0);

    // Create Worklet Node
    ttsNode = new AudioWorkletNode(ttsContext, 'tts-processor', {
      processorOptions: { sab: ttsSab }
    });

    // Handle messages from Worklet (e.g., playback finished)
    ttsNode.port.onmessage = (e) => {
      if (e.data.type === 'playback_ended') {
        updateCircleState('LISTENING');
        console.log('[TTS] playback finished');
      }
    };

    ttsNode.connect(ttsContext.destination);
    console.log("TTS Audio Engine Initialized");

  } catch (e) {
    console.error("Failed to init TTS Engine:", e);
    alert("Audio Engine initialization failed! Only supported in localhost or https. Check console for details.");
  }
}

// Handle Receive Audio Chunk (Binary)
function handleAudioChunk(arrayBuffer) {
  if (!ttsSab || !ttsFloat32Data) return;

  // Resume context if suspended
  if (ttsContext.state === 'suspended') {
    ttsContext.resume();
  }

  // Convert incoming Int16 PCM to Float32
  // Assuming backend sends raw Int16 bytes from WAV
  const inputInt16 = new Int16Array(arrayBuffer);
  const inputLen = inputInt16.length;

  // Load current Write Index
  let writeIndex = Atomics.load(ttsStates, 0);

  for (let i = 0; i < inputLen; i++) {
    // Normalize Int16 to Float32 [-1, 1]
    const sample = inputInt16[i] / 32768.0;

    // Circular buffer write
    const bufferIndex = writeIndex % TTS_BUFFER_SIZE;
    ttsFloat32Data[bufferIndex] = sample;

    writeIndex++;
  }

  // Update Write Index atomically
  Atomics.store(ttsStates, 0, writeIndex);

  updateCircleState('SPEAKING');
}

// ==================== Audio Capture ====================
startBtn.addEventListener('click', async () => {
  try {
    listening = true;
    updateCircleState("LISTENING");

    // Ensure TTS Engine is ready
    if (!ttsContext) await initTTSAudioEngine();
    if (ttsContext.state === 'suspended') ttsContext.resume();

    const useRemoteMic = remoteMicMode; // 服务端决定：app.py 带 --mic_tcp_port 则使用远端麦克风
    if (useRemoteMic) {
      // 远端麦克风模式：不采集本机麦克风，音频由 remote_mic.py 通过 TCP 直接喂给服务端
      if (socket.readyState === WebSocket.OPEN) {
        socket.send(JSON.stringify({
          event: "config_audio",
          data: { sample_rate: 16000, source: "remote_mic" }
        }));
      }
      startBtn.disabled = true;
      stopBtn.disabled = false;
      return;
    }

    stream = await navigator.mediaDevices.getUserMedia({ audio: true });

    // Separate context for recording to avoid sample rate mess
    audioContext = new (window.AudioContext || window.webkitAudioContext)({ sampleRate: 16000 });

    // Send Audio Configuration
    if (socket.readyState === WebSocket.OPEN) {
      socket.send(JSON.stringify({
        event: "config_audio",
        data: { sample_rate: audioContext.sampleRate }
      }));
    }

    // Ensure AudioContext is active (required by some browser policies)
    if (audioContext.state === 'suspended') {
      await audioContext.resume();
    }

    source = audioContext.createMediaStreamSource(stream);
    analyser = audioContext.createAnalyser();
    analyser.fftSize = 512;
    dataArray = new Uint8Array(analyser.frequencyBinCount);

    // Create ScriptProcessor to send in frames
    processor = audioContext.createScriptProcessor(512, 1, 1);
    source.connect(analyser);
    source.connect(processor);
    processor.connect(audioContext.destination);

    processor.onaudioprocess = e => {
      if (!listening) return;
      const input = e.inputBuffer.getChannelData(0);
      // Float32 -> Int16
      const int16 = new Int16Array(input.length);
      for (let i = 0; i < input.length; i++) {
        int16[i] = Math.max(-1, Math.min(1, input[i])) * 0x7fff;
      }

      // Native WebSocket Send (Binary)
      if (socket.readyState === WebSocket.OPEN) {
        socket.send(int16.buffer);
      }
    };

    startBtn.disabled = true;
    stopBtn.disabled = false;
    drawUserWaveform();
    pulseAssistantCircle();

  } catch (err) {
    console.error('Cannot access microphone:', err);
    alert("Cannot access microphone! Please check browser permissions.");
    updateCircleState("READY");
    startBtn.disabled = false;
  }
});

stopBtn.addEventListener('click', () => {
  listening = false;

  // Send Stop signal via JSON
  if (socket.readyState === WebSocket.OPEN) {
    socket.send(JSON.stringify({ event: 'duplex_stop' }));
  }

  // Interrupt TTS
  if (ttsNode) ttsNode.port.postMessage({ type: 'abort' });

  cancelAnimationFrame(animationId);
  if (stream) stream.getTracks().forEach(t => t.stop());
  if (processor) processor.disconnect();
  if (source) source.disconnect();
  voiceCircle.style.transform = 'scale(1)';
  ctx.clearRect(0, 0, waveformCanvas.width, waveformCanvas.height);
  startBtn.disabled = false;
  stopBtn.disabled = true;
  updateCircleState("READY");
});

// ==================== Draw Waveform ====================
function drawUserWaveform() {
  if (!listening) return;
  animationId = requestAnimationFrame(drawUserWaveform);

  analyser.getByteTimeDomainData(dataArray);
  ctx.fillStyle = '#0f172a';
  ctx.fillRect(0, 0, waveformCanvas.width, waveformCanvas.height);

  ctx.lineWidth = 2;
  ctx.strokeStyle = '#38bdf8';
  ctx.beginPath();

  const sliceWidth = waveformCanvas.width / dataArray.length;
  let x = 0;
  let lastY = waveformCanvas.height / 2;

  for (let i = 0; i < dataArray.length; i++) {
    const v = dataArray[i] / 128.0;
    const y = lastY + (v * waveformCanvas.height / 2 - lastY) * 0.2;
    if (i === 0) ctx.moveTo(x, y);
    else ctx.lineTo(x, y);
    lastY = y;
    x += sliceWidth;
  }
  ctx.stroke();
}

// 画远端麦克风输入波形（int16 base64），确认远端音频链路实时可见
function drawRemoteMicWaveform(b64) {
  if (!b64) return;
  try {
    const bin = atob(b64);
    const bytes = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
    const i16 = new Int16Array(bytes.buffer);
    const mid = waveformCanvas.height / 2;
    ctx.fillStyle = '#0f172a';
    ctx.fillRect(0, 0, waveformCanvas.width, waveformCanvas.height);
    ctx.lineWidth = 2;
    ctx.strokeStyle = '#4ade80';
    ctx.beginPath();
    for (let i = 0; i < i16.length; i++) {
      const x = (i / i16.length) * waveformCanvas.width;
      const y = mid - (i16[i] / 32768) * (waveformCanvas.height / 2 - 4);
      if (i === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    }
    ctx.stroke();
  } catch (e) {
    console.error('waveform decode error:', e);
  }
}

// ==================== Assistant Circle Animation ====================
let pulseDirection = 1;
function pulseAssistantCircle() {
  if (!listening) return;
  animationId = requestAnimationFrame(pulseAssistantCircle);

  // Only animate scale via JS if NOT in CSS-animated states (like speaking/processing)
  if (voiceCircle.classList.contains('state-speaking') || voiceCircle.classList.contains('state-processing')) {
    return;
  }

  let currentScale = parseFloat(voiceCircle.style.transform.replace('scale(', '').replace(')', '')) || 1;
  if (currentScale >= 1.05) pulseDirection = -1;
  if (currentScale <= 1) pulseDirection = 1;
  currentScale += pulseDirection * 0.002;
  voiceCircle.style.transform = `scale(${currentScale})`;
}

// ==================== Update Circle State ====================
function updateCircleState(state) {
  // Reset classes
  voiceCircle.classList.remove('state-speaking', 'state-listening', 'state-processing');

  let statusText = "READY";

  switch (state) {
    case 'LISTENING':
      voiceCircle.classList.add('state-listening');
      statusText = "LISTENING";
      break;

    case 'THINKING':
      voiceCircle.classList.add('state-processing');
      statusText = "THINKING";
      break;

    case 'SPEAKING':
      voiceCircle.classList.add('state-speaking');
      statusText = "SPEAKING";
      break;

    case 'READY':
    default:
      statusText = "READY";
      break;
  }

  if (circleStatusEl) circleStatusEl.textContent = statusText;
}
