# SoulX-Duplug 项目导读

> 本文档为 SoulX-Duplug 全双工语音对话系统的完整技术说明，涵盖系统架构、代码组织、部署运行与运维排障，自包含全部部署与运维细节。

***

## 1. 项目简介

一个**全双工实时语音对话系统**：用户说话的同时 AI 就能响应（可以随时打断、边听边说），不是一问一答的传统对话。

- 底层基于开源 [SoulX-Duplug](https://github.com/Soul-AILab/SoulX-Duplug)，克隆自 `git clone https://github.com/Soul-AILab/SoulX-Duplug.git`，切换到 `dialogue-system` 分支
- 本仓库在开源基础上做了大量改造：
  - **ASR 云端化**：本地每块级联 SenseVoice 管状态机，说完时整段上讯飞 AIChain 拿整句文本+语言
  - **远程音频链路**：远端麦克风（remote\_mic）输入 + 远端播放器（remote\_player）输出，可无浏览器部署
  - **headless 模式**：不依赖网页，服务端参数决定模式
  - **可对接 Audio2Face**：播放端转发音频给口型推理，实现头部舵机同步

## 2. 项目档案

| 字段   | 内容                                                                                                                                                                                                                                                                  |
| ---- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 部署机器 | ubuntu（IP: 172.88.88.14，用户 hjadmin，CUDA 12.4）                                                                                                                                                                                                                       |
| 分支   | dialogue-system                                                                                                                                                                                                                                                     |
| 项目路径 | /nfs/kubeflow-data/SoulX/SoulX-Duplug                                                                                                                                                                                                                               |
| 关键配置 | 端口: VAD 8000 / LLM 6007 / TTS 6006 / Dialogue 55556；GPU: 0=VAD+TTS，1=LLM；config/config.yaml（chunk\_size=5120、max\_wait\_num=4、asr=sensevoice）；api.py 讯飞凭证（APP\_ID/APP\_KEY/SN/SCENE=main/STT\_ENGINE\_ID=3）；Dialogue 启动参数 --tcp\_target/--mic\_tcp\_port/--headless |
| 经验总结 | ①句首连发 stop 是 mistake\_len 过渡期正常现象 ②8000 看似"没启动无日志"=模型加载慢，等加载完才打印 Uvicorn running ③"no active session, dropping mic audio"=未带 --headless 且无浏览器会话                                                                                                                     |

## 3. 系统架构

### 3.1 服务器与运行环境

- 服务器: ubuntu（IP: 172.88.88.14）
- 用户: hjadmin，CUDA 版本: 12.4
- 主环境: `SoulX-Duplug/.venv`（Python 3.10.16，VAD/LLM/Dialogue 共用）
- TTS 环境: `SoulX-Duplug/.venv-tts`（Python 3.12，TTS 独立）

### 3.2 服务组成

| 服务              | 端口    | 作用                                     | 运行位置                  |
| --------------- | ----- | -------------------------------------- | --------------------- |
| VAD Server      | 8000  | 语音活动检测 + 状态预测（SoulX-Duplug）+ 级联/云端 ASR | 4090（172.88.88.14）    |
| LLM Server      | 6007  | Qwen2.5-7B-Instruct 文本生成               | 4090                  |
| TTS Server      | 6006  | IndexTTS-vLLM 语音合成                     | 4090                  |
| Dialogue System | 55556 | 对话管线（ASR→LLM→TTS）+ 前端 + TCP 音频推送       | 4090                  |
| remote\_mic     | —     | 采集远端麦克风，TCP 推给 55556（55559 端口）         | NX 机器人（172.66.88.206） |
| remote\_player  | 1212  | 接收 TTS 音频播放，可选转发给 A2F 做口型              | NX 机器人                |

### 3.3 数据流

```
【上行：用户说话 → 文本】
远端麦克风(remote_mic) / 浏览器
  → 55556 (mic_tcp_port=55559 或 WS)
  → 8000 VAD Server：每块本地 SenseVoice 级联（管状态机）+ 说完时整段上云(讯飞 AIChain)拿整句文本
  → 文本回 55556 → LLM(6007) → TTS(6006)

【下行：AI 回复 → 声音】
TTS 音频(24kHz int16)
  → 55556 TcpAudioSender → TCP → remote_player(1212) 播放
  →（可选 --forward）→ audio_face_stream → A2F 口型 → ROS → 头部舵机
```

### 3.4 TCP 帧协议（55556 ↔ 远端设备）

```
[type:1B][len:4B big-endian][payload]
type=0 音频（int16 PCM）
type=1 控制（0=stop 打断 / 1=pause / 2=resume）
```

- `remote_mic.py` 上行音频：16kHz int16 单声道
- `remote_player.py` 下行音频：24kHz int16 单声道
- 打断：55556 发 stop 控制帧 → 播放端清缓冲立即静音；TCP 断连同样兜底静音

## 4. 目录结构

```
SoulX-Duplug/
├── server.py                  # VAD Server 入口（uvicorn server:app，端口8000）
├── api.py                     # 讯飞 AIChain 云端 STT 客户端（recognize_pcm 整段识别）
├── service/                   # VAD 服务层（8000 内部逻辑）
│   ├── session.py             #   会话状态机（feed_audio 入口）
│   ├── engine.py              #   引擎调度（process）
│   └── model.py               #   核心：TurnTaking 状态机 + 每块级联 ASR + 云端整句识别(_recognize_utterance)
├── model/
│   └── asr.py                 # 本地 ASR 实现：ParaformerASR / SensevoiceASR / WhisperASR
├── config/
│   └── config.yaml            # VAD 配置（chunk_size、max_wait_num、asr 模型等）
├── dialogue_system/           # 对话系统（55556）
│   ├── app.py                 #   主入口：ASR→LLM→TTS 管线、barge-in、TCP 音频推送
│   ├── clients/
│   │   ├── llm_client.py      #   LLM 客户端（3 类，SYSTEM_PROMPT 人设"夏澜"）
│   │   ├── vad_client.py      #   8000 VAD 的 WS 客户端（拿状态/文本/语言）
│   │   └── tts_client.py      #   TTS 客户端
│   ├── frontend/              #   网页前端（index.html / script.js / processor.js / style.css）
│   ├── modules/
│   │   ├── index_tts_vllm/    #   IndexTTS-vLLM 服务（api_server.py，端口6006）
│   │   ├── qwen_llm/          #   Qwen2.5-7B LLM 服务（llm_server.py，端口6007）
│   │   ├── CosyVoice/         #   备选 TTS（Async CosyVoice）
│   │   └── utils/             #   backchannel_utils.py（"嗯"类语气词）、MyTn 文本归一化
│   ├── eval/                  #   全双工评估脚本
│   └── deploy.sh / offline_infer.py
├── remote_player.py           # 远端播放器（仓库内为参考副本，实际部署见 7.4）
├── remote_mic.py              # 远端麦克风（同上）
├── history.md                 # 远程音频链路改造的完整对话历史
└── dialogue_history.md        # ASR 云端化的排障与决策记录
```

## 5. 核心代码速查

| 文件                                                                                                               | 干什么                                                                                                           | 关键函数/位置                                                                                           |
| ---------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------- |
| [service/model.py](service/model.py)                                                                             | VAD 状态机：`<\|user_idle\|>/<\|user_nonidle\|>/<\|user_backchannel\|>/<\|user_complete\|>/<\|user_incomplete\|>` | `_asr()` 每块级联；`_recognize_utterance()` 云端整句+降级；speak 出口两处                                         |
| [api.py](api.py)                                                                                                 | 讯飞 AIChain WS STT：鉴权/建会话/推整段音频/收结果                                                                            | `recognize_pcm(pcm_bytes)` → `(text, language)`                                                   |
| [dialogue\_system/app.py](dialogue_system/app.py)                                                                | 对话管线调度                                                                                                        | `pipeline_worker`（ASR→LLM→TTS）；`TcpAudioSender`（推流+stop）；`interrupt()` 打断；`mic_vad_worker`（远端麦克风） |
| [dialogue\_system/clients/llm\_client.py](dialogue_system/clients/llm_client.py)                                 | LLM 调用 + 人设 prompt                                                                                            | 3 个类各有一份 `SYSTEM_PROMPT`，改人设需三处同步                                                                 |
| [dialogue\_system/clients/vad\_client.py](dialogue_system/clients/vad_client.py)                                 | 连接 8000 取状态                                                                                                   | `process()` 返回文本/语言                                                                               |
| [dialogue\_system/modules/index\_tts\_vllm/api\_server.py](dialogue_system/modules/index_tts_vllm/api_server.py) | TTS 服务                                                                                                        | `/tts` 整段合成                                                                                       |
| [config/config.yaml](config/config.yaml)                                                                         | VAD 参数                                                                                                        | `chunk_size=5120`、`max_wait_num=4`、`asr.model_name=sensevoice`                                    |

## 6. 环境与模型

### 6.1 环境准备（两个独立 Python 环境）

```bash
# 1. 主环境（VAD / LLM / Dialogue System），在项目根目录执行
uv venv --python 3.10.16 --seed
source .venv/bin/activate
pip install -r requirements.txt

# 2. TTS 环境（IndexTTS-vLLM）
uv venv .venv-tts --python 3.12 --seed
source .venv-tts/bin/activate
pip install -r dialogue_system/modules/index_tts_vllm/requirements.txt
```

### 6.2 模型下载（统一放 `pretrained_models/`，国内用 modelscope）

```bash
# SoulX-Duplug 模型（含 Qwen3-0.6B + GLM tokenizer + LoRA 权重）
huggingface-cli download --resume-download Soul-AILab/SoulX-Duplug-0.6B --local-dir ../pretrained_models
modelscope download --model Soul-AILab/SoulX-Duplug-0.6B --local_dir ../pretrained_models   # 或国内

# Qwen2.5-7B-Instruct（LLM 对话模型）
modelscope download --model Qwen/Qwen2.5-7B-Instruct --local_dir ../pretrained_models/Qwen2.5-7B-Instruct

# Index-TTS-1.5-vLLM（语音合成模型）
modelscope download --model kusuriuri/Index-TTS-1.5-vLLM --local_dir ../pretrained_models/Index-TTS-1.5-vLLM
```

模型目录结构：

```
pretrained_models/
├── Qwen3-0.6B-expand_vocab_v2/
├── glm-4-voice-tokenizer/
├── SoulX-Duplug/
│   └── SoulX-Duplug-0.6B-Bilingual.pth
├── Qwen2.5-7B-Instruct/
└── Index-TTS-1.5-vLLM/
```

## 7. 运行方式

### 7.1 一键启动四个服务（按顺序，四个终端）

```bash
# TTS Server（GPU 0）
source SoulX-Duplug/.venv-tts/bin/activate
cd SoulX-Duplug/dialogue_system/modules/index_tts_vllm
CUDA_VISIBLE_DEVICES=0 python api_server.py --host 0.0.0.0 --port 6006 --model_dir ../../../pretrained_models/Index-TTS-1.5-vLLM --gpu_memory_utilization 0.35

# LLM Server（GPU 1）
source SoulX-Duplug/.venv/bin/activate
cd SoulX-Duplug/dialogue_system/modules/qwen_llm
CUDA_VISIBLE_DEVICES=1 python llm_server.py --host 0.0.0.0 --port 6007 --model_dir ../../../pretrained_models/Qwen2.5-7B-Instruct

# VAD Server（GPU 0）
source SoulX-Duplug/.venv/bin/activate
cd SoulX-Duplug
CUDA_VISIBLE_DEVICES=0 uvicorn server:app --host 0.0.0.0 --port 8000 --workers 1

# Dialogue System（无 GPU 需求）
source SoulX-Duplug/.venv/bin/activate
cd SoulX-Duplug/dialogue_system
python app.py --tcp_target 172.66.88.206:1212 --mic_tcp_port 55559 --headless
```

浏览器模式：`python app.py`，访问 `http://172.88.88.14:55556`。

### 7.2 Dialogue System 启动参数

> 若 `app.py` 不加任何参数，则直接访问 `http://localhost:55556` 使用浏览器模式。

| 参数                     | 含义                                                                                        |
| ---------------------- | ----------------------------------------------------------------------------------------- |
| `--tcp_target ip:port` | 远端播放器地址（对应 remote\_player.py 的 --port），app 主动连接并推送音频流，支持即时打断；不设则语音无处播放                    |
| `--mic_tcp_port PORT`  | 接收远端麦克风 TCP 音频的本地端口（配合 remote\_mic.py --target）                                           |
| `--headless`           | 无浏览器模式，自动创建常驻会话。音频输入走远端麦克风（必须带 --mic\_tcp\_port，否则报错退出）、输出走远端播放器（建议带 --tcp\_target，不带仅警告） |

> 注: 不带 `--headless` 时若没有浏览器会话，远端麦克风音频会触发 `"no active session, dropping mic audio"` 告警（音频无处归属）。

### 7.3 服务详情

#### VAD Server（8000）

核心功能：音频流语音活动检测（VAD）、LLM Check（判断是否有效人声）、Cascade ASR（流式识别）、State Prediction（说话中/空闲/打断）、全双工实时处理。

配置文件 `config/config.yaml` 重要配置项：

| 配置                    | 含义                      |
| --------------------- | ----------------------- |
| chunk\_size           | 每帧音频采样数（5120 = 320ms）   |
| audio\_back\_size     | 向后上下文窗口（15360 = 960ms）  |
| audio\_ahead\_size    | 向前上下文窗口（640 = 40ms）     |
| max\_wait\_num        | 最大等待帧数（当前 4，越大越稳但延迟越高）  |
| max\_mistake\_num     | 连续"无语音"次数阈值（3 次后判定语音结束） |
| far\_field\_threshold | 远场检测阈值                  |

ASR 模型：`sensevoice`（多语言+自动语种检测，**当前使用**）/ `paraformer`（中文优化）/ `whisper`（多语言，可选 large）。

#### LLM Server（6007）

基于 Qwen2.5-7B-Instruct，流式输出（SSE），集成 TTS 客户端。

- `POST /chat`：文本生成（text/event-stream）
- `POST /chat_indextts`：LLM+TTS 联合生成（二进制流）
- 提示词：`dialogue_system/clients/llm_client.py` 的 3 个类（`QwenLLM_stream` / `QwenLLM_IndexTTS_stream` / `QwenLLM_Cosyvoice_stream`）各有一份一致的 `SYSTEM_PROMPT`，**改人设需三处同步**，已内置"语言跟随"指令。

#### TTS Server（6006）

IndexTTS-vLLM 文本到语音合成。

#### Dialogue System（55556）

前端文件：`frontend/index.html`（结构）/ `script.js`（WebSocket 音频传输）/ `processor.js`（音频处理）/ `style.css`。核心功能：实时语音采集与播放（Web Audio API）、WS 双向音频流、对话状态展示、中断检测与响应。

#### 云端语音识别 - 讯飞 AIChain WebSocket STT（ASR 云端化）

- 用户说完（speak 出口）时，将整段 `buffer_for_asr` 音频打包上传讯飞 AIChain STT 接口，返回整句最终文本 + 语言类型
- 本地每块级联 SenseVoice 保留（负责 VAD 状态机判断），云端只负责最终整句文本；失败/超时自动降级本地识别
- 代码：`api.py`（`recognize_pcm()` 整段识别入口）、`service/model.py`（`_recognize_utterance()`，float32→int16 PCM 转换 + 云端/本地切换）
- 配置项（api.py 顶部常量）：`BASE_URL=wss://aichain-sh.xfyun.cn`、`APP_ID/APP_KEY`（讯飞开放平台凭证）、`SN=test_sn`、`SCENE=main`、`STT_ENGINE_ID=3`、`SAMPLE_RATE=16000`、`RESULT_TIMEOUT_SECONDS=15`（超时触发本地降级）
- 鉴权：`sha256(APP_KEY + curtime)` checksum 拼在 WS URL；协议：`conversation.user.append` 推音频（base64+endFlag）、`stt.result` 增量返回、`event.cid_end` 结束；`session.config` 关闭服务端 VAD/turnDetection，靠客户端 endFlag 切句

### 7.4 远端工具（在 NX 机器人 172.66.88.206 上运行）

> `remote_player.py` / `remote_mic.py` 的实际位置（不在本仓库）：
> `/home/robot/head_ws/src/head_node_py/src/head_node_py/head_node_py/tools`

```bash
cd /home/robot/head_ws/src/head_node_py/src/head_node_py/head_node_py/tools

# 播放端（可选 --forward 转发给 A2F 口型，--forward-gate 严格同步）
python remote_player.py --port 1212 --device HECATE \
  --forward 127.0.0.1:1213 --forward-gate

# 麦克风端
python remote_mic.py --target 172.88.88.14:55559 --device "AI Wireless"
python remote_mic.py --list   # 列出本机音频设备
```

## 8. 日志监控

### 8.1 本地 vs 云端区分

| 日志前缀 | 含义 |
|---|---|
| `[TurnTiming]` | VAD 每块耗时：Enc/Chk/ASR/St/Total；`VAD Start/End`；`VAD End (api)/(local)` 标整句文本来源 |
| `[Timing] Cascade ASR` | **本地**每块级联识别耗时 |
| `[SensevoiceLang]` | **本地** SenseVoice 语种+文本 |
| `[CloudSTT]` | **云端**讯飞识别：OK/FAIL、总耗时、Lang、Text，失败降级本地 |
| `[TIMING] / [PARTIAL] / [FINAL-FRAME]` | api.py 云端识别内部过程 |
| `[system] ASR result / [client_id]` | 55556 侧：每轮 ASR 结果、LLM/TTS/E2E 各段耗时 |

一句话：`[Timing]/[SensevoiceLang]` = 本地；`[CloudSTT]` = 云端。

### 8.2 Dialogue System 终端输出（TurnTiming 时延监控）

| 字段                    | 含义                                         |
| --------------------- | ------------------------------------------ |
| VAD Start / VAD End   | 语音活动检测的开始/结束时间点                            |
| Enc                   | Encoder 编码时延（语音特征提取）                       |
| Chk                   | Check 时延（LLM 检查/推理）                        |
| ASR                   | 语音识别时延（语音→文本）                              |
| St                    | State 状态预测时延                               |
| Total                 | 该轮从 VAD End 到处理完成的总时延                      |
| No speech             | VAD 检测到无语音，该轮不产生有效文本                       |
| SensevoiceLang        | 语种检测结果，<\|zh\|>=中文，<\|en\|>=英文，<\|ko\|>=韩文 |
| Text                  | 识别出的文本内容                                   |
| CloudSTT              | 云端讯飞AIChain识别结果：OK/FAIL、总耗时、Lang(语言)、Text  |
| VAD End (api)/(local) | 最终整句文本来源标记：(api)=云端成功，(local)=云端失败降级本地     |

### 8.3 App 日志（E2E 流程监控）

| 字段                           | 含义                                                                  |
| ---------------------------- | ------------------------------------------------------------------- |
| VAD Process                  | VAD 处理耗时（从音频中截取有效语音段的时间）                                            |
| Utterance                    | VAD 切分出的用户语音片段文本                                                    |
| ASR result                   | 语音识别最终输出的文本结果                                                       |
| LLM Chunk                    | LLM 返回的一个流式文本片段（streaming chunk）                                    |
| LLM TTFT                     | Time To First Token — 从请求发出到收到 LLM 第一个 token 的耗时                    |
| TTS First Audio              | 从 TTS 开始到产出第一帧音频的耗时                                                 |
| E2E First Audio              | 端到端首帧音频时延（从用户说完到 AI 发出第一个声音的总耗时）                                    |
| Interrupt Latency (TTS loop) | TTS 播放循环中检测到打断并停止播放的延迟                                              |
| Interrupt Latency (post-TTS) | TTS 停止后后续清理/切换状态的额外延迟                                               |
| LLM Total                    | LLM 整个请求的总耗时                                                        |
| TTS Total                    | TTS 整个合成的总耗时                                                        |
| E2E Complete                 | 端到端完整一轮总耗时（用户说话结束到 AI 完整回复结束）                                       |
| Interrupted mid-stream       | 本轮被用户打断。Ratio 表示已播放比例（如 0.30=只播了 30% 被打断），Truncated message 为被截断的文本 |

## 9. 经验总结

1. 句首连发几个 stop 是 `mistake_len` 过渡期的**正常现象**，无需处理
2. 8000 端口看似"没启动无日志"= 模型加载慢，等加载完才打印 `Uvicorn running`
3. `no active session, dropping mic audio` = 没带 `--headless` 且无浏览器会话

