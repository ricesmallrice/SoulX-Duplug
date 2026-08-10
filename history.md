# SoulX 远程音频链路改造 —— 完整对话历史记录

> 会话时间段：2026-08-03（北京时间，时区 Asia/Shanghai）
> 项目目录：`/nfs/kubeflow-data/SoulX/SoulX-Duplug`
> 核心文件：`dialogue_system/app.py`、`remote_mic.py`、`remote_player.py`、`dialogue_system/frontend/script.js`

---

## 一、任务背景

用户最初目标：**"不用电脑麦克风，用另一台机器的麦克风把音频转发过来"**。逐步演进为完整的远程音频链路改造：

1. 另一台机器的麦克风 → 4090 服务器（app.py）的音频转发（最初走 UDP，后改 TCP）
2. 解决机器人播放端 ALSA underrun、TTS 截断（只说前两个字/3 秒/5 秒）问题
3. 因"卡字严重"将 TTS 播放链路从 UDP 改为 TCP；随后麦克风输入链路也改为 TCP
4. 清除所有 UDP 代码，保持纯 TCP
5. `remote_player_udp.py` / `remote_mic_udp.py` 改名为 `remote_player.py` / `remote_mic.py`
6. 方向反转：app.py 改为主动推送（`--tcp_target`）到机器人（机器人监听 `--port 1212`）
7. 打断（barge-in）优化：stop 信号 → 断连即静音 → mute_flag → **stop 控制帧**
8. 移除前端"远程麦克风"按钮，改为服务端参数决定模式（`--mic_tcp_port`）
9. 排查"收不到音频"：根因是 `remote_mic.py` 重采样广播 bug
10. 新增 headless 模式（去掉 web 端依赖）

---

## 二、环境与机器

| 角色 | 机器 | IP | 端口 |
|---|---|---|---|
| 主服务（app.py + VAD/LLM/TTS） | 4090 服务器 | 172.88.88.14 | 55556（Web/WS）、55559（收远端麦克风）、8000（VAD）、6006（TTS）、6007（LLM） |
| 远端麦克风 + 播放（remote_mic / remote_player） | 机器人/另一台机器 | 172.66.88.206 | 1212（remote_player 监听）、远程连 55559 |
| 浏览器（可选，headless 后不再需要） | 用户电脑 | — | 访问 `http://172.88.88.14:55556` |

**关键命令**：

```
# 4090 上启动 app（headless 模式，无需浏览器）
python app.py --headless --tcp_target 172.66.88.206:1212 --mic_tcp_port 55559

# 另一台机器启动播放端（机器人扬声器）
python remote_player.py --port 1212

# 另一台机器启动麦克风端
python remote_mic.py --target 172.88.88.14:55559 [--device <索引或名称>]
python remote_mic.py --list   # 列出音频设备
```

---

## 三、完整对话历史（按阶段）

### 阶段 0：问题背景（上一次会话）

- **用户**：我想要不用电脑麦克风，用另一台机器的麦克风的音频转发过来，怎么做
- 方案：在两台机器分别跑 `remote_mic_udp.py`（采集麦克风转发）和 `remote_player_udp.py`（接收 TTS 播放），最初基于 UDP。

### 阶段 1：播放端问题（ALSA underrun / TTS 截断）

- **用户**：robot@EII61-0001... python remote_player_udp.py --port 1212 ... ALSA underrun occurred ×5
- 修复：输出流固定块大小（blocksize=480，20ms）+ latency=0.1 + 队列空时写静音块（`SILENCE_BLOCK`），避免声卡欠载爆音。

- **用户**：传过去的音频只会说一句话的前两个字
- 排查：`STREAM_IDLE_TIMEOUT` 0.3s → 5s（空闲判定清空播放队列导致截断）。

- **用户**：ok阿从一句话说3s变成了一句话说5秒
- 修复：空闲判定不再清空播放队列，只清重排缓冲。

- **用户**：说一下现在音频传输的逻辑，重点告诉我音频播放和stop的关系
- 讲解 UDP 音频流与 stop 控制的关系。

- **用户**：改成tcp传输会更好吗
- **用户**：卡字比较严重，帮我改成tcp
- 将 TTS 播放链路从 UDP 改为 TCP（有序可靠，消除乱序卡字）。

### 阶段 2：麦克风链路 TCP 化 + UDP 清理 + 改名

- **用户**：把麦克风切到tcp吧，现在浏览器没办法收到从另一个设备传来的音频流
- 麦克风输入链路也改为 TCP。

- **用户**：告诉我启动两个远端程序和启动app的代码，吧udp都清掉
- 提供启动命令，清除所有 UDP 代码，保持纯 TCP。

- **用户**：改名
- `remote_player_udp.py` / `remote_mic_udp.py` 改名为 `remote_player.py` / `remote_mic.py`。

- **用户**：source ... python app.py --tcp_port 55558 172.66.88.206:1212 --mic_tcp_port 55559可以吗
- 报错 `unrecognized arguments: --tcp_port 55558 ...`：旧参数名，改用 `--tcp_target`。

- **用户**：那remote_mic呢 / 那之前为什么要加，我是需要把流发送到目标机器的 / 需要类似 ssh -L 55556:localhost:55556 hjadmin@172.88.88.14 的代码吗
- 讲解 remote_mic 直接 TCP 连 55559，无需 ssh 隧道（同网段直连）。

- **用户**：Terminal#4-22（unrecognized arguments 报错）→ Terminal#24-42（第二次报错）
- 修复参数名：`--tcp_target 172.66.88.206:1212`（参数名必须写全）。

- **用户**：Terminal#149-160 连上了但是没有出声阿
- 排查双路出声问题：浏览器（AudioWorklet）和机器人同时播 TTS。

- **用户**：我的电脑会出声是正常的吗
- 解释双路出声来源：浏览器本机 + TCP 播放端，属于并行播放。

- **用户**：怎么测试1212端口有没有收到数据
- 给出端口测试方法。

### 阶段 3：打断（barge-in）延迟优化

- **用户**：现在用户打断程序说话中间的延迟太高了，有没有什么不发送stop信号的降低延迟的方法
- **用户**：不用stop有哪些方案，不需要把stop信号发给机器人
- 方案演进：
  1. **stop 信号** → **断连即静音**（app 打断时主动断开 TCP，播放端"断连即清队列"）
  2. 加 `mute_flag`（threading.Event）：播放线程每 20ms 分片检查，置位即放弃当前段并 `out.abort()`，解决"清不掉正在写的段"导致打断延迟高

- **用户**：tcp连接端开会再接上吗 / 加这个 / 为什么是2s
- 解释断线自动重连（`_maintain` 每 2 秒重试）。

- **用户**：能不能在vadstart的时候就断连，不要能量预检
- 保持 VAD start（barge-in）即断连打断逻辑。

- **用户**：我现在的体感是llm开始回复之后tts才被打断，检查一下是不是server一旦有vadstart的日志就断tcp
- **用户**：没看到有日志阿 / Terminal#995-996 都有
- 排查：真正 bug 是 **mute_flag 未生效**（`clear_all_buffers()` 清不掉"正在写的那段"），修复为 mute_flag + 分片检查 + `out.abort()` 即时静音。

- **用户**：Terminal#1008-1017 [mic-tcp] mic client connected... / Terminal#1003-1021 为什么挂了？
- 排查远端麦克风 TCP 连接挂掉问题。

- **用户**：Terminal#781-854 这样是可以收到的，app可以收到吗
- 验证远端音频流确实到达 app。

### 阶段 4：麦克风模式改为服务端参数决定

- **用户**：可不可以不要那个远程麦克风的按钮，如果我带了参数就用远程麦克风，没带的话就用自己的麦克风
- 实现：移除前端"远程麦克风"复选框；app.py 加 `--mic_tcp_port`，带了就用远端麦克风（`MIC_MODE = "remote"`），否则用浏览器麦克风（默认 `"browser"`）；随 `connect_ack` 后的 `mic_mode` 事件告知前端。

- **用户**：Cannot access microphone! 我已经重启了阿
- 排查：浏览器弹麦克风授权 / script.js 缓存旧版 + 磁盘上 mic_mode 分支丢失（重新添加）。

- **用户**：Terminal#23-35（readlink/curl 结果）/ Terminal#35-71（Address already in use）
- 端口冲突：旧 app.py 进程未退出占着 55556/55559，需 `pkill -f "dialogue_system/app.py"`。

- **用户**：Terminal#1003-1021 为什么挂了？
- 继续排查连接稳定性。

- **用户**：f12根本没有这一行阿
- 前端反复丢失代码：script.js 的 mic_mode case、app.py 的 mic_mode emit 均丢失过，全面审计恢复。

- **用户**：Terminal#1008-1009 在日志打印从55559端口接受到的数据 / Terminal#991-1010（rx 静音级数据）
- 加 rx 诊断日志；确认麦克风数据含人声（peak=826/rms=206），链路通。

- **用户**：为什么刷新浏览器会让我选麦克风？我不是转发的吗 / 一上来就[WebSocket] disconnected / Terminal#1000-1008（session initialized + peak=826）
- **把收到音频的日志删了吧**
- 移除高频 rx 日志。

- **用户**：Terminal#1008-1022 [WebSocket] disconnected script.js:38:11
- Starlette WS 断开竞态：catch 加 `RuntimeError`（"Cannot call receive once a disconnect message..."）。

- **用户**：还有没有丢失的代码
- 全面审计前端与服务端代码完整性。

### 阶段 5：排查"还是收不到音频"（核心 bug）

- **用户**：还是收不到音频
- 排查链路：
  1. `mic_vad_worker` 的 else 分支（VAD 产出文本）无日志 → 加 `VAD Process (mic-tcp) | Utterance` 日志
  2. 连接全部正常（TCP 播放端、远端麦克风、浏览器），麦克风数据含人声（peak=826），但对话流程不产出音频
  3. **关键发现**：`mic_vad_worker` 依赖活跃浏览器 session（`pick_mic_session()`），无浏览器时远端音频被全部丢弃
  4. **根因确认**：`remote_mic.py` 的 `_resample_linear` 把单声道音频当 1D 输入时，`frac[:, None]` 与 `src[lo]` 广播成 (320,320) 矩阵，序列化成 **102400 个采样**发给 app，VAD 收到垃圾数据识别不了语音 → 修复：强制 `reshape(-1)` 保证 1D 运算
  5. 修复验证：48k→16k 重采样输出 (320,)，字节数 640，app 日志 `len=320` ✓

- **用户**：浏览器上一直没有语音波形图阿
- 加了远端麦克风波形转发：app 每 2 帧通过 WS `remote_mic_waveform` 事件发 int16 base64，前端画绿色波形。

- **用户**：几个程序是通过websocket连接的吗 / 哪6006和6007呢 / 这些端口通过websocket转发吗
- 讲解连接拓扑：浏览器↔app(55556) 和 app↔VAD(8000) 是 WebSocket；remote_mic→app(55559) 和 app→remote_player(1212) 是自定义 TCP 帧；6006(TTS/IndexTTS-vLLM) 是 gRPC、6007(LLM/Qwen2.5) 是 HTTP POST 流式。

### 阶段 6：headless 模式（去掉 web 端）

- **用户**：如果我想去掉web端，你有什么建议
- 方案：加 `--headless` 参数，启动时自动创建常驻 system 会话；用户选择"headless 最小改动 + 常驻持续对话"。
- 实现（app.py）：
  - `HEADLESS` 全局标志、`start_headless_session()`（创建常驻 system 会话，ws=None）
  - `pick_mic_session()` headless 时固定用 system 会话
  - `emit_to_room` / `emit_remote_mic_waveform` 对 `websocket=None` 安全
  - `ChatSession.websocket: Optional[WebSocket]`
  - `--headless` 参数 + 启动校验（必须带 `--mic_tcp_port`，警告不带 `--tcp_target`）

- **用户**：t_rms=0 len=320 ... 去掉这个
- 删除 `_feed_mic_audio` 的 rx 诊断日志（`frames/s / last_rms / len`）。

### 阶段 7：打断改 stop 控制帧（不断连）

- **用户**：接收到vad start的时候不要用中断tcp连接当作stop信号，直接转发stop信号该怎么做
- 实现：`interrupt()` 不再 `tcp_sender.close()`，改为 `tcp_sender.send_event("stop_audio", None)`（发 `type=1, code=0`）；播放端已有 `ptype==1` 控制帧处理（`clear_all_buffers()`），无需改动；"断连清缓冲"保留作异常兜底。

- **用户**：这个控制帧是怎么发送的
- 讲解：`send_event` → 查 `CTRL_CODE` → `_send(1, bytes([0]))` → `struct.pack(">BI",1,1)+payload` → `sendall` → 播放端解析 type=1 → 清队列 + mute 立即静音。与音频帧共用同一 TCP 连接，靠 TCP 有序性保证先到音频后到 stop。

- **用户**：现在不会断tcp连接了吗
- 说明：正常打断不断连；仍可能断开的是 3 种被动场景（`_maintain` 探测对端关闭、`_send` 写失败、播放端新连接关旧连接）。

- **用户**：什么情况下会发stop
- 唯一入口 `interrupt()`，3 个触发场景：完整 utterance（pipeline_worker）、远端 Barge-in（mic_vad_worker）、浏览器 Barge-in（WS 路径）；浏览器手动 Stop（duplex_stop）不发 TCP stop。

- **用户**：场景1是什么意思 / 那我一开口他就被打断了阿，怎么会说出完整的话呢
- 讲解 TurnTaking VAD 状态机：idle→nonidle→speak；interrupt 打断的是**机器人**不是用户；用户开口（nonidle/barge-in）→ 机器人闭嘴，用户说完（speak/utterance）→ 处理整句。

- **用户**：stop帧会在什么时候发
- 再次确认 stop 帧发送时机与重复发送的幂等性。

### 阶段 8：Git 改动总结 + 安全性分析 + 修复

- **用户**：你可以帮我总结一下离上一次gitcommit改了什么吗，并且分析改动的安全性，会不会让程序崩溃
- 总结（相对 commit `c0d0c3d`）：
  - app.py +448 行：TCP 双链路、headless、mic_mode、波形、relay、打断改 stop 帧、WS 竞态修复、诊断日志清理
  - script.js +49 / style.css +11：remoteMicMode、波形绘制
  - 新文件 remote_mic.py / remote_player.py
- 安全分析结论：正常运行不崩溃；两个低概率竞态：
  - **风险 A**：`_maintain` 的 `select` 与 `_send` 并发 close 同一 socket → `OSError(EBADF)` 未捕获 → 重连线程死亡，机器人永久静音（需重启恢复）
  - **风险 B**：uvicorn 关闭后 `run_coroutine_threadsafe` 抛 RuntimeError（低，进程已退出无影响）

- **用户**：风险A会导致什么
- 详解：`_send` sendall 失败 close 的同时 `_maintain` 正 select 旧 fd → EBADF → 线程崩溃 → 不再重连 → 后续 send_audio 静默丢弃 → **机器人哑掉直到重启**。

- **用户**：修一下
- 修复风险 A：`select` 包进 try/except，异常时按"对端关闭"处理走重连。

### 阶段 9：换麦克风 + TCP 发送日志 + 推送终止分析

- **用户**：我现在换了一个麦克风，怎么检索
- 讲解：`python remote_mic.py --list` 列出设备；`--device <索引或名称关键字>` 选择；默认自动选 USB 输入设备。

- **用户**：给tcp音频发送加一个日志
- [send_audio](file:///nfs/kubeflow-data/SoulX/SoulX-Duplug/dialogue_system/app.py#L129-L142) 加低频统计日志（约每 2 秒打印 `[tcp] audio sent: N frames / X bytes`）。

- **用户**：有没有什么异常情况会让tcp的推送终止
- 分析：断线自动恢复（播放端退出、sendall 失败、EBADF 竞态）；**真实隐患**：播放端半开连接时 `sendall` 无超时 → 永久阻塞且发生在 uvicorn 事件循环线程 → 卡死整个对话流程。

- **用户**：加一下
- 修复：连接后 `s.settimeout(3.0)`（send/recv 超 3 秒抛 timeout）；`_send` 错误路径加日志 `TCP player send failed: ... will reconnect` → 自动重连。

### 阶段 10：本请求

- **用户**：整理一下对话历史，告诉我我都解决了什么问题，把所有对话历史存储到history.md里，全部放进去
- 创建本文件。

---

## 四、已解决的问题清单

### 音频传输链路
1. ✅ **远程音频转发方案**：另一台机器麦克风 → 4090 服务器（最初 UDP，最终纯 TCP）
2. ✅ **UDP → TCP 全链路改造**：播放链路（卡字）+ 麦克风链路，清除所有 UDP 代码
3. ✅ **播放端 ALSA underrun**：blocksize=480（20ms）+ latency=0.1 + 静音块填充
4. ✅ **TTS 截断（只说前两个字/3 秒/5 秒）**：`STREAM_IDLE_TIMEOUT` 0.3s→5s + 空闲判定不再清空播放队列
5. ✅ **方向反转**：app.py 主动推送（`--tcp_target`）到机器人（remote_player 监听 1212）
6. ✅ **双路出声**：明确浏览器（AudioWorklet）与机器人并行播 TTS 的来源

### 打断（Barge-in）优化
7. ✅ **打断延迟高**：演进 stop 信号 → 断连即静音 → **`mute_flag` + 分片检查 + `out.abort()`** 即时静音
8. ✅ **VAD start 即断连**（不要能量预检）：保持 barge-in 即打断
9. ✅ **打断改 stop 控制帧（不断连）**：`interrupt()` 发 `type=1,code=0`，连接保持复用
10. ✅ **打断语义澄清**：interrupt 只停机器人生成/播放，用户说话始终完整采集

### 麦克风模式与"收不到音频"
11. ✅ **服务端参数决定麦克风模式**：`--mic_tcp_port` → 远端麦克风（`MIC_MODE=remote`），否则浏览器麦克风；移除前端按钮
12. ✅ **"收不到音频"根因**：`remote_mic.py` `_resample_linear` 广播 bug（1D 输入配 `[:, None]` 生成 (320,320) 矩阵 → 序列化 102400 采样垃圾数据）→ 强制 `reshape(-1)` 修复；app 端 `len=320` 验证通过
13. ✅ **远端麦克风波形显示**：WS `remote_mic_waveform` 事件 → 前端绿色波形（调试/确认链路）
14. ✅ **诊断日志清理**：删除 rx 高频日志（`frames/s / last_rms / len`）
15. ✅ **WS 断开竞态**：catch 加 `RuntimeError`（Starlette receive after disconnect）
16. ✅ **端口冲突**：旧进程占端口 → `pkill -f "dialogue_system/app.py"`
17. ✅ **前端代码丢失**：多次丢失 mic_mode case / emit，全面审计恢复

### 去 Web 端（headless）
18. ✅ **headless 模式**：`--headless` 自动创建常驻 system 会话，无需浏览器；`emit_to_room`/波形对 `websocket=None` 安全；校验必带 `--mic_tcp_port`

### 稳定性与可观测性
19. ✅ **TCP 发送低频日志**：每 2 秒 `[tcp] audio sent: N frames / X bytes`
20. ✅ **风险 A 修复**：`select` EBADF 竞态 → try/except 走重连（避免重连线程死亡、机器人永久静音）
21. ✅ **sendall 超时保护**：`s.settimeout(3.0)` + `_send` 错误日志（避免半开连接阻塞卡死 uvicorn 事件循环）
22. ✅ **设备检索**：`remote_mic.py --list` / `--device` 支持索引与名称关键字
23. ✅ **播放端设备采样率自适应**：`_resample_linear` 用设备原生采样率打开，回调内重采样到 16k

---

## 五、当前系统架构（最终状态）

### 连接拓扑

```
远端麦克风(172.66.88.206)                机器人扬声器(172.66.88.206)
      │ remote_mic.py                          ▲ remote_player.py 监听 1212
      │ TCP 帧 [type:0][len][int16@16k]        │ TCP 帧 [type:0][音频@24k] / [type:1][stop]
      ▼                                        │
    app.py (172.88.88.14:55559 收) ←──── app.py 主动推送 --tcp_target
      │
      ├─ WebSocket(:8000) → VAD 服务(识别说话/打断)
      ├─ HTTP POST(:6007) → LLM (Qwen2.5-7B)
      └─ gRPC/HTTP(:6006) → TTS (IndexTTS-vLLM)
```

### 启动命令

```
# 4090 服务器（headless，无需浏览器）
python app.py --headless --tcp_target 172.66.88.206:1212 --mic_tcp_port 55559

# 另一台机器：播放端
python remote_player.py --port 1212

# 另一台机器：麦克风端
python remote_mic.py --target 172.88.88.14:55559 --device HECATE
```

### TCP 帧格式
`[type:1B][len:4B big-endian][payload]`
- type=0 音频（int16 PCM）
- type=1 控制（payload=控制码：0=stop、1=pause、2=resume）

### 关键机制
- **打断**：VAD nonidle（Barge-in）→ `interrupt()` → 发 stop 控制帧（不断连）→ 播放端 `clear_all_buffers()` + `mute_flag` → `out.abort()` 即时静音
- **断线恢复**：`_maintain` 每 2 秒重连；select EBADF、sendall 超时（3s）、连接失败均自动重连
- **headless 常驻会话**：`pick_mic_session()` 固定返回 system 会话，LLM 上下文持续累积
