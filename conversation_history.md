# 远程音频链路改造 — 完整对话历史

> 时间跨度：2026-07 底 ~ 2026-08-03
> 涉及仓库：`/nfs/kubeflow-data/SoulX/SoulX-Duplug`
> 最近一次 git commit：`c0d0c3d`（feat: 升级全双工对话系统）

---

## 一、初始需求

**"不用电脑麦克风，用另一台机器的麦克风把音频转发过来"** —— 实现完整的远程音频方案：

```
远端麦克风(另一台机器) --??--> 4090 服务器(app.py) --??--> 机器人(远端播放器) 扬声器
```

最终演变为机器人（172.66.88.206）既负责"麦克风采集转发"（remote_mic.py），也负责"TTS 播放"（remote_player.py），4090 服务器上的 app.py 是中枢。

---

## 二、解决的每个问题（按时间线）

### 1. 播放端 ALSA underrun（爆音/卡顿）
- **现象**：`ALSA underrun occurred` 多次
- **修复**：输出流固定块 `blocksize=480`（20ms）+ `latency=0.1`（100ms 缓冲）+ 队列空时填充静音块防欠载

### 2. 机器人"只说一句话的前两个字"
- **现象**：传过去的 TTS 音频只播前两个字就停
- **修复**：`STREAM_IDLE_TIMEOUT` 从 0.3s 提到 5s（0.3s 内无新数据就清空播放队列导致截断）

### 3. "一句话说 3 秒变成 5 秒"仍截断
- **修复**：空闲判定**不再清空播放队列**，只清重排缓冲

### 4. "卡字严重" → 播放链路 UDP 改 TCP
- 播放端 remote_player 改为 TCP 接收（有序可靠，不再需要重排缓冲）

### 5. 麦克风链路也改 TCP
- 浏览器无法收到跨机音频流的根因是链路，麦克风输入也统一走 TCP

### 6. 清除所有 UDP 代码
- 删除全部 UDP 相关实现，保持纯 TCP

### 7. 改名
- `remote_player_udp.py` / `remote_mic_udp.py` → `remote_player.py` / `remote_mic.py`

### 8. 方向反转：app.py 主动推送
- 最初是播放端拉取；改为 **app.py 主动连接**（`--tcp_target ip:port`）机器人，机器人只监听（`--port 1212`）
- 好处：app 可控连接生命周期，打断时可控

### 9. 打断（Barge-in）延迟高
- **演进**：发 stop 信号 → 断连即静音（不依赖 stop 帧）→ 发现真正 bug 是 `mute_flag` 没生效
- **最终**：`mute_flag`（threading.Event）+ 播放线程每 20ms 分片检查 + `out.abort()` 即时静音 + **stop 控制帧**（见第 14 点）

### 10. 移除前端"远程麦克风"按钮
- 改为**服务端参数决定模式**：带 `--mic_tcp_port` → 远端麦克风（`MIC_MODE="remote"`），否则浏览器麦克风
- 前端收到 `mic_mode` 事件后不再采集本机麦克风，避免弹授权

### 11. 前端/服务端代码反复"丢失"
- script.js 的 `mic_mode` case、app.py 的 `mic_mode` emit 都出现过丢失（怀疑有同步/回滚操作）
- **处理**：全面审计恢复，重写关键分支

### 12. 【核心 bug】"还是收不到音频"
排查过程（本会话最长的一条线）：

| 步骤 | 发现 |
|---|---|
| 给 `mic_vad_worker` 加 VAD 产出日志 | 关键日志缺口补上 |
| 日志显示 `no active session, dropping mic audio` | **无浏览器时没有 session，远端音频被全部丢弃** |
| 用户不刷新浏览器 → 一直无 session | 需要浏览器页面保持连接才能建 session |
| 沙箱后台进程被杀 | app.py 必须由用户在宿主机终端运行 |
| `len=102400` | **`remote_mic.py` 的 `_resample_linear` 广播爆炸**：输入 2D `(N,1)`，`frac[:, None]` 与 `src[lo]` 广播成 `(320,320)` 矩阵，序列化成 102400 采样 → VAD 收到垃圾数据识别不了语音 |
| 修复 `_resample_linear` 强制 `reshape(-1)` | `len` 恢复 320（16k×20ms），VAD 恢复正常 |

### 13. 去掉 web 端 → headless 模式
- 需求：不想依赖浏览器
- **实现**：新增 `--headless` 参数
  - 启动自动创建常驻 `system` 会话（无 WebSocket，websocket=None）
  - `pick_mic_session()` headless 时固定用 system 会话
  - 常驻持续对话（LLM 上下文累积，client_id 固定 "system"）
  - `emit_to_room` / 波形发送对 `websocket=None` 安全
  - headless 必带 `--mic_tcp_port`；不带 `--tcp_target` 时警告
- **前端文件保留**（最小改动方案）

### 14. 打断机制：断连 → stop 控制帧
- 需求：VAD start 时不要用中断 TCP 连接当 stop，直接转发 stop 信号
- **实现**：`interrupt()` 从 `tcp_sender.close()` 改为 `tcp_sender.send_event("stop_audio", None)`
  - 发送 `[type=1][len=1][0x00]` 控制帧，**TCP 连接保持不断开**
  - 下一轮 TTS 直接复用连接，省掉 2 秒重连等待
  - 播放端（remote_player.py）已有 stop 帧处理：清队列 + `mute_flag.set()` + `out.abort()`

### 15. 安全加固（git 未提交改动分析后修复）
- **风险 A**：`_maintain` 的 `select.select` 未捕获 `OSError(EBADF)` → 与 `_send` 并发 close 竞态 → 重连线程死亡、机器人永久哑声
  - **修复**：select 包进 try/except，异常按"对端关闭"走重连
- **风险 B**：`sendall` 无超时 → 半开连接时永久阻塞，且阻塞发生在 uvicorn 事件循环线程 → 卡死整个对话流程
  - **修复**：连接后 `s.settimeout(3.0)`，超时走断线重连
- **TCP 发送日志**：`send_audio` 每 2 秒打印 `[tcp] audio sent: N frames / M bytes`

### 16. 杂项
- 删除 mic 接收诊断日志（`rx frames/s last_rms len`）
- 确认端口职责：55556(浏览器 WS)、55559(远端麦克风 TCP)、1212(远端播放器 TCP)、8000(VAD WebSocket)、6006(TTS gRPC)、6007(LLM HTTP)
- 麦克风换设备检索：`remote_mic.py --list` 列设备、`--device <索引或名称>` 选择

---

## 三、最终架构（当前状态）

```
远端麦克风 --TCP 55559--> app.py(4090) --WebSocket 8000--> VAD 识别
                                        |                          |
                                    识别出文本 → LLM(6007 HTTP) → TTS(6006 gRPC)
                                        |                          |
                     app.py --TCP 1212--> remote_player --> 机器人扬声器
```

- **协议**：全部自定义 TCP 二进制帧 `[type:1B][len:4B big-endian][payload]`
  - type=0 音频（int16 PCM）
  - type=1 控制（0=stop / 1=pause / 2=resume）
- **打断**：VAD nonidle（barge-in）→ `interrupt()` → TCP stop 帧 → 播放端清队列 + abort 即时静音
- **连接可靠性**：断线/失败/超时均 2 秒自动重连；播放端新连接关闭旧连接防双播

---

## 四、启动命令

```bash
# 1. 4090 服务器（app.py）— headless 无浏览器
python app.py --headless --tcp_target 172.66.88.206:1212 --mic_tcp_port 55559

# 2. 机器人/播放端（172.66.88.206）
python remote_player.py --port 1212

# 3. 麦克风所在机器（172.66.88.206 或另一台）
python remote_mic.py --list                        # 检索设备
python remote_mic.py --target 172.88.88.14:55559 --device HECATE   # 选择设备转发
```

---

## 五、关键文件

| 文件 | 职责 |
|---|---|
| `dialogue_system/app.py` | 中枢：WS/HTTP 服务、VAD、LLM、TTS、TCP 推流/收流、headless |
| `remote_player.py` | 远端播放器：收 TCP 音频，mute_flag 即时静音 |
| `remote_mic.py` | 远端麦克风：采集 + 重采样 + TCP 转发 |
| `dialogue_system/frontend/` | 浏览器页面（headless 下可选） |

## 六、待办/遗留

- [ ] 风险 B 修复已做，待实际场景验证
- [ ] `TcpAudioSender.close()` 已成为死代码（打断不再断连），可考虑删除
- [ ] remote_player.py 中控制帧 pause/resume（code 1/2）播放端未处理（仅 stop=0），如需可补
