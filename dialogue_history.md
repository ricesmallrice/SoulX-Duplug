# SoulX-Duplug 对话历史存档

> 项目：SoulX-Duplug 实时语音对话系统（8000 VAD 服务 + 55556 dialogue_system 管线 + remote_player 远端播放）
> 时间：2026-08
> 语言：全程中文

---

## 一、会话目标

围绕 SoulX-Duplug 实时语音对话系统的调试、协议扩展、性能分析、ASR 云端化改造展开。核心主线是把"每块级联本地 ASR"保留用于 VAD 状态机，在用户说完时把整段音频打包发给**讯飞 AIChain WebSocket STT 接口**（api.py）拿回 `(text, language)`，并在日志里区分本地 ASR 与云端 API。

---

## 二、早期排障（会话前半段）

### 1. 8000 端口"没启动无日志"
- **现象**：Terminal 显示 8000 未启动、无日志。
- **结论**：模型加载慢，`Uvicorn running on` 要等模型加载完才打印；8000 一直正常运行。8000 服务入口是 `uvicorn server:app --port 8000`，文件在 `/nfs/kubeflow-data/SoulX/SoulX-Duplug/server.py`（不是 service/server.py）。

### 2. `[mic-tcp] no active session, dropping mic audio` 刷屏
- **现象**：55556 持续打印该告警。
- **结论**：当前进程未带 `--headless` 且无浏览器会话。解决：`--headless --tcp_target 172.66.88.206:1212 --mic_tcp_port 55559` 重启。

### 3. TTS 服务崩溃（vLLM `EngineCore_DP0: -6` SIGABRT）
- **结论**：GPU 0 显存不足（被轮转模型/llm_server 占用）。

### 4. Gate 模式死锁（只分析未改）
- **现象**：A2F ACK 卡死，remote_player 收不到已 release 的音频。
- **根因**：上游静默后尾块 ACK 永不产生 + `gate_watchdog` 只告警不放行 + END 信号只转发无本地结算作用。
- 分析过的假设方案（未实施）：静默太久、hold 里不足 1s 尾音频时主动补发 END 到 A2F，避免永久卡死。

### 5. "日志骗人"问题
- **现象**：`_send` 在 `sock is None`/OSError 时静默 return，日志仍打 "sent"。
- **修复**：`_send` 返回 bool，`send_event`/`send_end` 按真实结果打 `sent`/`dropped`。

### 6. stop 信号连发
- **解释**：`mistake_len` 在句首过渡期（nonidle 但无 delta_text）累积到阈值前每块都报 nonidle → 连发 ~3 个 stop；句中 `user_incomplete → idle` 不触发，属于正常现象（用户纠正了我先前的错误分析）。

---

## 三、TCP 帧协议与信号机制

- **TCP 帧协议**：`[type:1B][len:4B big-endian][payload]`
  - type=0：音频（int16 PCM 24kHz）
  - type=1：控制（0=stop / 1=pause / 2=resume）
  - type=2：end（已按用户要求移除）
- **stop 信号**：发送逻辑、与音频数据的关系、连发原因均已解释；为 stop 加了 `reason` 日志。
- **end 信号**：按用户要求设计（type=2）→ 添加 → 确认发送 → 加日志 → **最终移除**。移除后 remote_player.py 的 type=2 分支成为死代码。

---

## 四、性能分析（orgin 对比）

- **变慢因素排序**：VAD 静音确认（chunk=5120 时 ~2.6s）> TTS 非流式整句合成 > LLM TTFT（长 system prompt 夏澜人设）> 传输（最小头）。
- **传输提速小项**（已分析未实施）：TCP_NODELAY、播放缓冲调小。
- 延迟日志解释：`[TurnTiming]`（每块 Enc/Chk/ASR/St/Total）、`VAD End`、`[CloudSTT] 总耗时` 的关系（云端耗时嵌套在 VAD End 内）。

---

## 五、ASR 云端化（主线，已实施）

### 1. 方案确定（经用户多次纠正）
- 用户纠正 1："每块级联仍然用本地的 asr，再识别到用户停止说话后，把音频打包给 api 接口"——每块本地照跑（管 VAD 状态机），句尾整段传 API（管最终文本）。
- 用户纠正 2：nonidle 检测需要识别文本（delta_text）与语音一起送入（`input_embeds_next` 拼接），所以本地级联不能去掉。
- 最终方案：**本地每块级联保留 + speak 时整段 buffer_for_asr 打包传讯飞 AIChain + 拿 (text, language) + 失败降级本地**。

### 2. api.py 阅读与理解
- 讯飞 AIChain WS STT 协议：`sha256(APP_KEY+curtime)` checksum 鉴权、URL `/v1/chat/{APP_ID}?curtime=..&checksum=..&sn=..&scene=main`、`conversation.user.append` 推音频（base64 + endFlag）、`stt.result` 增量结果（action=append/replace）、`event.cid_end` 结束、`data.language` 返回语言。
- 会话配置：`vad.enable=False`、`turnDetection.enable=False`（服务端不做端点检测，靠客户端 endFlag 切句）。
- **接入时不需要分帧**：一条 `conversation.user.append` 带全部 PCM + `endFlag=true` 直接发整段（`send_audio` 的"暂存一帧"机制只服务实时麦克风，集成时不用）。

### 3. 代码改动清单

**api.py**（`/nfs/kubeflow-data/SoulX/SoulX-Duplug/api.py`）
- 新增 `recognize_pcm(pcm_bytes, timeout=15) -> (text, language)`：整段 PCM 一条 append + endFlag=true 上传，复用 `build_auth_url/build_session_config/wait_for_event/build_audio_event/receive_results`。
- `sounddevice` 改为容错 import（服务器无 PortAudio 时不再崩溃）。
- `Transcript.apply` 修复：允许服务端整句全量 replace（`end == len(text)` 不再被拒），空文本 replace 帧静默忽略。
- WARN 日志改为打印完整 data JSON。
- **去掉 `language` 字段**（`LANGUAGE="auto"` 常量删除）——解决 eu（巴斯克语）误判，恢复 zh。
- 超时保留 `RESULT_TIMEOUT_SECONDS=15`（用户明确要求，不做 5s）。

**service/model.py**（`/nfs/kubeflow-data/SoulX/SoulX-Duplug/service/model.py`）
- 新增 `_recognize_utterance()`：`buffer_for_asr`（float32 [-1,1] 16kHz）→ `np.clip * 32767 → int16 → tobytes()` → ThreadPoolExecutor 线程里 `asyncio.run(recognize_pcm())`（避免 async 事件循环线程上 "loop already running"）→ 成功返回 `(text, language, "api")`，失败/超时降级本地 `cascade_asr.recognize` 返回 `(text, "unknown", "local")`。含 `[CloudSTT] OK/FAIL | 总耗时` 日志。
- 两处 speak 出口改为调用 `_recognize_utterance()`：静音确认（原 L280 附近）、`<|user_complete|>`（原 L360 附近）。返回 dict 增加 `language` 键。
- 每块本地级联 `_asr()`（原 L516-L544）完全未动（VAD 状态机、delta_text 注入、mistake_len 判定不变）。

### 4. 日志区分体系（本地 vs 云端）

| 前缀 | 含义 | 来源 |
|---|---|---|
| `[Timing] Cascade ASR: {t}s` | 每块级联识别耗时 | 本地 |
| `[SensevoiceLang] {lang} \| Text: ...` | SenseVoice 每块语言+文本 | 本地 |
| `[TurnTiming] ... ASR: ... Total: ... \| Text: ...` | 每块汇总 | 本地 |
| `[CloudSTT] OK/FAIL \| 总耗时 ... \| Lang ... \| Text ...` | 云端整句识别 | 云端 |
| `[TIMING] 首个识别结果到达 / [PARTIAL] / [FINAL-FRAME]` | api.py 内部识别过程 | 云端 |
| `[TurnTiming] VAD End (api)/(local): ...` | 最终整句文本来源标记 | 综合 |

### 5. 实测日志示例
```
[TIMING] 首个识别结果到达: 0.500s
[PARTIAL] language=zh | 换一个。
[FINAL-FRAME] language=zh | 换一个。。
[CloudSTT] OK | 总耗时: 0.869s | Lang: zh | Text: 你好。。
[TurnTiming] VAD End (api): 1.194s | Lang: zh | Text: 你好。。
```

### 6. 云端语言漂移问题排查（eu 巴斯克语）
- 现象：`[PARTIAL] language=eu | Gainera, taldea eta taldea izan da.`（"你好"被识别成巴斯克语）。
- 修复步骤：Transcript.apply 修复 → 打完整 data → 去掉 language 字段 → 语言恢复正常 zh。
- 残余：`end=-1` 空文本清空帧会触发 WARN，已静默处理（非 bug，服务端对无有效语音的清空帧）。

### 7. 完整链路（语音转发给 API 的逻辑）
```
浏览器 ──音频(16k f32)──▶ 8000 VAD
                          │  每块: 本地SenseVoice级联 → 状态机(delta_text注入)
                          │  nonidle: buffer_for_asr 累积整段
                          ▼
                       speak 出口
                          │  buffer_for_asr → int16 PCM 整段
                          ▼
                讯飞 AIChain WS (worker线程, 单条append+endFlag=true)
                          │  (text, language)
                          ▼
             返回 state dict → dialogue_system → LLM → TTS → 出声
```

---

## 六、对话系统侧（55556）

### 1. Previous turn 补历史机制
- `pending_message` 记录（上轮回复文本+音频时长+开始播放时间）是**给下一轮 pipeline_worker 用**的，最终消费方是 LLM（作为 assistant 消息入多轮历史）。
- 设计原因：多轮对话记忆 + 保持"模型视角 = 用户实际听到的"（按 `ratio = 已播时长/总时长` 截断，只把用户真实听到的部分写进历史）。
- `Ratio: 1.00` = 上轮回复已完整播放，补的是全量文本。

### 2. 提示词（Prompt）
- 位置：`dialogue_system/clients/llm_client.py` 的 `SYSTEM_PROMPT`，**3 个类各复制一份**（L16/L105/L169），组装时 `{"role": "system", "content": self.SYSTEM_PROMPT}` 拼到会话最前（L70/L134/L198）。
- **已修改**：三处同步加入语言跟随指令：
  > "语言跟随：始终使用与用户相同的语言回复。用户用什么语言提问，你就用什么语言回答——中文提问答中文，英文提问答英文，其他语言同理。除非用户主动要求，否则不切换语言、不解释语言规则。人格与工作原则不受语言影响，仅输出内容跟随语言。"
- 未实施（方案 B）：显式传 language 字段（vad_client 透传 + app.py 拼消息），更稳但需改数据流。

### 3. "嗯"类语气词屏蔽
- 核心：`dialogue_system/modules/utils/backchannel_utils.py`
  - `BACKCHANNEL` 集合（L14-L63）：嗯/嗯嗯/啊/哦/噢/哎/好/对/是/行/ok/yeah/hmm...（加词改这里）
  - 短词兜底规则（L70-L75）：≤2 字含"嗯啊哦"、≤5 字符含 "ok/mm/hmm/uh/yes/yeah"
  - `remove_leading_backchannel`（L84-L117）：去掉开头连续语气词（嗯啊哦噢呃哎哼嘿）
- 调用点：
  - model.py L581：每块级联识别后去开头语气词（**已生效**）
  - app.py L382-L384：`check_backchannel` **被注释掉**（原设计：只说"嗯"时不触发完整回复）
  - offline_infer.py L246/L279
- 另有一处 TTS 侧：`modules/utils/MyTn/cn_tn.py` L39 `FILLER_CHARS = ["呃", "啊"]`（合成前文本归一化删除）。
- **"说嗯不回复"实际由 VAD 状态机实现**：model.py `<|user_backchannel|>` 分支（L387-L396）不触发 speak、不调云端 API、不发 utterance，把"嗯"直接吃掉。所以 app.py 的 check_backchannel 是死代码（注释原文 "vad already handles this, but double-check here"）。

---

## 七、日志改善（分析 + 已实施部分）

### 已实施
- CloudSTT 总耗时：`[CloudSTT] OK | 总耗时: {t:.3f}s | Lang: {lang} | Text: {text}`（model.py `_recognize_utterance`，成功/失败都打耗时）。

### 分析过未实施（按优先级）
1. **8000 侧日志无 session_id 前缀**（多客户端共用进程无法区分）
2. **静音块 No speech 刷屏**（每 0.32s 一行，建议降噪）
3. api.py `[INFO] 等待 xx 时忽略` 噪音
4. `[SensevoiceLang]` 与 `[TurnTiming] Text:` 信息重复
5. 8000 用 print 无时间戳（建议引入 logging）
6. websocket error 无 traceback、WARN data 过长

---

## 八、遗留待办 / 可选优化

- 待重启生效：8000（api.py/model.py 改动）、55556（prompt 改动）
- 方案 B：语言透传到 55556（vad_client 返回 language + app.py 拼消息）
- 打开 app.py 的 backchannel 双保险（需先确认 resume_audio 副作用与 barge-in 不冲突）
- 云端文本重复标点清理（"你好。。"）
- 日志改善 6 项中的其余项
- gate 死锁主动补发 END 方案、TCP_NODELAY、TTS 流式化（仅分析）

---

## 九、所有用户消息原文

### 会话前半段（总结恢复）
1. "Terminal#32-35 为什么8000端口没办法启动也没有日志"
2. "Terminal#1012-1021 为什么"
3. "`/nfs/kubeflow-data/SoulX/SoulX-Duplug/dialogue_system/app.py` 解释一下stop信号现在的发送逻辑"
4. "和发送的音频数据之间的关系是什么"
5. "可以在同一条流上加上一个end信号吗，位置是每一个llmchunk对应的语音后面，不需要对end信号做出反映，并且告诉我end信号的格式"
6. "`/nfs/kubeflow-data/SoulX/SoulX-Duplug/remote_player.py` 这个可以读到tcp流吗"
7. "Terminal#1016-1022"
8. "可是现在8000还能收到声音，这个为什么关掉了"
9. "Terminal#807-830"
10. "现在程序会一直发送stop吗"
11. "可以给stop和end信号的发送加日志吗"
12. "[tcp] stop control -> clearing buffers (interrupt) 为什么会连发几个stop"
13. "可以现在有哪些情况会发stop"
14. "可以在每个stop的日志上加上stop的原因吗"
15. "Terminal#847-858 为什么会发这么多次"
16. "你没有读代码吧，一句话之前会连发几个stop，句子中间是正常的"
17. "这样会有一些冗余的代码吗"
18. "Terminal#1013-1013 这个是我说完最后一个字到tts出声的时间吗"
19. "Terminal#1014-1014 那这个是什么"
20. "`/nfs/kubeflow-data/SoulX/SoulX-Duplug-orgin` 对比一开始的代码，有什么让推理变慢的因素？"
21. "有没有可能让传输变快？现在延迟很高"
22. "end信号真的有成功发送吗，日志是不是骗我的"
23. "先不要改代码，分析一下如果上游静默太久、hold 里只有不足 1 秒尾音频，就主动补发一个 END 到 A2F，避免永久卡死。先读当前代码位置。是什么原因"
24. "Terminal#930-932 这个end成功发送了吗"
25. "是不是有的时候不会发end信号"
26. "[gate] ack released 48000 bytes ... remoteplayer没有收到"
27. "`/nfs/kubeflow-data/SoulX/SoulX-Duplug/dialogue_system/app.py#L511-512` 什么情况下会发end信号"
28. "现在不需要发送end信号了"
29. "哪一段代码是发送type=2"
30. "那一段代码是发送type=1"
31. "Terminal#930-935"
32. "告诉我asr相关的代码有哪些"
33. "可以换成调asr接口并返回语言类型的吗"
34. "我的意思是，有没有可能调网上现成模型的接口"
35. "每块只保留本地"是否有人说话"的 token 检测（免费），只在句子结束（speak）时调一次在线接口拿整句文本 + 语言 。这样可以做到吗，告诉我这样做的话代码要改哪些呢"
36. "如果我想要在模型返回用户说完的时候把所有的语音一起打包发给asr接口可以吗"
37. "现在是每一段语音都送入asr，这一段逻辑也要修改吧"
38. "那我现在是不是就可以去掉级联识别"
39. "nonidle检测是不是需要识别出的文本和语音一起送入"
40. "那么我现在想要用户说完之后把用户说话时候的音频通过api送入别的模型，该怎么改？"
41. "不，你没有理解，每块级联仍然用本地的asr，再识别到用户停止说话后，把音频打包给api接口，是这样的"
42. "`/nfs/kubeflow-data/SoulX/SoulX-Duplug/api.py` 阅读一下这个文件，告诉我文件内容，并且告诉我如果要接这个api的话应该怎么做，并且加上返回时间的日志"
43. "这个方案会保证每段音频仍然用本地asr识别，只有最后把一整段音频上传到接口吗，讲一下现在api.py接受音频的逻辑"
44. "api返回的结果会如何在日志里输出？"
45. "可以按我说的分析apipy改如何接入系统，并且在日志里区分本地asr和api吗"

### 会话后半段（当前）
46. "`/nfs/kubeflow-data/SoulX/SoulX-Duplug/api.py#L289-349` 这个实时发送音频是什么意思，有可能从识别用户说话开始的音频都发过去，最后给一个停止信号吗"
47. "改之前再告诉我一遍你想怎么改"
48. "整体 5s 超时（原来 15s 太长，交互场景收紧，超时走降级）这条是怎么想的"
49. "保留15s吧"
50. "Terminal#765-778"（eu 语言漂移日志）
51. "[WARN] 无法解析 stt.result，data keys=['action', 'index', 'language', 'position', 'text'] 这是什么"
52. "不需要发language类型吧"
53. "[PARTIAL] language=zh | 就是。 [WARN] data=...replace(0,7)... 这是什么"
54. "现在语音转发给api的逻辑是什么样的"
55. "Terminal#969-975"（VAD Process + Previous turn 日志）
56. "为什么要补进历史"
57. "这个记录是给谁的？"
58. "[SensevoiceLang] <|zh|> | Text: 换一个。 ... 这些日志是什么意思"
59. "现在的日志你觉得有哪里可以改善的地方吗"
60. "2. 云端识别总耗时缺失 ... 改一下这个"
61. "[CloudSTT] OK | 总耗时: 0.869s ... [TurnTiming] VAD End (api): 1.194s ... 这两个分别是什么耗时？"
62. "提示词在哪个文件"
63. "如果希望收到和输出的语言一致提示词该怎么写"
64. "prompt都加上始终使用与用户相同的语言回复。用户用什么语言提问，你就用什么语言回答——中文提问答中文，英文提问答英文，其他语言同理。除非用户主动要求，否则不切换语言、不解释语言规则。人格与工作原则不受语言影响，仅输出内容跟随语言。"
65. "屏蔽'嗯'之类的词的设置在哪里"
66. "为什么没启用说嗯的时候也不回复"
67. "你可以整理一下对话历史，告诉我我都解决了什么问题吗"
68. "你可以把所有对话历史存储到一个文件里吗，全部放进去"
