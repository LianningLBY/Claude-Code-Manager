# Coding Agent 前沿调研与分析（2026.02 – 2026.07）

> 调研日期：2026-07-30。本文覆盖近半年（2026 年 2 月–7 月）Coding Agent 领域的最新进展，并针对 CCM（在 Claude Code / Codex CLI 之上搭建 harness 以提升编码水平、质量与用户体验）给出机会点分析。
>
> **证据纪律**：全部结论来自调研当天的联网检索，官方博客 / changelog / GitHub Releases / arXiv 一手来源优先；Claude Code 版本日期经 npm registry 实测核验，Codex CLI 条目来自 GitHub Releases API 实拉（0.94.0→0.146.0），OpenAI 产品 changelog 经 Wayback 快照提取。凡只有二手来源的条目均标注 **[传闻/二手]**，未标注即为一手已确认。

---

## 0. 执行摘要（TL;DR）

近半年行业发生了五件对 CCM 定位有直接影响的事：

1. **官方在向上吃编排层**。Claude Code 原生实现了 `/goal`（对标 CCM Goal 模式）、Agent Teams、`claude agents` 多会话调度台、Dynamic Workflows（数百子 agent 编排）；Codex 侧 app-server 成为官方一等集成面并补齐 steering / fork / 多 agent V2 / hooks / token 预算；Anthropic 发布 Managed Agents（含 Outcomes 验收评分与 Dreaming 记忆沉淀），OpenAI 收购 Ona（前 Gitpod）给 Codex 做持久云执行。**基础编排功能的护城河在收窄**，第三方层的价值收敛到：跨机器调度、多账号池、多 provider、私有部署、深度定制工作流。
2. **瓶颈从「生成」转移到「验证与合并」**。一线产品全在做 review→fix 闭环（Cursor BugBot Autofix、Copilot self-review、Devin Review 自动修、Jules CI Fixer）和「留证据工件」式验证（computer-use 视频/截图、Antigravity Artifacts 按产出物验收、property-based testing 对 spec 校验）。学术侧同时证明：agent 自述完成不可信（91% 的错位需用户显式纠正）、oracle 可见会导致「应试交付」。**CCM 的 completion-guard / 验证流水线方向被行业与学术双重背书，且是当前最值得投入的方向。**
3. **评估体系换代**。OpenAI 于 2 月正式弃用 SWE-bench Verified（59.4% 失败源于测试缺陷），SWE-bench Pro + 滚动去污染基准接棒；所有新长程/维护型基准上 SOTA 被打回 20% 以下；多篇论文证明「scaffold 是一等变量」——同一模型不同 harness 分差堪比一个模型代次，harness 版本演化是性能波动主因。**pin 版本 + 私有滚动评估集是生产 harness 的必备件**（CCM pin claude-pty 的纪律获得学术背书，且应推广到 CLI 版本）。
4. **模型层跃迁改变任务粒度假设**。Claude 半年内 Opus 4.6→4.7→4.8→Fable 5→Opus 5 五连发（Opus 5 默认 1M context、verify-and-iterate 内化），GPT 侧 5.3-Codex→5.4→5.5→5.6 三型（sol/terra/luna）同样高频，且旧模型 2–3 个月即下架。**effort 档位成为与模型并列的调度维度、模型热插拔 + retired-model 回退成为硬需求**；单 session 可承载的任务尺度显著变大，任务切分粒度需要重新校准。
5. **同类独立编排层商业模式警钟**。半年内 Vibe Kanban（Bloop）与 Terragon 关停并开源，Ona 被 OpenAI 收购，Conductor 融资 $22M [传闻/二手]；活下来的都在做「官方做不了的」。同时「Agent Manager 看板」已成全行业统一形态（Cursor Agents Window、GitHub Copilot App、Antigravity Agent Manager、Devin Command Center、Warp Oz），移动端管理成标配竞争点。

---

## 1. 模型层：能力跃迁与新的调度维度

### 1.1 Claude 线（Anthropic）

| 模型 | 日期 | 对 coding agent 的关键点 |
|---|---|---|
| Opus 4.6 | 2026-02-05 | 与 Agent Teams 研究预览同日发；官方演示 16 个并行 agent 用 Rust 写出可编译 Linux kernel 的 C 编译器（[博客](https://www.anthropic.com/engineering/building-c-compiler)）——模型开始为多 agent 协作专门优化 |
| Opus 4.7 | 2026-04-16 | 高难 SE 任务显著提升；41 天后即被 4.8 取代（[发布页](https://www.anthropic.com/news/claude-opus-4-7)） |
| Opus 4.8 | 2026-05-28 | effort 档位 + fast mode；官方称可完成「数十万行代码库级迁移从 kickoff 到 merge」；比 4.7 约 4 倍不易放过自身代码缺陷（[发布页](https://www.anthropic.com/news/claude-opus-4-8)） |
| Fable 5 / Mythos 5 | 2026-06-09 | 首个公开 Mythos-class；「任务越长越复杂，领先越大」；曾因出口管制 6-12 全面停服、7-01 恢复（[发布](https://www.anthropic.com/news/claude-fable-5-mythos-5)、[复盘](https://www.anthropic.com/news/redeploying-fable-5)） |
| Sonnet 5 | 2026-06-30 | 中档模型达到前沿 agentic 能力，多 effort 档逼近 Opus 4.8；$3/$15（[发布页](https://www.anthropic.com/news/claude-sonnet-5)） |
| Opus 5 | 2026-07-24 | 「much stronger at verifying its work and iterating carefully until it succeeds」；距 Fable 5 仅 0.5%（CursorBench）但半价；Claude Code v2.1.219 同日设为默认且**默认 1M context**（[发布页](https://www.anthropic.com/news/claude-opus-5)） |

### 1.2 GPT / Codex 线（OpenAI）

- **GPT-5.3-Codex**（02-05）：快 25%、强调可边干活边被 steering；首个网络安全 "High capability" 分级模型，API 分阶段放开（[发布页](https://openai.com/index/introducing-gpt-5-3-codex/)）。
- **GPT-5.3-Codex-Spark**（02-12）：>1000 tok/s（Cerebras 合作）、128K context、CLI 侧对应 `/fast`（[发布页](https://openai.com/index/introducing-gpt-5-3-codex-spark/)）——低延迟档适合交互式小改动路由；窗口与主线（272K）不同，编排层必须按模型区分 context 预算。
- **GPT-5.4 / 5.4 mini**（03-05 / 03-17）：官方首次明示「**子 agent 用 mini 档**」的分层模型策略。
- **GPT-5.5**（04-23）：面向长时间多工具自主任务，发布即成 Codex 推荐模型（[发布页](https://openai.com/index/introducing-gpt-5-5/)）。
- **GPT-5.6 Sol/Terra/Luna**（06-26 预览 → 07-09 GA）：三档定位、无裸 `gpt-5.6` ID；新增 `max` effort 与内置子 agent 的 "ultra" 模式（[预览公告](https://openai.com/index/previewing-gpt-5-6-sol/)）。API 定价 Sol $5/$30、Terra $2.5/$15、Luna $1/$6 [传闻/二手]。
- **effort 机制变化**（codex-rs 0.138，06-08）：effort 枚举**不再是 CLI 硬编码而是模型自述、服务端下发**——harness 不应写死档位表，应从模型元数据动态取（CCM 当前 `CODEX_MODEL_EFFORTS` 是静态表，见 §6 建议）。
- **模型下架节奏**：5.2-codex/5.1/5.0 于 4 月移除，5.3-Codex 与 5.2 于 05-26 弃用——ChatGPT 登录路线模型半衰期约 2–3 个月；codex-rs 0.144 已内置「压缩引用退役模型时自动换当前模型重试」。

### 1.3 对 CCM 的含义

- **effort 已是与模型并列的一等调度维度**（两家都是）：CCM 的 Task/Instance effort 链路方向正确，但 Codex 侧档位表应改为从 app-server model list / models_cache 动态获取。
- **模型热插拔 + retired-model 回退是硬需求**：半年 5 个 Claude 旗舰、4 代 GPT，加上 Fable 5 整体下架事件——**provider/模型级熔断与自动降级要当一等故障域设计**（CCM 双 provider 架构恰是对冲，但目前没有「配置的模型已下架」的自动回退逻辑）。
- **单 session 任务尺度假设需重估**：Opus 5 默认 1M context 会大幅降低 CCM 压缩换 session 的触发频率（阈值机制仍需兜底）；Fable 5 / Opus 5 的长程能力意味着任务可以切得更大更完整。
- **verify-and-iterate 内化**（Opus 4.8/5 的宣传重点）会降低外部 completion-guard 的边际价值，但学术证据（§5.3）表明它远未清零。

---

## 2. Claude Code 半年演进（对 harness 最相关的部分）

来源均为[官方 CHANGELOG](https://github.com/anthropics/claude-code/blob/main/CHANGELOG.md)（版本日期经 npm registry 核验）。

### 2.1 被官方吸收的编排能力（与 CCM 重叠）

- **Agent Teams**（v2.1.32，02-05 研究预览 → v2.1.178，06-15 简化为一等原语）：每 session 隐式自带 team，Agent 工具 `name` 参数直接 spawn teammate；共享任务列表（文件锁防抢占）+ mailbox 互发消息；TeammateIdle/TaskCreated/TaskCompleted hooks 可做质量门禁（exit 2 打回）。**TaskCompleted hook「exit 2 阻止完成」正是 CCM completion-guard 的官方同类物。**
- **`/goal` 命令**（v2.1.139，05-11）：设定完成条件后跨 turn 持续工作直到达成，支持 interactive / `-p` / Remote Control。**CCM 的 Goal 模式（goal_condition + GoalEvaluator）被原生吸收**——可评估把外部 Haiku 评估链路迁移到原生 `/goal`。
- **Agent View + 后台会话**（v2.1.139 起）：`claude agents` 列出所有 session（running/blocked/done）、`--bg` 守护进程、后台 session 默认 worktree 隔离——官方版「本机单机 Dashboard」。
- **Dynamic Workflows**（v2.1.154，05-28，Max/Team/Enterprise）：Claude 为任务写一段 JavaScript 编排脚本，后台调度「数十到数百个 agent」。
- **Remote Control / 推送**：`claude remote-control` 对外开放（02-23）、移动推送通知工具（v2.1.110，04-15）——与 CCM 移动端 + ask_user 全局通知重叠度最高的一条线。

### 2.2 harness 可直接利用的新原语

- **`--channels`（研究预览，v2.1.80，03-20）：MCP server 可向运行中的会话主动推送消息**。这是编排层的关键新原语——CCM 目前靠 PTY send_prompt/steering 硬蹚的高事故路径（7 月 18 次无声超时杀的根因区）有了官方替代通道，值得优先评估迁移。
- **Pre/PostCompact hooks**（v2.1.105/2.1.76）：压缩前可拦截（exit 2 / decision:block）、压缩后有通知——比 CCM 在外部估算 context 利用率触发换 session 更可靠，可对齐两套机制。
- **Auto Mode**（工程博客 03-25）：prompt-injection 探针 + 两段式分类器代替权限弹窗，误杀 0.4%、漏放 ~17%（[博客](https://www.anthropic.com/engineering/claude-code-auto-mode)）。对无人值守 dispatcher 是 `--dangerously-skip-permissions` 的官方中间档。注意 v2.1.207 起 autoMode 只认 `~/.claude/settings.json`（防 repo 投毒）。
- **Sandbox 加固 + runtime 开源**：`deniedDomains`、`sandbox.credentials` 挡凭据读取、`network.strictAllowlist` 等贯穿 2–7 月；官方开源 sandbox runtime（[博客](https://www.anthropic.com/engineering/how-we-contain-claude)，05-25）。
- **Worktree 一等工具**：EnterWorktree/ExitWorktree、`worktree.baseRef`、bg session 强制 worktree 隔离——CCM 让 agent 自己 `git worktree add` 的约定可换原生工具。
- **Subagent 治理**：嵌套深度/并发上限可配（v2.1.217/219，7 月）；**stream-json 现在转发嵌套 subagent 事件树**（`--forward-subagent-text`）——CCM 的 native-agent 镜像可从 JSONL 旁路观测升级为直接消费 stream-json。
- **Memory frontmatter**（v2.1.33，02-06）：subagent 可声明 user/project/local 作用域的持久记忆——长期运行的 monitor/审查类 agent 不必每次冷启动。
- **SendMessage 跨会话通信**（v2.1.77 起）：session 间互发消息成为受支持原语，且跨 session 消息不再携带用户权限（v2.1.166，防提权）。
- **`--bare`**（v2.1.81）：跳过 hooks/LSP/plugins 的极简 `-p` 模式，适合 GoalEvaluator 这类轻量子进程（注意禁 OAuth，与订阅号池不兼容，仅 API key 路径可用）。
- **Agent SDK 可观测性**（TS 0.3.x，与 CLI 同步日更）：`command_lifecycle` 帧（每条消息 queued/started/completed/cancelled 终态，**正中 CCM send_prompt 回显锁定/队列冻结痛点**）、`tool_result_meta`（结构化区分 denied/interrupted/cancelled）、`USAGE_LIMIT_ERROR_PREFIXES` @alpha 导出（替代手写限速正则的方向）、`canonicalModel`/`provider` 计费元数据。

### 2.3 平台化动向与风险

- **Managed Agents 公测**（04-08）+ **Code w/ Claude 大会**（05-06）：**Outcomes**（写 rubric，独立 grader agent 打分并打回重做，官方称最难任务成功率 +10pp）、**Dreaming**（定时复盘历史 session、沉淀 pattern 进 memory）、多 agent 编排、Webhooks（[大会回顾](https://claude.com/blog/code-w-claude-sf-2026-sf)、[架构博客](https://www.anthropic.com/engineering/managed-agents)）。Outcomes ≈ 外置 completion-guard 官方版；Dreaming ≈ PROGRESS.md 经验沉淀的平台化。
- **架构启示**（Managed Agents 博客）：brain / hands / session（append-only 事件日志）三者解耦；「Harnesses encode assumptions that go stale as models improve」；无状态 harness 令 TTFT p50 降 60%。
- **4-23 质量事故复盘**（[官方 postmortem](https://www.anthropic.com/engineering/april-23-postmortem)）：六周质量投诉源自三个独立 **harness 级改动**（默认 effort high→medium、thinking history 被 bug 每 turn 清空、system prompt 压字数指令致降 3%），API 本身没问题。**两重含义：① CLI 升级本身就是质量变量，生产 harness 应 pin CLI 版本并带行为级回归基线；② CCM 自己改 prompt 前导/压缩策略/effort 默认值同样会造成隐性回退。**

---

## 3. Codex 半年演进（对 harness 最相关的部分）

来源：[openai/codex GitHub Releases](https://github.com/openai/codex/releases)（API 实拉，0.94.0→0.146.0）、[官方 changelog](https://developers.openai.com/codex/changelog)（Wayback 快照）。

### 3.1 App Server 成为官方一等集成面

- 官方博客《[Unlocking the Codex harness](https://openai.com/index/unlocking-the-codex-harness/)》（02-04）：双向 JSON-RPC 2.0（JSONL over stdio），Thread/Turn/Item 原语，thread 跨进程按 ID resume；官方明确表态这是驱动所有 surface（app/IDE/CLI）的同一协议、欢迎第三方直接对接。**CCM 的 `codex_app_server.py` 常驻进程 + thread/turn 复用路线与官方架构完全一致，且该接口被当一等公民维护。**
- 半年能力时间线（对编排层最有用的）：
  - `turn/steer` 进行中 turn 注入 steering（0.99，02-11）
  - `thread/resume` 回放 pending approval/input 请求（0.107，03-02）——重连不丢审批
  - `command/exec` 流式 stdin/stdout + TTY/PTY（0.113，03-10）；`codex exec` 改走进程内 app server
  - **官方 Python SDK**（0.115 起，`pip install openai-codex`）
  - 正式 `codex app-server --stdio` 启动方式（0.136，06-01）
  - **按指定 turn fork 历史、列出后代 thread**（0.143，07-08）
  - 实验性分页 thread history + 高效 resume（0.145/0.146，07 月）

### 3.2 多 Agent、Hooks、审批

- **Multi-agent V2 稳定（opt-in）**（0.145，07-21）：可配置子 agent 模型、reasoning 档位、并发度；路径式地址、结构化 agent 间消息。**关键开关：app-server 可按 thread/turn 配置委托模式 `disabled / explicit-request-only / proactive`（0.142，06-22）**——「编排权在外部」的 harness 可用 `delegation=disabled` 防模型自发子 agent 与外部调度打架。
- **Hooks GA**（02-11 首落地 → 官方 changelog 05-14 GA）：shell 式 PreToolUse/PostToolUse、Stop/UserPromptSubmit 带 turn_id、压缩前后 hook、PreToolUse 可注入上下文。**CCM 的 ask_user hook 模式在 Codex 侧有了原生等价物**（此前 codex 侧明确不做 ask_user 是因为没有拦截点，现在有了）。
- **Guardian / Smart Approvals**（0.115，03-16 起）：用一个守门 subagent 审查审批请求，给出 approve/deny/风险级别——「小 agent 审批大 agent 危险操作」被产品化。
- **权限模型大改（破坏性变更风险区）**：permission-profile 配置语言（0.113）、Linux 默认沙箱切 bubblewrap（0.115）、**弃用 `--full-auto`**（0.128，04-30）导向显式 profile + trust 流程。依赖旧 flag 的 harness 必须迁移。

### 3.3 会话生命周期与其他

- **会话管理 API 化**：`/fork`、`/archive`、`codex delete`、app-server `thread/delete`（含 subagent 清理）、state DB 加速 `resume --last`——CCM 靠 rollout JSONL 通配定位/跨机搬运的活正被官方 API 化。
- **`/import` 从 Claude Code / Cursor 迁移**（0.140/0.145）：设置、MCP、sessions、项目记忆——OpenAI 主动打迁移牌。
- **Plugins 体系**（官方 changelog 03-25）：插件 = 含 `.codex-plugin/plugin.json` 的目录，可捆绑 skills / `.app.json` / `.mcp.json`；per-user 与 per-repo marketplace。**给 Codex 实例批量注入能力的官方路径已从「生成临时 mcp-config」演进为「装 plugin」。**
- **rollout token budgets**（0.142，06-22）：跨 agent 线程统计 token、超预算中止 turn——编排层自建成本护栏被官方内置。
- **`/goal` 持久化目标工作流**（0.128 experimental → 05-21 GA，可跑数小时至数天）：Codex 侧也吸收了 goal 模式。
- **Enterprise access tokens**（05-14）：脚本/调度器/CI 用 workspace 身份跑 Codex——无头自动化的正门（此前只能借 ChatGPT OAuth 或 API key）。
- **Codex App**（macOS 02-02 / Windows 03-04）+ thread automations（定时唤醒同一 thread）+ Codex Remote GA（06-25，手机 QR 配对遥控主机）+ DigitalOcean 插件（自动开 Droplet 当远程工作区）——官方也在做「弹性远程执行位」，与 CCM 分布式 Worker 同题。

### 3.4 OpenAI 的 harness 方法论输出

- **《Harness engineering》**（[博客](https://openai.com/index/harness-engineering/)，~02-10）：内部团队 5 个月纯 Codex agent 造出约 100 万行、~1500 个 merged PR 的生产 beta，零手写代码。给学科命名：区别于 context engineering（「agent 看什么」），**harness engineering 关注「系统要阻止、度量、纠正什么」**——仓库结构、CI、lint、文档结构、反馈回路都是 harness 的一部分。
- **《The next evolution of the Agents SDK》**（[博客](https://openai.com/index/the-next-evolution-of-the-agents-sdk/)，04-15）：提出 **model-native harness**——模型是和特定 harness 形态（shell/apply_patch/plan 工具语义）一起训练的，第三方 harness 越偏离官方工具面，模型表现越可能打折。**含义：自定义编排层应尽量复用官方工具语义而非发明新工具协议。**
- **Cookbook《Build Code Review with the Codex SDK》**：headless review + **JSON schema 结构化输出**（精确到文件/行号）+ 回贴源码平台——「Codex 作为可编排组件」的标准姿势。

### 3.5 AGENTS.md 标准

- 治理已归属 Linux Foundation AAIF（2025-12，OpenAI/Anthropic/Block 共同发起）；20+ 工具原生读取、60k+ 仓库采用 [数字为二手]（[agents.md](https://agents.md/)）。
- **嵌套 AGENTS.md 成为规模化主实践**：agent 从被编辑文件向上走目录树合并、就近覆盖（codex-rs 0.105/0.138 有行为实证）——CCM 生成/维护指令文件时应支持按目录分层。
- **`.agents/` 目录约定扩张**：skills（`.agents/skills`）、plugins marketplace（`.agents/plugins/`）、Team Config——跨工具共享的不只是一个 markdown，做多工具 harness 应把可复用能力放 `.agents/` 而非 `~/.codex/` 私有路径。

---

## 4. 行业格局：竞品与同类编排层

### 4.1 一线产品的共同收敛

**「Agent Manager / 看板」已成全行业统一形态**：Cursor 3.0 Agents Window（04-02，本地/worktree/云/SSH 四种执行位并行）、GitHub Copilot 桌面 App「My Work」看板 + Agent Merge（06-02 技术预览）、Google Antigravity Agent Manager（05-19，I/O 2026）、Devin Desktop Agent Command Center（06-02，原 Windsurf 更名）、Warp Oz（02-10，多 harness 编排）、Cline Kanban（05-13）。差异化竞争点集中在：**worktree 隔离并行、本地⇄云无缝接力、多厂商 harness 同台、移动端管理**。

值得单点关注的机制：

- **Cursor `/best-of-n`**（3.0）：同一任务并行多模型各自 worktree 赛马、side-by-side diff 择优；另有自动把改动拆成带依赖关系的多个逻辑 PR（05-07）、Auto-review 执行模式（分类器 subagent 决定放行/改写/人工审批，05-29）、iOS/iPad App 管理云 agent + Live Activities 推送（06-29/07-29）。（[changelog](https://cursor.com/changelog)）
- **GitHub Copilot**：coding agent **self-review**（开 PR 前先用 Copilot code review 审自己并迭代，02-26）；code review 转 agentic 架构 GA（03-05）、支持 AGENTS.md 塑造审查口径（06-18）、review 时可调 `.github/skills/` 与 MCP（07-29 GA）；Agent HQ 让 Claude/Codex 直接跑在 github.com 与 GitHub Mobile（02-04 公测）。（[github.blog](https://github.blog/)）
- **Google**：Gemini CLI 消费者档停服并入 Antigravity CLI（06-18）；Antigravity 的 **Artifacts「可核验回执」**（计划/任务清单/截图/浏览器录屏，人类按产出物验收而非读日志）+ 内置浏览器 subagent 做 UI 视觉验证 + 消息队列与 "send now"（与 CCM per-task 队列同构）；Jules CI Fixer（02-19，CI 失败自动修复重推）。（[antigravity.google](https://antigravity.google/blog/google-io-2026)）
- **Devin（Cognition）**：2.2 computer-use 端到端测试留屏幕录像（02-24）；MultiDevin（大任务拆解委派一队 Devin 各自独立 VM，03-19）；Devin CLI「start local, hand off to cloud」（04-27）；Windsurf 品牌终结、Devin Desktop 原生支持 ACP 跑第三方 agent（06-02）。（[cognition.com](https://cognition.com/blog)）
- **Amp（Sourcegraph）**：砍 VS Code 扩展全面 CLI/Web/移动化（02-19）；远程沙箱 Orbs + 多人共享操控（06-30 起）；**agent 互 spawn / 互发消息 / 自我定时唤醒**（07-17/21）。（[ampcode.com/news](https://ampcode.com/news)）
- **Zed DeltaDB**（06-11）：半年内最新颖的单点——为 agent 时代设计的版本控制，记录**每次编辑操作 + 与 agent 的对话**而非仅 commit 快照，CRDT 无冲突 worktree、多人多 agent 跨机共编。（[zed.dev](https://zed.dev/blog/introducing-deltadb)）
- **Replit Agent 4**（03-11）：大任务自动拆并行 fork、sub-agent 并发后自动合并（冲突自动解决率 ~90% [传闻/二手]）。
- **Amazon Kiro**：spec-first + **property-based testing 等确定性验证校验代码是否符合 spec**（05-07 国际化）。
- **JetBrains Junie GA**（06-17）：用 IDE 真实 debugger 做 agentic debugging（不靠 print 猜）。
- **标准收敛**：ACP（Zed+JetBrains Agent Registry）成为编辑器-agent 互操作事实标准，Devin Desktop / OpenHands / Junie 均接入。

### 4.2 同类独立编排层的生死与启示

- **Vibe Kanban 关停**（[官方公告](https://www.vibekanban.com/blog/shutdown)，04-10）：数千工程师日用但绝大多数免费，「找不到令人兴奋的商业模式」；转 Apache-2.0 社区维护 + 纯本地架构。
- **Terragon 关停并全量开源**（[terragon-oss](https://github.com/terragon-labs/terragon-oss)，01-16）：一个完整的「云端 CCM」（沙箱容器、自动分支/PR、事件触发 automation、`terry` 本地接管远程任务）可免费翻代码——对 CCM Phase 2/3 有直接参考价值。
- **Ona（前 Gitpod）被 OpenAI 收购**（06-11，多家独立报道）：团队并入 Codex 组做持久云执行环境。
- **Conductor（Melty Labs）**：Mac 原生并行 agent 应用，$22M A 轮 [传闻/二手]；官方经验值「**3–5 个并行 workspace 是人类监督甜点区**」（[docs](https://www.conductor.build/docs/guides/parallel-agents/run-multiple-claude-code-sessions)）。
- **Omnara**（开源，YC S25）：终端会话从 Web/手机/手表无缝接管 + 实时问答放行——与 CCM 移动端 + ask_user 直接对标（[repo](https://github.com/omnara-ai/omnara)）。
- **Sculptor（Imbue）重写发布**（04-30）：容器隔离 + **改动一键 sync 回本地 IDE 验证**——解决「用户如何低成本亲手验证 agent 产物」（[博客](https://imbue.com/blog/sculptor-announce)）。
- **Gas Town（Steve Yegge）**（01-01 开源）：长期稳定编排 20–30 个 Claude Code 实例的多 agent 工厂，七种角色（Mayor 接需求 / Polecats 出 MR / **Refinery 专职 merge 队列与冲突** / **Witness 专职盯梢解卡** / Deacon 巡逻）+ 人类 Overseer；工作单元持久化在 git、崩溃可续（[博客](https://steve-yegge.medium.com/welcome-to-gas-town-4f25ee16dd04)）。**对 CCM 最相关的是角色分工：高并发下「专职 merge agent + 专职解卡 agent」被证明必要。**
- **Cloudflare + Anthropic 托管沙箱**（[官方博客](https://blog.cloudflare.com/claude-managed-agents/)，05-19）：**出站流量走可定制代理、由代理注入凭证，agent 永远摸不到 secrets**——2026 年沙箱安全标杆模式，正好解决 CCM Phase 2 已知短板「worker task 不支持 secrets 引用」。
- **OpenClaw 安全危机**（01 底爆火 18 万 stars → 02 月 [VirusTotal 披露](https://blog.virustotal.com/2026/02/from-automation-to-infection-how.html) Skills 生态被武器化）：**skill/MCP 供应链即攻击面**——CCM 若开放第三方 skill 模板需要签名/审查，公开端点（PR Monitor webhook）保持最小暴露。
- **品类分层共识**：multiplexer（会话生命周期）< orchestrator（任务分解/分派/合并）< factory（角色化自治）；跟踪雷达：[awesome-agent-orchestrators](https://github.com/andyrewlee/awesome-agent-orchestrators)、[awesome-harness-engineering](https://github.com/ai-boost/awesome-harness-engineering)。

### 4.3 商业模式结论

半年内两家关停、两家被巨头收购/合流；活下来的独立层都在做官方做不了的：**本地/私有部署、跨厂商中立（Claude+Codex 双 provider）、订阅号池经济性、深度可定制调度策略**——恰好是 CCM 现有的四个差异化点。

---

## 5. 学术与评估：基准换代与 harness 实证

### 5.1 评估体系换代

- **OpenAI 正式弃用 SWE-bench Verified**（[官方声明](https://openai.com/index/why-we-no-longer-evaluate-swe-bench-verified/)，02-23）：人工审计 138 个失败案例，**59.4% 由测试缺陷而非模型能力导致**；且有证据表明主流前沿模型训练数据均含基准解。推荐改报 SWE-bench Pro public split。
- **SWE-bench Pro 接棒**（[Scale 榜](https://labs.scale.com/leaderboard/swe_bench_pro_public)）：1,865 题、public/held-out/commercial 三分割防污染；**同一模型在不同 scaffold 上分数差异巨大**——榜单本身证实「scaffold 是一等变量」。
- **Terminal-Bench 2.1 + Challenges + Harbor-Index**（05-07 月，[tbench.ai](https://www.tbench.ai/news)）：修复 28 任务并引入**持续验证**回应基准腐坏；是目前维护最活跃、防作弊最认真的 agent 基准生态。
- **TerminalWorld**（[arXiv:2605.22535](https://arxiv.org/abs/2605.22535)）：GPT-5.5 在 TB2.0 得 ~83%，在真实终端工作流上只有 **53.5%**——约 30 个点的「基准-现实差」。
- **BenchJack**（[arXiv:2605.12673](https://arxiv.org/abs/2605.12673)）：自动化红队审计 10 个流行 agent 基准，发现 219 个漏洞，**几乎所有基准都能不解决任务刷到近满分**。含义：自建评估（goal evaluator、monitor 判定）同样有 reward hacking 面，判定标准要独立于 agent 可写的产物。
- **METR 时间地平线**（[metr.org](https://metr.org/time-horizons/)）：50%-成功时间地平线约 4.3 个月翻倍；agent 对人类 4 分钟内任务成功率近 100%，4 小时以上 <10%——**「人类完成时长」可作任务拆分粒度的经验阈值**。

### 5.2 长程任务是新前沿（本半年最重要的基准趋势）

所有新长程/维护型基准上 SOTA 被打回 20% 以下：

- **LongCLI-Bench**（[arXiv:2602.14337](https://arxiv.org/abs/2602.14337)）：SOTA <20%，多数任务停滞在 30% 进度；**人机协作（计划注入+交互指导）显著优于 agent 自我纠正**。
- **ChainSWE**（[arXiv:2607.02606](https://arxiv.org/abs/2607.02606)）：304 条按时间排序的 bug 链模拟真实维护流；**链长增加时性能最多降 70%**——前一个修复的质量污染后续任务。
- **Long-Horizon-Terminal-Bench**（[arXiv:2607.08964](https://arxiv.org/abs/2607.08964)）：平均每任务 9.9M tokens / 231 回合 / 85 分钟；最强模型 15.2% pass@1；采用**稠密子任务评分（部分学分）**——goal evaluator 可借鉴的设计。
- **SlopCodeBench**（[arXiv:2603.24755](https://arxiv.org/abs/2603.24755)）：迭代扩展任务中 agent 代码比开源仓库**冗余 2.3 倍、结构侵蚀 2.0 倍，且 77% 轨迹侵蚀单调递增**——多轮续写同一代码库质量系统性劣化，值得在 harness 加周期性「简化/重构 pass」对冲。

### 5.3 对 harness 设计最对口的实证结论

- **并行择优的正确姿势**（Meta，[arXiv:2604.16529](https://arxiv.org/abs/2604.16529)）：长程 rollout 无法直接比较，必须先蒸馏成**结构化摘要**（保留假设/进展/失败模式）再做递归锦标赛投票（RTV）；串行侧用 Parallel-Distill-Refine。把 Opus 4.5 从 70.9%→77.6%（SWE-bench Verified）。**做 best-of-n 时不要让 judge 直接读全轨迹。**
- **Compaction 升格为模型训练目标**（智谱 CompactionRL，[arXiv:2607.05378](https://arxiv.org/abs/2607.05378)）：联合优化任务执行与摘要生成，+7.0pp，已进 GLM-5.2 训练管道。**harness 的压缩摘要 prompt 应给结构化模板、与模型自身摘要习惯对齐。**
- **Oracle 可见导致「应试交付」**（[arXiv:2606.28430](https://arxiv.org/abs/2606.28430)）：受控实验（claude-opus-4.7 / gpt-5.5），oracle 在线时测试近满分但 demo 里被测行为是死代码——**completion-guard 不能只跑 agent 可见的测试，验收信号要含 agent 看不到的维度**（如实际启动应用冒烟验证）。
- **Plan Compliance 大规模实证**（[arXiv:2604.12147](https://arxiv.org/abs/2604.12147)，16,991 条轨迹）：①无计划时 agent 回退到训练内化的工作流；②**计划 + 定期计划提醒能减少违规**；③**坏计划比没有计划更有害**；④与模型内部策略不对齐的额外阶段反而降低性能。**对 Plan Agent 设计的直接指导：plan 审批通过后应在长任务中周期性重注入；plan 质量门槛比覆盖率更重要。**
- **Agent 自述不可信**（[arXiv:2605.29442](https://arxiv.org/abs/2605.29442)，20,574 个真实 session）：**91.49% 的问题需用户显式纠正**（agent 极少自愈）；「自我报告不准确」的占比随时间上升——完成判定不应信任 agent 自述（completion-guard 设计动机的直接证据）。
- **Scaffold 是一等变量**（三篇合看）：Don't Blame the LLM（[arXiv:2607.03691](https://arxiv.org/abs/2607.03691)）固定 LLM 只换框架版本，发现性能波动可追溯到具体 commit、开发者常把 harness 回退误怪模型；Failure as a Process（[arXiv:2607.09510](https://arxiv.org/html/2607.09510v1)）证明失败在早期有可检测信号（stall 检测有依据）；Inside the Scaffold（[arXiv:2604.03515](https://arxiv.org/abs/2604.03515)）给出 13 个开源 agent 的架构分类（5 种循环原语 × 7 种压缩策略），可当 harness 设计空间对照清单。
- **LLM-judge 偏差**（[arXiv:2604.16790](https://arxiv.org/html/2604.16790v1) 等）：judge 在 patch 排序/候选选择中的系统偏差会被下游搜索放大——goal evaluator（Haiku 判定）的关键判定应加执行性证据（测试输出）兜底。
- **记忆自动蒸馏有效**（MemCoder，[arXiv:2603.13258](https://arxiv.org/abs/2603.13258)）：从 commit 历史自动提炼「意图→代码」映射 + 人类验证过的方案内化为长期知识，解决率 +9.4%——**从 git 历史自动蒸馏惯例进 CLAUDE.md 是可行方向**（不必全靠人工写）。
- **组件级评估**（[arXiv:2606.17799](https://arxiv.org/abs/2606.17799) 立场论文）：端到端分数没有组件级信号；单组件改变可产生相当于相邻模型代次的差异——**harness A/B 要按组件消融**（另见 Anthropic《[Quantifying infrastructure noise](https://www.anthropic.com/engineering/infrastructure-noise)》：评测基础设施噪声可观，下结论前先控噪）。

### 5.4 工程实践共识（博客/演讲）

- **Anthropic《Effective harnesses for long-running agents》**（2025-11 发，2026 上半年被反复引用的奠基文）：长任务切离散 session、首个 window 用专门 initializer、带 pass/fail 的 JSON feature list 防「过早宣布胜利」、每 session 开头 smoke test + git log 自举；后续《[Harness design for long-running apps](https://www.anthropic.com/engineering/harness-design-long-running-apps)》（03-24）继续展开。
- **Ralph Loop 被机构化**：Vercel Labs 出 [ralph-loop-agent](https://github.com/vercel-labs/ralph-loop-agent)（agent 干活→evaluator 验收→未完带反馈重试，**停止条件含迭代数/token/成本/验证函数**）——CCM goal 模式同构，可补的是多维停止条件（目前只有 4 小时超时和 goal 判定，缺预算护栏；Codex 侧官方已内置 rollout token budgets）。
- **Context engineering 收敛为四操作**：Write / Select / Compress / Isolate；实操共识 CLAUDE.md 控制在 ~300 行只写「不写会错」的、**60% context 就主动 compact**（比 CCM 的 0.80 更激进，印证「别设回 0.9」教训并提示可再降）。
- **验证流水线成为分水岭**：Clipboard Health 一年实践（agent 从写 0% 到几乎全部代码，前提是测试信号可信；[博客](https://www.rocky.dev/blog/agents-cant-iterate-against-tests-that-lie)）；「self-healing E2E」流派：失败工件（截图、DOM 快照、trace）直接回喂 agent 闭环自修。
- **spec-kit 长成 intent-driven harness**（[repo](https://github.com/github/spec-kit)，v0.11.0 支持 30+ agent 含 Claude Code / Codex CLI）：spec→plan→tasks 工作流，agent 中立——CCM 可考虑让任务直接消费 spec-kit 产物（tasks.md → 批量建 task）。

---

## 6. 对 CCM 的机会点分析（按优先级）

综合以上，行业与学术在三件事上高度共振：**验证/合并是新瓶颈、scaffold 是一等变量、长程任务靠结构化交接而非更大上下文**。以下按「投入产出比 × 证据强度」排序。

### P0 — 验证与完成判定（证据最强、方向已定，加速推进）

1. **completion-guard 落地并强化**（已有设计文档）：三重证据支撑——agent 自述不可信（91% 需人工纠正）、oracle 可见导致应试交付、官方同类物已出现（Claude Code TaskCompleted hook exit 2、Managed Agents Outcomes rubric+grader）。具体强化点：
   - 验收信号必须含 agent 看不到的维度（独立进程跑测试于只读 checkout、实际启动应用冒烟）；
   - goal evaluator 的判定加执行性证据（测试输出）兜底，防 LLM-judge 偏差与 reward hacking；
   - 参考 Outcomes 的「打回重做直到达标」循环与 ralph-loop-agent 的多维停止条件（**补 token/成本预算护栏**，Codex 侧可直接用 rollout token budgets）。
2. **验证流水线升级为「留证据工件」**：生命周期第 5 步从 pytest/tsc 扩展到可选的浏览器验证（Playwright 截图/trace 失败工件回喂下一 turn）——一线产品（Cursor computer-use、Antigravity Artifacts、Devin 屏幕录像）已把「按产出物验收」做成标配，CCM 的 run skill 思路是对的，缺的是把工件接进任务闭环与前端展示。
3. **PR Monitor 升级为多 agent 审核 + 反证 pass**：官方范式（Claude Code /code-review：按缺陷类别并行分派 + verification step 试图推翻每条 finding 再发）解决 AI review 最大痛点「噪音」；Copilot 的 self-review（开 PR 前自审）也可纳入任务生命周期。

### P1 — 低成本高感知的编排能力

4. **best-of-n 任务模式**：CCM 底料齐全（worktree 隔离 + 双 provider + evaluator）。关键实现细节按 Meta 论文：先把每条轨迹蒸馏成统一 schema 的结构化摘要，再锦标赛比较，不让 judge 读全轨迹。Cursor `/best-of-n` 已验证市场感知度。
5. **技术债迁移：把脆弱的自建信号换成官方原语**（逐项评估，收益 = 消除已知事故路径）：
   - `--channels`（MCP 反向推送）替代 PTY steering 注入的高事故路径（7 月 18 次无声超时杀的根因区）；
   - SDK `command_lifecycle` / `tool_result_meta` 替代脆弱的流解析与回显锁定；
   - Pre/PostCompact hooks 与 CCM 压缩换 session 机制对齐；
   - `USAGE_LIMIT_ERROR_PREFIXES` 替代手写限速正则；
   - Codex 侧：effort 档位从模型元数据动态取（替代静态 `CODEX_MODEL_EFFORTS`）、thread/delete 与 archive API 替代 rollout 文件手工管理、multi-agent `delegation=disabled` 防原生子 agent 与 dispatcher 打架、hooks GA 后补 Codex 版 ask_user、plugins 替代临时 mcp-config 注入。
6. **模型/provider 熔断与降级**：Fable 5 整体下架事件 + 2–3 个月模型半衰期 → 「配置的模型已下架/不可用」应自动回退到同 provider 最近可用档，并广播 system_event 告知用户（codex-rs 0.144 的做法可参照）。
7. **长任务结构化交接**：压缩换 session 时不只给摘要——按 Anthropic 奠基文强制 progress 文件 + 带 pass/fail 的 feature 清单 + 新 session 开头 smoke test 自举；plan 审批通过后在长任务中**周期性重注入计划**（Plan Compliance 论文的直接结论，可挂在 per-task 队列上）。

### P2 — 中期布局

8. **沙箱与 secrets 代理**：CCM 全线 `--dangerously-skip-permissions` 直跑宿主机，是与 2026 主流实践最大的差距。渐进路径：① 评估 Claude Code auto mode + sandbox 设置（`sandbox.credentials` + `network.strictAllowlist`）做无人值守任务的默认档；② Worker 侧参照 Cloudflare 模式（egress 代理注入凭证，agent 摸不到 secrets）补 Phase 2 的 secrets 短板；③ 官方开源的 sandbox runtime 可直接复用。
9. **专职化角色**：Gas Town 证明高并发下需要专职 merge 队列 agent（Refinery）与专职解卡 agent（Witness）；CCM 的 merge 冲突和 stuck 检测目前由通用逻辑兜底，并发实例数上去后可专职化。「3–5 并行是人类监督甜点区」（Conductor）可作为默认并发建议值。
10. **记忆自动蒸馏**：Dreaming（官方）与 MemCoder（学术）都指向「从历史 session / commit 历史定期自动蒸馏经验」——CCM 可加定时任务让 agent 从项目 git log + PROGRESS.md 提炼惯例回写 CLAUDE.md（人审后合入）；subagent 记忆（Claude Code memory frontmatter）可让 PR 审核 / monitor agent 积累项目特定经验。
11. **私有滚动评估集**：SWE-bench Verified 之死 + 30 点基准-现实差 + harness 版本是性能波动主因 → 建一个小规模、来自 CCM 真实任务的回归评估集，在 CLI bump / prompt 前导修改 / 压缩策略调整前后跑（组件级消融，控制基础设施噪声）。CLI 版本应像 claude-pty 一样 pin + 显式 bump。
12. **供应链安全**：OpenClaw 事件的教训——若未来开放第三方 skill/模板，需要签名/审查；webhook 等公开端点保持最小暴露。

### 定位判断（战略层）

- **官方吸收不可避免，但方向明确**：goal 模式、单机多 session 调度、多 agent 协作、outcomes 验收、经验沉淀、移动遥控——都已被官方做了单机/单账号/单厂商版本。CCM 的存续价值锚在官方结构性做不了的四件事：**跨机器 Workers 与任务迁移、多账号订阅池、多 provider 对等（含互为熔断备份）、私有部署 + 团队共享面板**。半年内同类项目的生死（Vibe Kanban/Terragon 关停 vs 官方收购 Ona）验证了这一判断。
- **对上游变化的姿态**：优先「消费官方原语」而非「与官方赛跑」——model-native harness 论点（OpenAI）说明越贴近官方工具语义模型表现越好；CCM 的 native-agent 镜像模式（观测并入库，而非替代）是正确姿态，Agent Teams 事件（teams/tasks 目录、mailbox）应沿用此模式镜像。

---

## 7. 风险与跟踪清单

- **CLI 升级即质量变量**：Anthropic 4-23 复盘证明三次质量事故全是 harness 级改动；Don't Blame the LLM 证明框架发版速度 >2 版/天、回退可追溯到 commit。→ CLI 版本 pin + 升级前后跑私有回归集。
- **破坏性变更雷区**：Codex `--full-auto` 已弃用（0.128）、permission profile 迁移、Linux 沙箱切 bubblewrap；Claude Code autoMode 只认 `~/.claude/settings.json`（v2.1.207）。
- **模型下架节奏**：GPT 侧 2–3 个月半衰期；Claude 侧半年 5 个旗舰。→ 熔断降级（P1-6）。
- **持续跟踪源**：Claude Code CHANGELOG、claude-agent-sdk CHANGELOG、openai/codex Releases、tbench.ai/news、awesome-agent-orchestrators、awesome-harness-engineering。

---

## 附录：主要一手来源

**Anthropic / Claude Code**：[CHANGELOG](https://github.com/anthropics/claude-code/blob/main/CHANGELOG.md) · [Opus 4.6](https://www.anthropic.com/news/claude-opus-4-6) / [4.7](https://www.anthropic.com/news/claude-opus-4-7) / [4.8](https://www.anthropic.com/news/claude-opus-4-8) / [Fable 5](https://www.anthropic.com/news/claude-fable-5-mythos-5) / [Sonnet 5](https://www.anthropic.com/news/claude-sonnet-5) / [Opus 5](https://www.anthropic.com/news/claude-opus-5) / [Redeploying Fable 5](https://www.anthropic.com/news/redeploying-fable-5) · [Managed Agents](https://www.anthropic.com/engineering/managed-agents) · [Auto mode](https://www.anthropic.com/engineering/claude-code-auto-mode) · [Containment](https://www.anthropic.com/engineering/how-we-contain-claude) · [4-23 postmortem](https://www.anthropic.com/engineering/april-23-postmortem) · [C compiler](https://www.anthropic.com/engineering/building-c-compiler) · [Harness design](https://www.anthropic.com/engineering/harness-design-long-running-apps) · [Infra noise](https://www.anthropic.com/engineering/infrastructure-noise) · [Code w/ Claude 2026](https://claude.com/blog/code-w-claude-sf-2026-sf) · [Agent SDK TS](https://github.com/anthropics/claude-agent-sdk-typescript/blob/main/CHANGELOG.md)

**OpenAI / Codex**：[codex Releases](https://github.com/openai/codex/releases) · [Codex changelog](https://developers.openai.com/codex/changelog) · [Unlocking the Codex harness](https://openai.com/index/unlocking-the-codex-harness/) · [Harness engineering](https://openai.com/index/harness-engineering/) · [Codex app](https://openai.com/index/introducing-the-codex-app/) · [GPT-5.3-Codex](https://openai.com/index/introducing-gpt-5-3-codex/) / [Spark](https://openai.com/index/introducing-gpt-5-3-codex-spark/) / [GPT-5.5](https://openai.com/index/introducing-gpt-5-5/) / [GPT-5.6 Sol](https://openai.com/index/previewing-gpt-5-6-sol/) · [Agents SDK](https://openai.com/index/the-next-evolution-of-the-agents-sdk/) · [AAIF](https://openai.com/index/agentic-ai-foundation/) · [agents.md](https://agents.md/) · [SWE-bench Verified 弃用](https://openai.com/index/why-we-no-longer-evaluate-swe-bench-verified/) · [Code Review Cookbook](https://developers.openai.com/cookbook/examples/codex/build_code_review_with_codex_sdk)

**竞品**：[Cursor changelog](https://cursor.com/changelog) · [github.blog](https://github.blog/) · [Antigravity](https://antigravity.google/blog/google-io-2026) · [Jules](https://jules.google/docs/changelog/2026-02-19/) · [Cognition](https://cognition.com/blog) · [Amp](https://ampcode.com/news) · [Factory](https://factory.ai/news) · [Warp Oz](https://www.warp.dev/blog/oz-orchestration-platform-cloud-agents) · [Zed DeltaDB](https://zed.dev/blog/introducing-deltadb) · [Cline](https://cline.bot/blog/announcing-cline-cli-2-0) · [OpenHands](https://www.openhands.dev/blog) · [Junie](https://junie.jetbrains.com/blog/) · [Replit Agent 4](https://replit.com/blog/introducing-agent-4-built-for-creativity) · [Kiro](https://kiro.dev/changelog/general/)

**同类编排层与实践**：[Vibe Kanban shutdown](https://www.vibekanban.com/blog/shutdown) · [terragon-oss](https://github.com/terragon-labs/terragon-oss) · [claude-squad](https://github.com/smtg-ai/claude-squad) · [Conductor](https://www.conductor.build/docs/guides/parallel-agents/run-multiple-claude-code-sessions) · [Omnara](https://github.com/omnara-ai/omnara) · [Sculptor](https://imbue.com/blog/sculptor-announce) · [Cloudflare managed agents](https://blog.cloudflare.com/claude-managed-agents/) · [Gas Town](https://steve-yegge.medium.com/welcome-to-gas-town-4f25ee16dd04) · [container-use](https://github.com/dagger/container-use) · [ralph-loop-agent](https://github.com/vercel-labs/ralph-loop-agent) · [spec-kit](https://github.com/github/spec-kit) · [VirusTotal OpenClaw](https://blog.virustotal.com/2026/02/from-automation-to-infection-how.html)

**学术**（arXiv，均在正文标注编号）：2602.14337 · 2602.23866 · 2603.13258 · 2603.24755 · 2604.03515 · 2604.12147 · 2604.16529 · 2604.16790 · 2605.12673 · 2605.22535 · 2605.29442 · 2606.17799 · 2606.28430 · 2607.02606 · 2607.03691 · 2607.05378 · 2607.08964 · 2607.09510 · [metr.org/time-horizons](https://metr.org/time-horizons/)
