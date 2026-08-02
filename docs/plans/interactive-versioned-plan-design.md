# 交互式、版本化 Plan 架构与实施计划

> 状态：已实施，待生产环境迁移与手工验收。
>
> 2026-08-02 决策：Plan 从 `Task(mode="plan")` 中提升为稳定的一等制品；一个主
> Task 可以有多个 Plan，一个 Plan 可以有多个不可变 Version，一次实际规划工作是一个
> 可暂停/恢复的 Pipeline Run。Planner/Reviewer 的必要提问恢复同一个 Run；用户在成品
> 方案上请求 Revise 时创建新 Run 和新 Version，但不创建新 Plan。
>
> 代码已切换到本文的一等 Plan/Version/Run/Input/Application 架构；旧
> `Task(mode="plan")` 数据在迁移后通过 legacy link 解析，前端和所有新产品写入只使用 canonical
> Plan API。通用 `POST /api/tasks` 已拒绝 `mode=plan`，窄化的旧读写 API 仅在 contract
> 观察期服务历史客户端。生产行为仍取决于是否已部署本提交。
> 首页沿用现有 New Task Mode 下拉，不新增“执行 Task / Plan 先行”切换。

> 2026-08-02 实施复核：全局行动区统一命名为 `Plans requiring action`；Planner/Reviewer
> 只能在全局 Settings 配置，新 Plan 冻结完整路由快照；每 Run 的用户交互轮数是独立的
> `0–5` 全局设置，单轮问题数量仍无业务上限。Plan 创建请求、标题、Revise/Fork 请求和
> 回答因持久化而拒绝高置信 API key/token/private-key 文本，并引导使用 Secrets 引用。

## 0. 决策摘要

当前实现已经有 `PlanAgentRun` 和 `PlanAgentStep` 审计记录，但用户可见 Plan、调度 Task、
审批状态、最终内容和一次 Pipeline 生命周期仍集中在一条 `Task(mode="plan")` 上。结果是：

- Planner/Reviewer 只能一次性返回方案或审查结论，没有持久化的用户输入出口；
- 用户点 Revise 会 supersede 旧 Plan Task 并创建新 Plan Task，稳定的 Plan 身份丢失；
- `plan_content` 只有最新值，早期 Planner 输出只是截断后的 step 日志，不是可审批版本；
- Task 状态、Plan 决策状态和 Pipeline 执行状态共享同一套字段，容易产生 owner、busy、
  Worker 迁移和前端筛选上的耦合；
- approve/apply 绑定 Plan Task，而不是绑定用户实际看到的不可变方案版本。

目标模型为：

```text
Task / Session
├─ Plan A
│  ├─ PlanVersion v1
│  ├─ PlanVersion v2
│  ├─ PlanRun #1 (initial)
│  │  ├─ PlanStep: planner → v1
│  │  ├─ PlanStep: reviewer → revise
│  │  ├─ PlanInputRequest（可选）
│  │  ├─ PlanStep: planner → v2
│  │  └─ PlanStep: reviewer → approve
│  ├─ PlanRun #2 (user_revision) → v3
│  └─ PlanApplication（精确引用某个 version）
├─ Plan B
└─ Plan C

Standalone Plan
└─ target_task_id = NULL，其余 Version/Run/Step 语义相同
```

核心规则：

1. **Task 是主 Session/执行工作，Plan 是规划制品，Run 是后台执行。** 三者不共享状态。
2. **Plan ID 稳定。** 普通 Revise 不再创建新 Plan；只有用户明确新建/分叉规划目标时才创建。
3. **Version 不可变。** 每次 Planner 成功输出完整 Markdown 都创建一个新 Version。
4. **Reviewer 审查精确 Version。** Reviewer 结论、用户审批、应用和执行 Task 都绑定
   `plan_version_id`。
5. **澄清不是修订。** Planner/Reviewer 缺少必要输入时暂停当前 Run；回答后恢复同一 Run，
   不创建新 Plan，也不创建新 Run。
6. **成品 Revise 是新 Run。** 用户对 reviewable/approved/rejected Version 提交修改意见时，
   在同一 Plan 下启动新的 Run，产出下一 Version。
7. **逻辑连续不依赖原生 Session。** 每个 Claude/Codex Step 仍是 disposable、严格只读的
   独立调用；恢复时从数据库重建完整上下文。
8. **等待用户不占资源。** `waiting_user` Run 不持有 Instance、进程、账号、Codex thread
   或部署 blocker。
9. **主 Task 独立。** Plan 创建、运行、等待、审批、拒绝和失败都不改变目标 Task 的状态、
   session 或消息队列。
10. **Apply 仍由真实用户消息触发。** Approve 不自动回填主 Session；应用成功只在真实
    user message 已持久化并成功准入时成立。

## 1. 目标与非目标

### 1.1 目标

- 一个主 Task 同时拥有多个互相独立的 Plan；
- 一个 Plan 保留完整、可导航、不可变的版本历史；
- Planner 和 Reviewer 都能以结构化方式请求必要用户输入；
- 用户回答可带文本、选项和最多 10 个现有安全上传附件；
- 服务重启、WebSocket 断线、Worker 短暂离线后仍能恢复等待状态；
- 用户 Revise 不再制造新的用户可见 Plan/Task；
- Reviewer 自动 revision round 与用户主动 revision 统一进入同一版本图；
- 审批、拒绝、应用、创建执行 Task 都有精确 Version 审计；
- 保留当前 primary/fallback 选号、只读沙箱、staleness、ACL、Worker 和部署安全约束；
- 平滑迁移现有 Plan Task、revision chain、审批和 application 历史。

### 1.2 非目标

- 不把 Plan 变成可写代码的 Agent；
- 不 resume 主 Task 的 Claude/Codex 原生 thread 进行规划；
- 不长期保留 Planner/Reviewer 原生 thread；
- 不在 approve 时自动发送 ACK 或模型 turn；
- 不让模型通过 Claude `AskUserQuestion` hook 或 Codex 主线程 steer 直接绕过持久 API；
- 不自动合并多个 Plan 或隐式选择 latest-wins；
- 不在第一阶段提供任意动态 Pipeline DAG；本期仍是 Planner → Reviewer → 可选 revision；
- 不允许用户输入明文 API key、token 或其他 secret；需要凭据时只能提示用户使用现有
  Secret 配置能力。

## 2. 术语与层级

### 2.1 Task

现有主会话和最终执行工作。Task 可以拥有多个关联 Plan，但不保存 Plan 的内容、审批、
应用或 Pipeline 状态。Standalone Plan 没有目标 Task，但仍关联 Project/repo/Worker。

### 2.2 Plan

用户可见的稳定规划主题，是版本、Run、输入请求和应用记录的聚合根。Plan 保存：

- 标题、初始规划请求、创建者；
- `target_task_id`（NULL 表示 standalone）；
- Project、repo、branch、Worker 归属；
- 当前 Version 和当前 active Run 指针；
- 归档/关闭信息；
- 创建、更新时间。

Plan 不保存“planning/reviewing/waiting”这类运行状态；API 根据 active Run、当前 Version 和
决策记录生成 `display_state`。如需列表查询性能，可维护受 CAS 保护的投影字段，但它不是
独立事实来源。

### 2.3 PlanVersion

一次 Planner 成功输出的完整、不可变方案快照。它保存：

- `plan_id`、单调递增的 `version_number`；
- `parent_version_id`；
- `produced_by_run_id`、`produced_by_step_id`；
- 完整 Markdown `content`；
- 此方案实际使用的主 Task 对话截止点、session 快照和 repo 指纹；
- Reviewer 最终 verdict/feedback；
- 人类 decision 及审批审计；
- `superseded_by_version_id`（仅表达“不是当前版本”，不删除历史决策）。

同一 Plan 下 `(plan_id, version_number)` 唯一。Version 正文和上下文一经创建不得更新；
Reviewer 结果、人类 decision 和 superseded 指针只允许各自完成一次受 CAS 保护的状态转换。
任何会改变方案正文的操作都创建下一个 Version。

### 2.4 PlanRun / Pipeline Run

一次实际规划请求。Run 类型：

- `initial`：创建 Plan 后首次规划；
- `user_revision`：用户基于某个 Version 提交修改意见；
- `refresh_context`：用户明确要求用最新主 Session/repo 重新规划；
- `retry`：仅用于管理员显式重跑 terminal operational failure，保留来源 Run。

Run 保存冻结的 Pipeline 配置、request/feedback、base Version、上下文快照、Worker 路由、
当前 stage/round、generation、结果 Version 和错误。一个 Plan 同时最多一个 active Run。

“Pipeline”专指 Planner/Reviewer 的定义和配置；“PlanRun”才是这套定义的一次执行实例。
修改全局 Pipeline 设置不会漂移已创建 Run。

### 2.5 PlanStep

单次 Planner 或 Reviewer 模型调用，继续沿用当前 route、account、provider/model/effort、
output/error 和起止时间审计。Step 的原始输出可截断用于诊断；完整方案必须进入
PlanVersion，不能依赖 step 日志恢复。

### 2.6 PlanInputRequest

一个持久化的阻塞式用户输入请求，包含当前阶段已知的全部必要问题。每个请求至少一个问题，
但不设置问题数量的业务上限；它绑定 exact Run 和来源 Step，保存：

- `requested_by=planner|reviewer`；
- 问题 schema、请求原因；
- `open|answered|cancelled`；
- 结构化 answers、可选补充文字、附件；
- answer 用户和时间；
- 幂等键。

第一版一个 Run 同时最多一个未终结 InputRequest（prepared/open）。一次请求回答后不可编辑；
需要纠正时由用户提交新的 revision Run，或由模型在同一 Run 创建下一次 InputRequest。

### 2.7 PlanApplication

记录某个精确 Version 被用于真实 user message 或 standalone execution Task：

- `application_type=chat_message|execution_task`；
- `plan_id`、`plan_version_id`；
- 目标 Task/session 快照；
- `user_log_id` 或 `execution_task_id`；
- 操作人和时间。

每个 Version 最多成功应用一次，以保持当前“不会重复自动携带”的产品语义；同一个 Plan 的
后续新 Version 可以再次显式应用。一个 user message 可以携带来自多个 Plan 的多个 Version。

## 3. 用户行为语义

### 3.1 创建关联 Plan

1. 用户在主 Task 的 Plans modal 输入规划请求和附件；
2. 服务端创建 Plan 和 `initial` Run；
3. Run 快照目标 Task 当前对话、session、repo 和 Pipeline 配置；
4. Dispatcher 唤醒，主 Task 状态和消息队列不变；
5. 同一目标 Task 最多存在 3 个 open Plan work items：active Run 或 open InputRequest 计入，
   已完成且等待人工审批的 Version 不占运行并发，但继续出现在 review 区域。

### 3.2 创建 standalone Plan

使用相同 Plan API，只是 `target_task_id=NULL`。Plan 保存 Project/repo/branch/Worker 选择。
批准某个 Version 后可以从该 Version 创建一次 execution Task；Plan 和 Version 保持只读历史。

### 3.3 Planner 请求输入

Planner 必须先读取允许访问的 repo/context。只有无法从代码或已有上下文确定、且会实质改变
方案的用户偏好/约束才允许请求输入。

Planner 返回 `request_input` 后：

1. Step 正常完成；
2. 事务内创建 `prepared` PlanInputRequest，Run 仍保持 running 和 exact owner；
3. 生命周期清理 disposable thread/process；
4. cleanup 被精确确认后，在同一事务发布 `open` request、把 Run 置为 `waiting_user` 并释放
   Instance owner；
5. 前端在 `Plans requiring action` 的 `Input needed` 分组显示请求；
6. 用户回答后 Run 原子地回到 `queued`；
7. 新 Planner Step 使用原请求、冻结上下文、全部既有问答和已有 Version 继续规划。

如果提问发生在第一版方案之前，不创建空 Version。

### 3.4 Reviewer 请求输入

Reviewer 只能在某个明确 Version 上提问。回答后统一回到 Planner，由 Planner 把用户决定写进
一个新的完整 Version，再交 Reviewer 复审。这样批准的 Markdown 本身包含所有关键决定，
不会依赖不可见的问答历史才能正确实施。

### 3.5 Reviewer 要求修改

Reviewer 的 `revise` 反馈仍在同一个 Run 内返回 Planner；Planner 下一次成功输出创建新的
Version。达到 `max_revision_cycles` 后，最新 Version 标记 `review_exhausted` 并交给用户，
不自动失败。

### 3.6 用户请求 Revise

用户从当前或历史 Version 发起 Revise 时：

- 必须提交 `base_version_id` 和 `expected_current_version_id`；
- 服务端在 Plan operation lock 内确认没有 active Run；
- 创建 `user_revision` Run，而不是 Plan/Task；
- 新 Run 默认重新快照主 Task 当前对话和 repo；
- 旧 Version、Reviewer 记录、审批和 application 历史不变；
- 新 Run 的第一条 Planner prompt 包含 base Version 和用户 feedback。

如果当前 Version 已变化，返回 409，让用户确认基于新版本重写反馈，不能静默从旧 base 分叉。
显式 Fork 操作除外：Fork 创建新 Plan，并记录 `forked_from_version_id`。

### 3.7 Approve / Reject

- Approve 和 Reject 只作用于当前、reviewable Version；
- Reviewer 的 `approve` 只是模型审查结论，不等于人类 Approve；
- staleness 首次返回 409，显式确认后才能决定；
- Approve 不启动模型、不唤醒主 Task、不自动 Apply；
- Reject 将当前 Version 的人类 decision 设为 rejected；之后仍可在同一 Plan 上 Revise；
- 历史或 superseded Version 默认只读，管理员审计 API 仍可查看。

### 3.8 Apply

Chat composer 提交 `plan_version_ids`。服务端必须确认：

- Version 当前已由用户批准；
- Plan 属于当前目标 Task；
- Version 尚未应用；
- ACL、routing 和 staleness confirmation 有效；
- 用户消息及附件已通过现有验证。

用户消息、PlanApplication 和 queued message admission 必须保持现有的原子/补偿语义：只有真实
user message 已持久化且成功进入执行队列后才保留 application；准入失败回滚。模型 prompt
使用不可变 Version 快照，UI user message 同样展示该快照。

## 4. 状态机

### 4.1 PlanRun

Run 使用稳定的 terminal/active 状态，stage 单独存储：

```text
queued
  └─► running(stage=planner)
          ├─► waiting_user ──answer──► queued
          ├─► queued(stage=reviewer) ─► running(stage=reviewer)
          │       ├─ revise ─────────► queued(stage=planner, round+1)
          │       ├─ request_input ──► waiting_user
          │       └─ approve/exhausted ─► completed
          ├─► failed
          └─► cancelled
```

允许状态：`queued|running|waiting_user|completed|failed|cancelled`。

关键不变量：

- `running` 必须有精确 generation 和 Instance/process/thread owner 证据；
- `waiting_user` 不得有任何运行 owner；
- `waiting_user` 必须引用一个 `open` InputRequest；
- `completed` 必须引用 `result_version_id`；
- `failed/cancelled` 可以没有 Version；已生成的旧 Version 仍保留；
- terminal Run 永不复活，重试创建新 Run；
- 一个 Plan 最多一个 `queued|running|waiting_user` Run。

### 4.2 PlanVersion

Version 内容不可变，两个正交维度分别记录：

- Reviewer：`unreviewed|approve|revise|request_input|exhausted|disabled`；
- 人类 decision：`pending|approved|rejected`。

新 Version 成为 `plans.current_version_id` 时，旧 current Version 写
`superseded_by_version_id`，但旧 decision/application 不改变。只有 current Version 能进入
全局 `Plans requiring action` 的 review/execute 分组。

### 4.3 Plan 展示状态

API 按以下优先级派生 `display_state`：

1. archived；
2. active Run running/queued；
3. active Run waiting_user；
4. current Version 的 Reviewer 状态属于 `approve|exhausted|disabled`，且 human decision
   pending → awaiting_review；
5. current Version approved 且未应用 → approved；
6. current Version applied → applied；
7. current Version rejected → rejected；
8. 没有可决定的 current Version，且 latest Run failed/cancelled → failed/cancelled；
9. draft。

API 另行返回 `latest_run_status` 和 `latest_run_error`。例如已应用 v1 后用户尝试生成 v2 但 Run
失败，主展示仍可保留 v1 的 applied 状态，同时明确展示“latest revision failed”警告。前端
不得从旧 Task status 猜 Plan 状态。

## 5. 结构化模型协议

### 5.1 Planner schema v2

Planner 返回严格 discriminated union：

```json
{
  "action": "propose",
  "plan": "# 完整 Markdown 方案"
}
```

或：

```json
{
  "action": "request_input",
  "reason": "为什么缺少这些信息会阻断可靠规划",
  "questions": [
    {
      "id": "deployment_database",
      "header": "数据库",
      "question": "目标生产数据库是哪一种？",
      "response_type": "single_choice",
      "options": [
        {"value": "sqlite", "label": "SQLite"},
        {"value": "postgresql", "label": "PostgreSQL"}
      ],
      "required": true
    }
  ]
}
```

### 5.2 Reviewer schema v2

Reviewer verdict：

- `approve`：当前 Version 可交用户审批；
- `revise`：提供 Planner 可执行的具体 feedback；
- `request_input`：提供必要问题和 reason，回答后回 Planner；

字段使用严格 schema，`extra=forbid`；非法、缺字段或同时返回 plan/questions 时显式失败。

### 5.3 问题约束

- 每次至少一个问题，不限制问题数量；Planner/Reviewer 应在一次请求中合并当前已经知道的
  全部必要问题，不能为了规避数量而拆成多轮；
- `response_type=text|single_choice|multi_choice`；
- choice 数量 2–5，value 在同一问题内唯一；
- header 最长 20、question 最长 2,000、reason 最长 4,000；
- 仅使用结构化输出总大小、单字段长度和现有模型输出上限保护传输/存储，不以问题条数拒绝；
- 每个 Run 默认最多 3 次用户交互，可在全局 Pipeline 设置中配置 0–5；
- 达到上限仍请求输入时 Run 失败并保留最新 Version，不伪装成 reviewable；
- prompt 明确禁止询问能从 repo 获取的信息、无关偏好、secret 或扩大任务权限的问题。

### 5.4 恢复 Prompt

每次恢复使用有界、结构化上下文：

1. Plan 初始请求及附件清单；
2. Run 类型、用户 revision feedback、base Version；
3. 冻结的主 Task transcript；
4. 已生成 Version 和 Reviewer feedback；
5. 按时间排序的全部 InputRequest/answers；
6. 当前 repo 指纹及与 Run 开始时的变化提示。

Plan 答案不写回主 Task LogEntry；它只属于 Plan。等待期间主 Session 的新消息不会静默进入
当前 Run。用户需要纳入最新对话时显式选择 `Refresh context`，创建新 Run。

## 6. 数据模型

新表的关联遵循本项目现有数据库兼容策略：可以声明索引/逻辑外键，但业务正确性不能依赖
SQLite FK cascade。聚合服务必须显式校验引用、执行级联归档和完整性检查。Plan/Version/Run/
Application 属于审计数据，默认只允许归档，不提供普通用户 hard delete；目标 Task 被删除时
保留原 `target_task_id` 作为历史引用，并以 `target_missing` 阻止新的 revision/application。

### 6.1 `plans`

| 字段 | 说明 |
|---|---|
| `id` | 稳定 Plan ID |
| `target_task_id` | 关联主 Task；NULL=standalone |
| `project_id/target_repo/target_branch` | 执行位置快照 |
| `worker_id` | standalone 归属；关联 Plan 跟随 target |
| `priority/timeout_hours` | 调度优先级和执行时间限制；关联 Plan 创建时从 target 快照 |
| `title/initial_request` | 用户可见标题和初始意图 |
| `initial_attachments` | 已验证上传引用 |
| `current_version_id` | 当前 Version 软引用 |
| `active_run_id` | 唯一 active Run 软引用/CAS 门禁 |
| `forked_from_version_id` | 显式 Plan Fork 来源 |
| `created_by/created_at/updated_at` | 审计 |
| `archived_at/closed_at` | 用户生命周期；不代替 Run/decision 状态 |
| `lock_version` | 乐观并发版本 |

索引：`target_task_id`、`project_id`、`worker_id`、`created_by`、`updated_at`。

### 6.2 `plan_versions`

| 字段 | 说明 |
|---|---|
| `id/plan_id/version_number` | 主键、归属和单调版本号 |
| `parent_version_id` | 前一 Version |
| `produced_by_run_id/step_id` | 来源审计 |
| `content` | 完整不可变 Markdown |
| `context_session_id/context_log_id/context_snapshot` | 实际使用的主对话快照 |
| `repo_revision` | Planner 输出时 repo 指纹 |
| `review_verdict/review_feedback` | Reviewer 最终结论 |
| `reviewed_by_step_id/reviewed_at` | 审计 |
| `human_decision/decided_at/decided_by` | 用户决定 |
| `superseded_by_version_id` | 后继 Version |
| `created_at` | 创建时间 |

唯一约束：`(plan_id, version_number)`、`produced_by_step_id`。Version 正文及上下文字段不提供
UPDATE API。

### 6.3 扩展 `plan_agent_runs`

第一阶段保留物理表名以降低迁移风险，领域和 API 名称统一为 PlanRun：

- 新增 `plan_id`、`run_type`、`base_version_id`、`result_version_id`；
- 新增 `request_text`、附件、context snapshot/repo revision；
- 新增 `current_stage`、`generation`、`worker_id`、`instance_id`；
- 新增 `open_input_request_id`、`interaction_count`；
- 新增 `execution_seconds/last_execution_started_at`，使 timeout 不累计 waiting_user 时间；
- 状态收敛为本设计的六种；
- 保留 Pipeline route/config、round、error、时间审计；
- `plan_task_id` 暂时保留为 legacy 映射，contract 阶段再移除。

### 6.4 扩展 `plan_agent_steps`

- 新增 `plan_id`、`version_id`、`input_request_id`、`generation`；
- `step_type=planner|reviewer`；
- step 状态增加 `cancelled`，不得用 process exit 0 掩盖结构化 fatal error；
- output 仍为有界诊断副本，Version content 为完整权威数据。

### 6.5 `plan_input_requests`

字段：`id, plan_id, run_id, source_step_id, requested_by, reason, questions,
status, answers, response_text, attachments, answered_by, created_at, answered_at,
cancelled_at, idempotency_key`。

状态为 `prepared|open|answered|cancelled`。`prepared` 表示模型结果已持久化，但原生资源尚未
确认清理；它是内部状态，绝不向用户展示。

`plan_runs.open_input_request_id` 是跨 SQLite/PostgreSQL/MySQL 可移植的“每 Run 最多一个未
终结请求”门禁。先持久化 prepared request，再清理外部资源；cleanup 成功后，发布 open、
转换 Run 状态和释放数据库 owner 必须在同一事务中完成。

### 6.6 `plan_applications`

字段：`id, plan_id, plan_version_id, application_type, target_task_id,
target_session_id, user_log_id, execution_task_id, applied_by, created_at`。

- `plan_version_id` 唯一，保证一个 Version 只应用一次；
- `chat_message` 必须有 `user_log_id` 且无 `execution_task_id`；
- `execution_task` 反之；
- 应用内容从 Version 复制到 LogEntry 的 `applied_plans` snapshot，删除/归档 Plan 不影响历史消息。

### 6.7 Legacy 映射

新增 `plan_legacy_task_links(task_id, plan_id, version_id, run_id)`，用于：

- 旧 URL/API 按任意历史 Plan Task id 找到新 Plan/Version；
- 数据迁移前后做逐行对账；
- Worker 协议滚动升级期间兼容旧 payload。

旧客户端和旧 Worker 退出后可以停止通过它写入或传输，但该映射继续保留，用于历史深链接
重定向和审计。除非未来有独立、显式批准的数据清理方案，否则不得随 contract migration 删除。

## 7. 并发、幂等与故障恢复

### 7.1 Plan operation lock

所有创建 Run、回答问题、Approve/Reject、Apply、Fork、Archive 操作都先取 `plan_id` 级本机锁，
再用数据库 CAS 作为跨进程最终门禁。不能只依赖 asyncio lock。

### 7.2 唯一 active Run

创建 Run 使用：

1. 插入 queued Run；
2. CAS `plans.active_run_id IS NULL → new_run_id`；
3. CAS 失败则回滚并返回 409；
4. Run terminal 后只在 `active_run_id == run.id` 时清空指针。

这样不依赖 MySQL 不支持的 partial unique index。

### 7.3 回答 InputRequest

- 请求携带 `expected_run_generation` 和 idempotency key；
- CAS `request.status=open` 且 `run.status=waiting_user`；
- answers schema 与附件全部校验后一次提交；
- 重复相同 idempotency key 返回原结果；
- 不同答案竞争时后到者返回 409；
- commit 后 `dispatcher.wake()`，wake 丢失由轮询恢复。

### 7.4 Version exactly-once

Planner Step 成功解析后，在同一事务中：

1. 按 Plan counter/CAS 分配下一个 `version_number`；
2. 插入 Version，`produced_by_step_id` 唯一；
3. 链接旧 current 的 `superseded_by_version_id`；
4. 更新 `plans.current_version_id` 和 step.version_id。

服务重启重放同一 Step 时通过 `produced_by_step_id` 找回既有 Version，不能重复创建 vN/vN+1。

### 7.5 恢复规则

- `waiting_user`：原样保留，不自动失败、不占 Instance；
- `running + prepared InputRequest`：先按 exact owner 证据完成/重试 cleanup；只有 cleanup 已
  证明成功才发布 open request 和 waiting_user，证据不确定时 fail closed；
- `queued`：Dispatcher 正常重新领取；
- `running` 且有精确活进程/thread 证据：继续等待；
- `running` 但 owner 已确认死亡：当前 Step 标记 interrupted，Run 回到 queued；由于所有 Step
  严格只读，可以安全重新调用，但必须保留失败尝试与成本审计；
- owner 证据不确定：fail closed，禁止启动重复 Step；
- InputRequest 已 answered 但 wake 丢失：reconciler 把合法 Run 恢复为 queued；
- Run 已产生 Version 但 terminal update 中断：按 step/version 唯一键完成对账，不重新调用模型。

### 7.6 取消

- Cancel Run：精确停止当前 Step，取消 open InputRequest，Run → cancelled，保留已有 Version；
- Archive Plan：有 active Run 时先要求用户显式 Cancel，不能隐式强杀；
- Stop/Interrupt 主 Task 不影响 PlanRun；Cancel PlanRun 不影响主 Task；
- 所有停止都沿用 exact PID/start identity、Codex turn id 和 generation 安全规则。

## 8. Dispatcher、Instance 与部署门禁

最终架构直接调度 PlanRun，不再为每个 Run 创建用户可见或隐藏的 `Task(mode="plan")`。

### 8.1 领取与容量

- Dispatcher 在同一个 `instance_capacity_lock` 下选择普通 Task 和 queued PlanRun；
- 排序继续遵循数字越小优先级越高；PlanRun 继承 Plan/目标 Task priority，同优先级使用
  queued 时间/id 稳定排序，不能设置一套会让普通 Task 或 PlanRun 永久饥饿的独立轮询；
- Instance 新增 `current_plan_run_id`，与 `current_task_id` 必须二选一；
- claim 在 deployment lease shared lock 内把 Run CAS 为 running，并持久化 exact generation、
  instance owner 后才能 launch；
- 所有容量统计、blocker 查询、stop/start/destroy/recovery 识别两类 owner。

### 8.2 等待用户时释放

模型返回 `request_input` 后，Runner 先持久化 Step 和 prepared InputRequest，但 Run 仍保持
running/owner；只有 exact cleanup 成功后，才能在一个事务中发布 open request、切换
waiting_user 并释放 Instance owner。清理无法证明时 Run 保持运行证据并进入
failed/cleanup-error 路径，不能展示成安全等待。

`waiting_user` 不阻塞一键更新；其状态完全持久化，新进程启动后可继续。正在运行或 admission
中的 PlanRun 必须计入更新、修复、回滚、restart blocker。

### 8.3 资源与超时

- Planner/Reviewer 单步 timeout 沿用现有配置；
- Run wall-clock timeout 只累计模型执行时间，不累计等待用户时间；
- waiting_user 默认不自动过期，用户可 Cancel/Archive；
- 每 Run 的 revision round、interaction count、route fallback 都有独立上限；
- `MAX_ACTIVE_PLANS_PER_TASK` 统计 queued/running/waiting_user 的不同 Plan，不统计同一 Run 的
  Step，也不受 TasksPage 类型筛选影响。

## 9. Runner 实现

将 `PlanAgentRunner.run()` 从单个长 for-loop 改为可重入状态机：

```text
advance(run_id, generation)
  读取 Run/Plan/Version/Input 历史
  校验 generation、active_run_id、stage 和无 open 未回答请求
  按 current_stage 启动一个 disposable Step
  解析结构化结果
  原子持久化以下一种 outcome：
    - version_created → reviewer/complete
    - input_requested → waiting_user
    - reviewer_revise → planner(round+1)
    - reviewer_approve/exhausted → completed
    - operational failure → retry/failed
  清理 exact native resources
```

每次 `advance` 至多运行一个模型 Step，随后把下一个状态持久化并重新入队；不要在一个巨大
协程里跨多个模型调用和长期用户等待。Planner 产出待审 Version 后设置
`queued/current_stage=reviewer`，Reviewer revise 后设置 `queued/current_stage=planner`；两者都在
确认当前 Step 的原生资源已清理后释放 owner。这使 shutdown、Worker relay、重启恢复和测试
更简单。

Provider 约束保持不变：

- Claude：plan permission、no session persistence、只读工具、进程组精确清理；
- Codex：read-only sandbox、whole-map 禁 MCP、untrusted project、disposable thread/delete；
- primary/fallback 和同 route 多账号耗尽语义不变；
- `request_input` 是结构化成功结果，不是 operational failure，不触发 route fallback；
- schema invalid、timeout、auth、usage limit、transport failure 继续走各自现有处理；
- 用户回答不直接 steer 已结束 thread。

## 10. Context、附件与 staleness

### 10.1 Context 所有权

- 初始/refresh/user_revision Run 创建时冻结主 Task 对话和 session 快照；
- 同一 Run 内的用户问答追加到 Plan context，不写主 Task LogEntry；
- 等待期间主 Task 新消息不自动并入；
- 每次 Planner 输出 Version 时重新记录当时 repo 指纹；
- Reviewer 只审查 Version 对应内容和实际 repo 状态，并记录审查时指纹变化。

### 10.2 附件

- Plan 初始附件、revision 附件和 input answer 附件分开保存 provenance；
- 复用 `validate_upload_attachments` 的 owner、root、regular file、non-symlink 校验；
- 每个请求最多 10 个，总 prompt 使用现有有界策略；
- Worker 执行前同步 exact 附件清单并校验数量/摘要；
- 删除前端草稿不会删除已持久化、仍被 Plan 引用的上传文件；
- 不允许附件成为越权读取任意服务器路径的入口。

### 10.3 Staleness

Approve、Apply、创建 execution Task 时基于**目标 Version 的** context/repo snapshot 比较：

- 主对话新增 → `conversation_changed`；
- repo HEAD/dirty 指纹变化 → `repository_changed`；
- 目标 Task/Project/Worker 不可用 → hard conflict；
- 普通过期首次 409，用户显式确认后继续；
- `Refresh context` 不修改旧 Version，而是同一 Plan 下创建新 Run/Version。

## 11. Worker 与分布式协议

### 11.1 归属

- 关联 Plan 的每次 Run 在 admission 时读取目标 Task 的权威 Worker；
- standalone Plan 使用自身 Worker；
- Plan/Version/Input/Application 的 Manager 记录为用户可见权威；Worker 持有执行所需的
  durable mirror，并以 Manager 分配的全局 id 通信；
- Worker 不得自行改写 Pipeline config、Plan current Version 或人类 decision。

### 11.2 Relay

新增 PlanRun relay payload，至少包含：

- Plan/Run/base Version ids；
- generation 和 expected Worker assignment；
- request、冻结 context、附件 manifest；
- Pipeline config；
- 已有 Version/Reviewer/Input history 的有界恢复上下文。

Step outcome 回传使用 exact generation 和 idempotency key。Manager 只有在 Worker 结果与当前
Plan.active_run_id、Run generation 同时匹配时才提交。

### 11.3 Worker 迁移

第一版采用保守规则：

- queued/running/waiting_user Run 阻止目标 Task 单独迁移；
- reviewable/approved/applied 历史不阻止迁移，因为完整 Version 已在 Manager 持久化；
- 后续实现 Plan 组迁移时，必须同步 Run/Input/Version/附件并做两阶段 ACK；
- Worker 断线时 waiting_user 仍可查看和提交回答，但 resume 保持 queued，直到权威 Worker
  恢复或管理员完成安全迁移。

## 12. API 设计

### 12.1 Canonical API

```text
POST   /api/plans
GET    /api/plans?target_task_id=&kind=&display_state=
GET    /api/plans/{plan_id}
PATCH  /api/plans/{plan_id}                    修改标题/归档等非版本内容
POST   /api/plans/{plan_id}/fork
GET    /api/plans/resolve-legacy-task/{task_id} 历史链接重定向

GET    /api/plans/{plan_id}/versions
GET    /api/plan-versions/{version_id}
POST   /api/plan-versions/{version_id}/approve
POST   /api/plan-versions/{version_id}/reject
POST   /api/plan-versions/{version_id}/create-execution-task

GET    /api/plans/{plan_id}/runs
POST   /api/plans/{plan_id}/runs               initial/revise/refresh/retry
GET    /api/plan-runs/{run_id}
POST   /api/plan-runs/{run_id}/cancel
POST   /api/plan-runs/{run_id}/input-requests/{request_id}/answer
```

`POST /api/plans` 同时支持 standalone 和 related，并且始终冻结当时的全局 Pipeline 配置；
前端不再调用旧 `POST /api/tasks/{task_id}/plans`，通用 `POST /api/tasks` 的
`mode=plan` 创建入口明确返回 410。旧 Task 形态的窄化 API 只用于发布 contract 期兼容。

### 12.2 Chat API

请求从：

```json
{"message": "开始实施", "plan_task_ids": [123]}
```

演进为：

```json
{"message": "开始实施", "plan_version_ids": [456]}
```

过渡期允许二者之一，禁止同时传；legacy id 服务端解析为精确迁移 Version。响应和 LogEntry
snapshot 新增 `plan_id/version_id/version_number`，继续兼容旧 `id/title/content` 渲染。

### 12.3 并发参数

所有 mutation 请求携带必要的 expected 值：

- create revision：`base_version_id`、`expected_current_version_id`；
- answer：`expected_run_generation`、`idempotency_key`；
- approve/reject：`expected_current_version_id`；
- apply：由 chat operation lock + Version unique application fence；
- Worker response：`run_id/generation/step_id`。

冲突统一返回 409，并返回当前 Plan/Run/Version 摘要供前端刷新。

## 13. WebSocket 事件

新增小 payload 失效通知：

- `plan_created`；
- `plan_run_created`；
- `plan_run_status_changed`；
- `plan_input_requested` / `plan_input_answered`；
- `plan_version_created`；
- `plan_version_decided`；
- `plan_version_applied`；
- `plan_archived` / `plan_restored`。

事件只带 `plan_id/run_id/version_id/display_state/updated_at` 等摘要，不携带完整 Markdown、
questions answer 或附件。前端收到事件后 refetch canonical API；断线重连同样全量对账，不能把
WebSocket 当权威状态。管理员可订阅全局 `plans`；普通成员只可订阅通过 Plan/Task ACL 的
`plan:{id}` / `task:{id}`，并由 15 秒 HTTP 轮询覆盖新 Plan 尚无 scoped subscription 的窗口。

## 14. 前端交互

### 14.1 Plans modal

- 左侧/列表层级以 Plan 为单位，不再显示 revision Task 链；
- 每个 Plan 显示 `vN`、当前 stage、Reviewer route、stale、decision/application；
- Plan 详情提供 Version selector 和时间线；
- 默认展示 current Version，可切换历史 Version；
- 支持与上一 Version 的 Markdown/text diff；
- 用户 Revise 后留在同一 Plan 页面，显示新的 Run 进度，不产生新卡片；
- 显式 `Fork as new Plan` 才创建另一张 Plan 卡片。

### 14.2 Plans requiring action / Input needed

- 首页以 `Plans requiring action` 统一包裹 `Input needed` 与 review/execute 两类动作；
- Plans modal 使用 `Input` 过滤器和 attention badge；
- InputRequest 使用专用表单渲染 text/single/multi choice；
- 表单按响应中的完整 questions 数组渲染，不截断、不分页丢题，也不因问题数量拒绝提交；
- 支持附件上传、预览、失败重试、移除；
- 提交中冻结输入，成功后清空草稿；409 时保留草稿并刷新状态；
- 刷新页面后从 API 恢复 open questions；
- 回答完成后显示只读 Q&A 审计；
- waiting_user 不显示旋转中的模型或占用 Instance 的假象。

### 14.3 Review 与 Apply

- `Plans requiring action` 的 review/execute 分组只展示 current Version decision=pending，
  或已批准但尚未创建 execution Task 的 standalone Version；
- TasksPage 的 Normal/Standalone/Related 类型筛选不影响该区域；
- Approve/Reject 文案带 Version，例如 `Approve v3`；
- composer attachment 显示 `Plan #12 · v3`；
- user message 展开内容展示 exact applied Version snapshot；
- v1 已 applied、v2 新生成时，清晰显示两条状态，不把整个 Plan 永久标成 applied；
- standalone `Create execution task` 绑定批准的 Version，成功后显示目标 Task 链接。

### 14.4 Task 首页

Plan 不再依赖普通 Task list response：

- Normal tasks 仍查询 Tasks；
- Standalone Plans 查询 `plans?kind=standalone`；
- Related Plans 查询 `plans?kind=related`；
- UI 可以维持现有三类筛选外观，但数据源和类型改为显式 union；
- awaiting review / needs input 使用独立查询，不受 task list filter、分页和 count 影响。

## 15. 权限与安全

- Plan ACL 继承 Project/目标 Task，同时保存 created_by；关联 Plan mutation 要求同时有 Plan 和
  target Task 控制权；standalone 按 Project/creator 权限；
- Plan 列表和 WebSocket 事件都执行现有 Team CCM 可见性过滤；
- Version、InputRequest、Run 不能通过猜 id 绕过 Plan ACL；
- 模型问题严禁索取 secret；后端对疑似凭据字段不做自动保存/转发；
- 所有附件复用上传根目录、owner、non-symlink、regular-file 校验；
- Planner/Reviewer 继续禁 Bash 写入、MCP、Apps、多 Agent、网络和项目 trust；
- prompt 中把用户回答标为不可信输入，不能把它解释为扩大工具或文件权限；
- 完整 Plan/answers 不写 WebSocket 或普通结构化日志，避免意外泄露；
- account id 只出现在授权 run audit response，错误消息继续脱敏。

## 16. 数据迁移与兼容

采用 expand → backfill → dual-read → cutover → contract，禁止一次 migration 直接删除 Task 字段。

### 16.1 Expand

1. 创建 `plans/plan_versions/plan_input_requests/plan_applications/
   plan_legacy_task_links`；
2. 扩展 run/step；
3. 为新表添加索引、唯一约束和 portable check；
4. 模型导入加入 Alembic metadata；
5. 保持旧 API/Task 行为不变。

### 16.2 Backfill

按 `supersedes_plan_task_id` 把历史 Plan Task 组成有向链：

1. 在部署 blocker 已证明没有 active Plan 进程/claim 后开始迁移；验证无 cycle、一个节点最多
   一个 predecessor/successor、同链 target/project 一致；
2. 每条链创建一个 Plan，root 的请求作为 `initial_request`；
3. 每个有 `plan_content` 的旧 Task 按链顺序创建一个 Version；
4. 旧 `plan_agent_runs/steps` 关联新 Plan，并把成功 Run 对应到 Version；
5. `plan_review` → current Version decision pending；
6. `completed + plan_approved=True` → approved；
7. `cancelled + plan_approved=False` → rejected；
8. `superseded` → 历史 Version，并链接下一 Version；
9. `plan_applied_*` / `plan_execution_task_id` → PlanApplication；
10. pending carrier（无论是否存在历史 attempt）→ 新建唯一 queued canonical Run，并把旧
    Task 标为 superseded，避免部署后 Task 与 Run 双重领取；failed 且没有 content 的 Task →
    failed Run；
11. 理论上不应存在 in_progress/executing Task 或 planning/reviewing Run；任一 Task、Run、
    Instance/进程 active 证据都使 migration fail closed，包括远端 Worker 状态；
12. 每个旧 Task 写 legacy link，不改变旧 id/URL 可解析性；superseded carrier 只读保留。

若链分叉、target 冲突、应用字段不完整或多个 Task 声称同一 application，migration 必须 fail
closed 并输出脱敏的 task ids；不能猜测链顺序。

### 16.3 Dual-read / 校验

- 新 API 读新表，旧 API 通过 legacy links 投影兼容 TaskResponse；
- 临时后台对账新旧数量、current Version、审批、application、run/step 关联；
- 新写入只进入新模型；旧字段仅做兼容投影，不允许两个方向同时可写；
- Manager 与 Worker 协议增加 capability/version 握手，旧 Worker 在包含新 PlanRun 时返回 409，
  不静默降级成旧 Plan Task。

### 16.4 Cutover / Contract

- 前端全部切换 Plan/Version API；
- Dispatcher 只 claim PlanRun；
- 旧 Plan Task 标记只读 legacy，不再入队或出现在普通 Task count；
- `mode=plan` 通用创建入口已关闭；观察至少一个发布周期后移除剩余窄化旧 mutation
  endpoints、Worker legacy payload 和 `Task.plan_*` 字段；保留只读 legacy resolver/link；
- downgrade 只保证 schema 可回退，不承诺把多 Version/交互 Run 无损压回单 Task 模型；发布
  前必须依赖现有 SQLite 快照/外部数据库人工备份策略。

## 17. 分阶段实施

### Phase 1：领域表与只读投影

- 新模型、schema、Alembic expand migration；
- legacy chain backfill 和一致性校验；
- Plan/Version/Run read API；
- 旧行为不变，前端不切换；
- 加 migration 与 dual-read contract tests。

完成门槛：现有数据库迁移后，新 API 对所有 Plan 历史的数量、内容、审批、应用和 revision 顺序
与旧 API 一致；downgrade/restore 路径在三种数据库方言测试通过。

### Phase 2：可重入 Runner 与用户输入

- Planner/Reviewer schema v2；
- `advance()` 单 Step 状态机；
- InputRequest/answer API；
- waiting_user 恢复、取消、interaction limit；
- 仍不开放前端入口，先用 API 集成测试验证。

完成门槛：Planner/Reviewer 两条 request-input 路径都能跨进程重启恢复；等待期间无进程、
thread、Instance owner 或部署 blocker。

### Phase 3：Plan/Version mutation 与前端切换

- canonical create/revise/fork/approve/reject/application API；
- Plans modal、Needs input、Version history/diff；
- Chat `plan_version_ids` 和 applied snapshot；
- 首页 Plan 数据源切换；
- 旧 API 保留兼容。

完成门槛：用户 Revise 后 Plan id 和卡片不变，只增加 Run/Version；所有刷新/重连状态一致。

### Phase 4：Dispatcher/Instance/Worker 对等

- PlanRun 直接 claim 和 `Instance.current_plan_run_id`；
- capacity、termination、update blocker、recovery 全覆盖；
- Manager/Worker PlanRun relay、附件同步、generation CAS；
- 停止创建新的 Plan carrier Task。

完成门槛：本机与 Worker 的 create → ask → answer → revise → approve → apply 全链一致；故障注入
无重复 Step、Version 或 application。

### Phase 5：Legacy contract

- 停止旧写 API；
- 从普通 Task list/count/filter 排除 legacy Plan Task；
- 清理 Task Plan 字段、legacy services 和兼容 UI；
- 更新 README、TEST、AGENTS/CLAUDE 文档和运维手册。

完成门槛：仓库无新代码依赖 `Task.mode == "plan"` 或 `Task.plan_content/plan_approved/...`；
迁移后的生产副本完成手工验收后才删除兼容层。

## 18. 文件级实施范围

### 18.1 后端新增

- `backend/models/plan.py`：Plan、PlanVersion、PlanInputRequest、PlanApplication；
- `backend/schemas/plan_resource.py`：canonical resource/mutation response；
- `backend/services/plan_service.py`：聚合根、CAS、版本、决策、application；
- `backend/services/plan_run_queue.py`：PlanRun claim/recovery；
- Alembic expand/backfill/contract migrations；
- 对应单元、API、migration、dispatcher、Worker 测试。

### 18.2 后端修改

- `backend/models/plan_agent.py`；
- `backend/schemas/plan.py`；
- `backend/api/plans.py`、`backend/api/chat.py`、`backend/api/tasks.py`；
- `backend/services/plan_agent_runner.py`、`plan_tasks.py`；
- `dispatcher.py`、`instance_manager.py`、`task_termination.py`、`update_service.py`；
- `worker_proxy.py`、`task_migrator.py` 及 Worker relay/capability；
- `ws_broadcaster.py`、`main.py`、`database.py`/Alembic metadata。

不要求在第一 PR 机械重命名所有旧文件；先建立正确权威边界，再做命名清理。

### 18.3 前端

- `frontend/src/api/client.ts`：Plan/Version/Run/Input/Application 类型和 API；
- `components/PlanReview/`：Plan 列表、Version selector/diff、Run progress、Input form；
- `Chat/ChatView.tsx`：按 Version 创建/选择/应用；
- `Tasks/TaskList` / `TasksPage`：Plan 独立数据源和筛选；
- WebSocket invalidation/refetch；
- 对应 Vitest/RTL 测试。

## 19. 测试计划

### 19.1 领域与状态机单元测试

- 一个 Task 创建多个 Plan，Plan id 稳定且互不覆盖；
- 同一 Plan 的 Version number 严格单调且 content 不可更新；
- 同一 Plan 并发创建 Run 只有一个成功；
- Planner propose 创建 Version；非法 union 不创建 Version；
- Planner request_input：Run waiting、无 Version、open request；
- Reviewer request_input：answer 后回 Planner并产出包含回答的新 Version；
- Reviewer revise 多轮 Version 链和上限；
- 用户 Revise 新 Run/Version但 Plan 不变；Fork 创建新 Plan；
- answer CAS、idempotency、错误 schema、重复/竞态回答；
- terminal Run 不可复活；cancel 保留旧 Version；
- derived display_state 全组合；
- 单次 1 个、4 个及更多问题均可通过，且只受整体 payload/字段大小保护；
- interaction/revision 轮数上限边界值；轮数限制不得被误实现为单轮问题数量限制。

### 19.2 API/ACL 测试

- related/standalone create；
- 无目标 Task session、target 删除、共享 shadow、跨用户/跨 Project 拒绝；
- Version history、Run/Step/Input audit 权限；
- approve/reject 只允许 current reviewable Version；
- stale 409 与 confirm；
- revision expected current Version 冲突；
- answer 文件验证、最多 10 个、symlink/越界/owner 错误；
- old/new API dual-read、legacy id resolution；
- WebSocket payload 不包含完整 Plan/answers；
- task type filter 不影响 awaiting review/needs input。

### 19.3 Runner/provider 测试

- Claude/Codex Planner 的 propose/request_input schema；
- Reviewer approve/revise/request_input schema；
- primary/fallback、同 route 多账号、usage/auth/transient/timeout；
- request_input 不触发 fallback；
- Claude process group、Codex exact turn/thread delete；
- read-only repo fingerprint 前后相同；
- cleanup 不确定时 fail closed；
- 恢复 prompt 包含全部问答但不包含等待期间未显式刷新的主消息；
- step output 截断不影响完整 Version；
- 重放 step outcome 不重复创建 Version。

### 19.4 Dispatcher/termination/update 测试

- 普通 Task 与 PlanRun 共用 capacity；
- `current_task_id/current_plan_run_id` owner XOR；
- queued→running claim generation CAS；
- waiting_user 释放所有 owner；
- answer 后 wake 丢失由 poll 恢复；
- dead owner 精确回收、不误停复用 Instance；
- Cancel PlanRun 不影响主 Task，Interrupt 主 Task 不影响 PlanRun；
- running/admission PlanRun 阻止 update/restart，waiting_user 不阻止；
- shutdown/cancellation 每个 transaction 边界故障注入；
- 服务重启对 queued/running/waiting/terminal 的恢复矩阵。

### 19.5 Worker 测试

- Manager/Worker capability 握手；
- related Plan 跟随目标 Worker，standalone 使用指定 Worker；
- input answer 在 Worker 离线时持久保存、恢复后只 resume 一次；
- generation 不匹配结果丢弃且保留审计；
- 附件 manifest 数量/摘要不匹配 fail closed；
- active/waiting Run 的迁移 fence；
- Manager authoritative decision/application 不被 Worker 覆盖；
- relay 重试不重复 Step/Version/Application。

### 19.6 Migration 测试

至少构造：

- standalone/related 单 Plan；
- 2–5 节点 revision chain；
- plan_review/approved/rejected/applied/execution-task；
- failed/pending、无 content；
- run/step primary/fallback audit；
- legacy attachment metadata；
- chain cycle、分叉、target 冲突、悬空 application 等坏数据；
- SQLite/PostgreSQL/MySQL upgrade；
- upgrade 后 counts/content/ids/application 对账；
- snapshot restore 与可支持范围内 downgrade。

### 19.7 前端测试

- 多 Plan 列表、独立 progress、刷新和 WS 重连；
- Version selector、历史 Markdown、diff；
- Revise 保持 Plan 卡片/id，出现 vN+1；
- Fork 出现新 Plan；
- Needs input text/single/multi、附件、重试、409 保留草稿；
- waiting_user 无运行 spinner；
- approve/reject 标明 Version；
- composer 选择 exact Version，payload 使用 `plan_version_ids`；
- Applied message 展示 exact snapshot；
- v1 applied 后 v2 可独立审批/应用；
- standalone execution Task 幂等；
- task filter、awaiting review、needs input 三个查询互不影响；
- 移动端 modal、键盘操作、焦点和基本无障碍。

## 20. 手工验收计划

### 20.1 基本层级

1. 在同一主 Task 创建 Plan A/B；
2. 确认主 Task 可继续聊天，Plan 独立运行；
3. A 完成 v1，B 等待输入；
4. 刷新页面，A/B 状态和问题不丢失；
5. 回答 B 后确认仍是 Plan B、Run #1，只新增 Version；
6. 对 A 提交 Revise，确认仍是 Plan A，但创建 Run #2 和 v2；
7. 对 A 执行 Fork，确认只有此时才出现新 Plan。

### 20.2 Planner/Reviewer 输入

1. 用确定会缺少部署选择的请求触发 Planner question；
2. 确认问题出现时没有活进程/Instance owner；
3. 回答含选项、文字和附件；
4. 重启服务后继续并完成；
5. 触发 Reviewer question，确认回答回到 Planner，最终 Markdown 包含该决定；
6. 达 interaction limit 时明确失败，不无限追问。

### 20.3 Version/审批/应用

1. 查看 v1/v2 完整内容和 Reviewer feedback；
2. Approve v2，确认没有模型 turn；
3. 发送真实消息并选择 v2，确认 user message 展示完整 snapshot；
4. 让 v3 产生，确认 v2 applied 历史不变、v3 可单独审批；
5. 对 stale Version 验证首次 409 和显式确认；
6. standalone Version 创建 execution Task，重复点击返回同一 Task。

### 20.4 并发/恢复/Worker

1. 同一 Task 同时运行三个 Plan，第四个返回 429；
2. 同一 Plan 双击 Revise，只有一个 Run 成功；
3. 双浏览器回答同一 InputRequest，只有一个答案成功；
4. 在 Planner、Reviewer、waiting_user 各阶段重启服务；
5. 在 Worker 断连、恢复和 generation 变化时重复上述流程；
6. running PlanRun 阻止更新，waiting_user 不阻止；
7. Claude 和 Codex primary/fallback 各覆盖一次并验证 repo 零写入。

## 21. 发布门禁与完成定义

以下全部满足才算完成，不能只以 UI 可提问为完成：

- 新建和 Revise 路径不再创建 `Task(mode="plan")`；
- Plan、Version、Run、Step、InputRequest、Application 均有独立持久模型和 ACL；
- Planner/Reviewer request-input 均可跨服务重启恢复；
- waiting_user 无任何进程、thread、Instance owner、capacity 或 update blocker；
- 所有审批和应用绑定 exact Version；
- 旧 Plan Task 历史迁移无丢失且 legacy URL 可解析；
- 本机与 Worker 行为对等；
- Worker protocol v1 先握手，再以 attachment size/SHA-256 manifest、generation CAS 和 durable
  application receipt 验证导入/回答/应用；丢失 HTTP ACK 可按 receipt 查询恢复，不能重复应用；
- 后端全量测试、前端全量测试、生产构建、Ruff、ESLint、Alembic current/head 通过；
- SQLite 自动迁移在备份副本完成，PostgreSQL/MySQL 依项目部署规范人工演练；
- 完成本文 20 节手工验收；
- 更新 `plan-agent-design.md` 状态、README、TEST、AGENTS.md/CLAUDE.md 关键路径；
- 生产部署和重启仍需用户单独确认。

## 22. 风险与控制

| 风险 | 控制 |
|---|---|
| 状态表数量增加、查询复杂 | Plan 聚合服务统一读写；API 返回派生 display_state；禁止前端拼状态 |
| Run 等待时恢复重复调用模型 | durable status + generation + step/version unique idempotency |
| 旧 Plan revision 链异常 | migration 预检 fail closed，不猜测、不删除旧 Task |
| Task/PlanRun 抢 Instance 产生 owner 竞态 | 同一 capacity/admission lock、DB CAS、owner XOR、exact generation |
| 用户回答不进入最终方案 | Reviewer question 回 Planner；最终批准 Version 必须自包含 |
| 等待期间主对话/repo 改变 | 对话不隐式刷新；Version 输出时重记 repo；审批/application stale 检查 |
| 多版本审批/应用含义不清 | 所有按钮和审计显示 vN；application 对 Version 唯一 |
| Worker 滚动升级协议不一致 | capability handshake，未知版本 409/fail closed |
| 大范围一次改造难回滚 | expand/backfill/dual-read/cutover/contract 分阶段，每阶段独立验收 |

## 23. 最终产品语义

```text
创建 Plan       = 新的规划主题
Planner 提问    = 当前 Run 暂停，回答后继续当前 Run
Reviewer 提问   = 当前 Run 暂停，回答后回 Planner 并形成自包含新 Version
Reviewer revise = 当前 Run 内的新 Planner round / 新 Version
用户 Revise     = 同一 Plan 下的新 Run / 新 Version
Fork            = 新 Plan
Approve/Reject  = 对 exact Version 做决定
Apply           = 把 exact approved Version 绑定到真实用户消息或执行 Task
```

该语义是后续实现、API 命名、状态机、UI 文案和测试断言的共同权威；任何兼容层都不能改变它。
