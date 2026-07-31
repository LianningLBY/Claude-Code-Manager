# 独立 Plan Task 与 Plan Agent 设计

> 状态：已实施（含 Planner/Reviewer 独立 primary/fallback 路由），待生产环境手工验收。
>
> 2026-07-29 决策：Plan 永远是独立制品。Plan 完成或批准都不会自动唤醒目标
> session；用户必须通过下一条真实消息携带方案，或显式创建执行 Task。

## 0. 决策摘要

Plan 统一建模为 `mode="plan"` 的独立 Task：

- 从 New Task 创建的 Plan 是 standalone Plan Task；
- 从现有 Task/session 创建的 Plan 是关联 Plan Task，通过
  `plan_target_task_id` 指向目标 Task；
- 一个目标 Task 可以关联多个互相独立的 Plan；
- Plan 完成后进入 `plan_review`，每个 Plan 单独 approve/reject；
- approve 只改变 Plan 的审批状态，不启动模型、不修改目标 Task 状态；
- 关联 Plan approve 后成为目标 Task 的“已批准、待应用”附件；
- 用户下一次发送普通消息时显式选择 Plan，服务端把方案与该条真实用户指令放在
  同一个 turn 中；
- standalone Plan approve 后提供“创建执行 Task”，新 Task 执行方案，Plan Task
  自身保持只读历史；
- reject 只终止 Plan，不取消目标 Task；重新规划创建新的独立 Plan，可记录
  `supersedes_plan_task_id`；
- 不自动注入，不需要 ACK turn，也不需要 `QueuedMessage.readonly_turn`。
- Plan Task 是持久制品和生命周期容器，不是 Claude/Codex 原生 session；每个模型步骤由
  `PlanAgentRunner` 临时启动，完成后不保留可续聊的 Plan session。

## 1. 为什么不在 approve 时直接“回填 session”

Claude/Codex 的原生 session 没有安全的无回复消息追加接口：

1. 调 `enqueue_message` 会启动一个真实 turn，模型必然回复，并可能把方案理解为执行指令；
2. 直接修改 Claude JSONL 或 Codex rollout 会耦合私有格式，PTY 热 session 和 Codex
   app-server 也不会可靠加载外部追加内容；
3. 为压住这个额外 turn 而设计 ACK 协议，会重新引入 per-turn 权限控制、PTY 事后写检测、
   主 session 活跃态竞争和额外模型成本。

因此“回填”定义为 CCM 级关联：approve 后 Plan 成为持久的待应用上下文附件；直到用户
发送下一条真实消息，模型才第一次看到方案。

## 2. 产品流程

### 2.1 Session 内创建 Plan

```text
目标 Task/session
  └─ 创建 Plan A ─┐
  └─ 创建 Plan B ─┼─ 各自只读 Planner → 可选 Reviewer → plan_review
  └─ 创建 Plan C ─┘

Plan A reject  ───────────────► 仅 Plan A cancelled
Plan B approve ───────────────► approved / pending application
下一条用户消息选择 Plan B ───► [approved plan + 用户真实指令] 同一个 turn
```

- 每个目标 Task 最多 3 个并行运行中的 Plan；
- 主 Task 在 Plan 运行、完成、approve/reject 时均不变更状态；
- 已批准 Plan 不会自动附到早于 approve 已进入队列的消息；
- Chat composer 显示持久 Plan 附件，用户可选择一个或多个，也可暂不应用。

### 2.2 New Task 创建 standalone Plan

1. TaskForm 选择 Plan，创建独立 `mode="plan"` Task；
2. 只运行规划流水线，完成后进入 `plan_review`；
3. approve 后 Task 标记为已批准，但不执行；
4. 用户点击“创建执行 Task”，以 Plan 的项目、分支、provider 配置为基础创建新的
   `mode="auto"` Task，并把批准方案放进新 Task 的初始 prompt；
5. Plan Task 永远保留为只读审计记录。

### 2.3 Reject 与重新规划

- reject：`plan_approved=False, status="cancelled"`；
- “根据反馈重新规划”不是复活原 Plan，而是创建新 Plan Task；
- 新 Plan 记录 `supersedes_plan_task_id`，旧方案及审查历史保持不变。

## 3. 数据模型

### 3.1 Task 新字段

| 字段 | 说明 |
|---|---|
| `plan_target_task_id` | 关联目标 Task；NULL = standalone Plan |
| `plan_context_session_id` | 创建时 session 快照，仅审计，不作为关联主键 |
| `plan_context_log_id` | 创建时已纳入上下文的最大 LogEntry id |
| `plan_context_snapshot` | 有界且不可变的对话快照，供跨 Worker 的 Planner 使用 |
| `plan_repo_revision` | 创建时 HEAD + dirty 指纹 JSON |
| `supersedes_plan_task_id` | 用户要求重做时指向上一 Plan |
| `plan_pipeline_config` | 版本化 Planner/Reviewer primary/fallback 路由快照 |
| `plan_approved_at/by` | 审批审计 |
| `plan_applied_at` | 首次绑定到真实用户消息的时间 |
| `plan_applied_to_session_id` | 应用时目标 session 快照 |
| `plan_applied_log_id` | 携带该 Plan 的真实 user_message |
| `plan_execution_task_id` | standalone Plan 创建出的执行 Task |

关联必须使用 `task_id`。`Task.session_id` 会因压缩、恢复、换号和迁移变化，只能保存快照。

### 3.2 `plan_agent_runs`

记录一次 Plan Task 的 Planner/Reviewer 流水线：

- `id`, `plan_task_id`, `status`, `combo_used`, `round`;
- Planner/Reviewer provider/model/effort 快照；
- 完整 `pipeline_config` 快照；
- 最终 verdict、feedback、`review_exhausted`、error；
- `created_at`, `updated_at`, `finished_at`。

### 3.3 `plan_agent_steps`

每次模型步骤一行：

- `run_id`, `step_type`, `round`, provider/model/effort；
- `route_slot`（primary/fallback）与实际 `account_id`；
- `status`, 截断后的 output/error；
- `started_at`, `finished_at`。

Task 是用户可见 Plan 制品，run/step 是内部执行与审计记录，不复用
`sub_agent_sessions` 的回调式生命周期。

## 4. 过期与冲突

Plan 创建时记录：

- `plan_context_log_id`；
- 当前 `session_id`；
- Git HEAD；
- dirty workspace 的稳定摘要。

审批和应用时重新比较：

- 新增对话：提示“方案基于较早对话”；
- HEAD/dirty 指纹变化：提示“仓库状态已变化”；
- 目标 Task 已删除、迁移中或无控制权限：拒绝操作；
- 警告默认不硬阻止，用户二次确认后仍可 approve/apply。

多个已批准 Plan 不做隐式 latest-wins，也不自动合并；用户在 composer 明确选择本轮携带
哪些 Plan。服务端按 Plan id 顺序组装并保留来源标记。

## 5. Planner / Reviewer 流水线

### 5.1 Planner

- 一次性、无 Plan session 持久化；
- cwd 指向 Plan Task 的 repo；
- 输入为 Plan description、目标 Task 截止 `plan_context_log_id` 的对话摘要、repo 状态；
- 输出结构化实施方案。
- 默认 primary 为 `Claude / claude-fable-5 / high`，fallback 为
  `Codex / gpt-5.6-terra / xhigh`。

### 5.2 Reviewer

- 默认开启，可在全局 Plan Pipeline 设置中关闭；
- 独立一次性模型 turn；
- 审查方案与 repo 现状的一致性；
- verdict：`approve` / `revise`；
- revise 反馈回 Planner，默认总计最多 2 个完整 Planning/Reviewing round；
- 达上限后保留最新方案和 Reviewer 未解决意见，仍交用户审批，不自动失败。

Reviewer 不直接重写最终方案。需要修改时统一由 Planner 根据 feedback 产出新版本，保持
作者责任和审计链清晰。

默认 primary 为 `Codex / gpt-5.6-sol / xhigh`，fallback 为
`Claude / claude-sonnet-5 / high`。Planner 与 Reviewer 的模型、provider、effort
完全独立，不从主 Task 的执行模型隐式继承。

### 5.3 路由、选号与 fallback

每层配置一个 primary 和一个 fallback，route 是完整的
`{provider, model, effort}`，而不只是 model 字符串：

1. 对 primary 调用现有 `ClaudePool.select()` / `CodexPool.select()`；
2. 只选择声明支持该模型、当前可用且满足 Standard tier 的账号；
3. 已证明的 usage limit、auth、capacity/transient 失败会排除当前账号并尝试同 route
   的下一个兼容账号；
4. primary 的兼容账号耗尽后才尝试 fallback；
5. fallback 也耗尽时 Plan 直接 failed，不进入普通 Task 的通用重试/重新排队链。

因此不新增一套“选号逻辑”。Plan runner 只封装辅助模型步骤的 route fallback，账号可用性、
模型兼容、CloudRouter/Apex 投影、冷却与轮询仍由现有号池负责。每次尝试写入
`plan_agent_steps`，run 同时保留请求配置和实际成功组合，便于解释为什么发生 fallback。
Admin Settings 的 Plan Pipeline 区域将全局配置持久化到数据库；`.env` 的
`PLAN_PLANNER_*` / `PLAN_REVIEWER_*` 仅在尚未保存设置时提供部署回退。TaskForm 和
ChatView 不提供逐 Plan 覆盖，后端创建时固化全局设置快照，之后修改设置不会漂移已排队
或历史 Plan。API 暂时保留显式覆盖字段用于兼容已有客户端。

### 5.4 结构化输出

优先使用当前 CLI 的原生 schema：

- Claude：`--json-schema`；
- Codex app-server：`turn/start.outputSchema`；
- 服务端解包 provider 原生 envelope 后仍做严格字段校验；
- 当前 pinned CLI 的 schema 能力是运行前提；结构化输出不可用或不合法时显式失败，
  不降级为可能误判的自由文本协议。

### 5.5 真正只读

“禁用 Edit/Write”不是硬只读，因为 Bash 仍可写文件。

Claude：

- `--permission-mode plan`；
- `--no-session-persistence`；
- 只开放 `Read,Grep,Glob` 等读取工具，默认禁用 Bash、MCP、Agent、Task、Monitor；
- 创建/执行地记录无 optional lock、禁 fsmonitor 的 repo/dirty 指纹，审批和应用时用于
  stale 检测。

Codex：

- 复用按 `CODEX_HOME` 分片的常驻 Codex App Server，而不是另起 `codex exec`；
- 每个 Planner/Reviewer 步骤创建一个全新 disposable native thread，绝不 resume 主
  Task thread，终态后 `thread/delete` 并删除 rollout；
- `sandbox=read-only`，turn 层重复 `sandboxPolicy=readOnly` 且禁网络；
- whole-map `mcp_servers={}`，项目 trust 强制 untrusted；
- 显式关闭 Apps、MCP Apps、多 Agent fanout、memories 和其他 autonomous features。

Codex App Server 是 CCM 为每个账号 `CODEX_HOME` 维护的常驻 `codex app-server --stdio`
传输进程。它可以并发承载多个相互隔离的 native thread，省去每步重新启动 CLI 的成本；
这里复用的是 transport 和账号登录态，不是主 session/thread。

两者都必须经过安全的辅助进程执行器，具有：

- provider/model route、现有账号池与 CloudRouter 路由；
- timeout；
- transient retry；
- Claude 进程组 SIGINT → SIGTERM → SIGKILL 清理；
- Codex exact turn interrupt + disposable thread 删除；
- shutdown admission 和 exact process evidence；
- Dispatcher Instance 容量形成全局并发上限，目标 Task 另有最多三个 active Plan 的限制。

不能直接拿旧 GoalEvaluator 充当 Planner：GoalEvaluator 只读对话摘要并返回
`achieved/reason`，不看仓库也不产出方案。实现使用独立 `PlanAgentRunner`，但沿用
GoalEvaluator 已验证的账号路由、CloudRouter admission、Codex home guard、生命周期注册、
取消与 shutdown 回收原则。CloudRouter 在这里仅负责 API 账号投影的并发/凭据准入，不参与
Plan 的语义判断。`GoalEvaluator` 仍只服务 Goal 模式，不参与 Planner/Reviewer 决策。

## 6. Approve 与应用协议

### 6.1 Approve

`POST /api/tasks/{plan_task_id}/plan/approve`

- CAS 校验 Plan 仍处于 `plan_review`；
- 校验调用者同时有 Plan 与目标 Task 控制权；
- 返回 staleness 信息；若过期且未显式确认，返回 409；
- standalone：`plan_approved=True, status="completed"`；
- 关联 Plan：同样完成，并出现在目标 Task 的待应用 Plan 列表；
- 不调用 dispatcher、不开 turn、不改变目标 Task。

### 6.2 Reject

`POST /api/tasks/{plan_task_id}/plan/reject`

- 只更新 Plan Task；
- 不修改 `plan_target_task_id` 指向的 Task；
- 需要反馈时走 revise API 创建新 Plan；不在原记录上重跑。

### 6.3 应用到下一条消息

Chat 请求新增：

```json
{
  "message": "请开始按方案实施",
  "plan_task_ids": [123]
}
```

服务端在 task operation lock 内：

1. 校验每个 Plan 已批准、尚未应用、目标是当前 Task；
2. 校验 routing、ACL、staleness confirmation；
3. 持久化真实 user_message；
4. 写 Plan application 审计字段并绑定该 `user_log.id`；
5. 构建模型 prompt：

```text
[用户本轮明确选择的已批准方案]
<approved_plan task_id="123">
...
</approved_plan>

[用户本轮指令]
...
```

6. 作为一个普通 QueuedMessage 入队。

应用状态绑定 user log/queued message，而不是进程成功：同一 QueuedMessage 的 transient retry
仍携带相同方案，但后续新消息不会重复自动附加。

### 6.4 创建执行 Task

`POST /api/tasks/{plan_task_id}/plan/create-execution-task`

- 仅 standalone、已批准、尚未创建执行 Task 的 Plan 可调用；
- 幂等返回既有 `plan_execution_task_id`；
- 创建新的 `mode="auto"` Task；
- 初始 prompt 显式包含批准方案；
- 执行 Task 的 metadata 记录 `created_from_plan_task_id`。

## 7. API

```text
POST /api/tasks/{target_task_id}/plans          创建关联 Plan Task
GET  /api/tasks/{target_task_id}/plans          Plan 历史
POST /api/tasks/{plan_task_id}/plan/approve
POST /api/tasks/{plan_task_id}/plan/reject
POST /api/tasks/{plan_task_id}/plan/revise      创建独立修订 Plan
POST /api/tasks/{plan_task_id}/plan/create-execution-task
GET  /api/tasks/{plan_task_id}/plan/runs
POST /api/tasks/{plan_task_id}/cancel            取消 active Plan
GET  /api/settings/plan-pipeline                  读取全局 Plan Pipeline 设置
PUT  /api/settings/plan-pipeline                  保存全局 Plan Pipeline 设置
```

TaskForm 的 standalone Plan 创建继续走普通 Task create API，后端使用同一 Plan lifecycle。

WebSocket 事件只携带 id、状态、版本/摘要，不携带完整 plan_content。前端收到事件后通过
持久 API 回填，避免大 payload、断线丢内容和 Worker relay 重复复制。

## 8. 前端

### 8.1 ChatView

- 新增 Plans 按钮/抽屉；
- 展示关联 Plan 历史、独立进度、模型、过期状态和审批按钮；
- 输入框可创建新的 Plan；
- 已批准未应用 Plan 显示为持久 composer attachments；
- 发送时显式提交 `plan_task_ids`；
- 当前主 turn 运行中也可创建/审批 Plan，但应用只发生在 approve 之后新提交的消息。

### 8.2 TasksPage

- PlanPanel 处理全局待审批项和 standalone “创建执行 Task”；
- ChatView 的 Plans 面板处理关联历史、revision、附件选择和 stale 状态；
- 两处 approve 都采用相同的 stale 二次确认协议。

### 8.3 Settings

- 独立 Admin Settings 页面配置 Planner/Reviewer 的 primary/fallback provider、model、
  effort、Reviewer 开关和最大总轮数；
- 新建 standalone/关联 Plan 均使用同一份全局设置，创建表单不再展示模型配置。

## 9. Worker

- 关联 Plan 默认创建到目标 Task 当前 Worker，保证能读取同一 repo；
- Plan Task 使用 Manager 分配的全局 Task id，并携带 `plan_target_task_id`；
- 完整 `plan_pipeline_config` 随 Task 复制到 Worker；实际账号可用性由执行地 Worker
  解析，但不得改写模型 route 快照；
- 待审批或待应用的 Plan 会阻止目标 Task 迁移，关联 Plan 不能单独迁移，避免两者分居
  不同 Worker；
- Plan 内容通过持久 API/任务同步复制，WebSocket 只作失效通知；
- 迁移期间用现有 task operation lock 和 generation fence 返回 409；
- active、`plan_review`、approved-but-unapplied Plan 都阻止目标 Task 迁移；处理完成后
  可迁移，后续再支持成组迁移。

## 10. 测试与验收

### 10.1 后端

- 一个目标 Task 创建三个 Plan，第四个活跃 Plan 返回 429；
- 多 Plan 独立完成、approve/reject，目标 Task 状态/session/log 不变；
- approve 不调用 `dispatcher.wake/enqueue_message`；
- 过期 Plan 首次 approve/apply 返回 409，确认后成功；
- approve 前已排队消息不携带方案；
- `plan_task_ids` 只接受属于当前 Task 的已批准 Plan；
- 多 Plan 按显式顺序注入且只应用一次；
- session 压缩/换号后应用到当前 session；
- standalone Plan 创建执行 Task 幂等；
- Planner/Reviewer schema、revision、非法结构化输出、timeout、cancel；
- Planner/Reviewer 每层 primary/fallback、同 route 多账号耗尽、两路耗尽 terminal fail；
- Claude 只读命令、Codex disposable read-only App Server thread 与 repo 零写入；
- 配置 API 默认组合、Task/Worker 配置快照与 run/step route/account 审计；
- Worker route、断连、迁移 fence、ACL。

### 10.2 前端

- Plan 历史刷新/重连回填；
- 多 Plan 进度互不覆盖；
- approve 不触发聊天 sending 状态；
- 已批准 Plan attachment 跨刷新保留，可选择/取消；
- 发送 payload 含显式 Plan ids；
- standalone 创建执行 Task 后跳转新 Task；
- stale warning 二次确认。
- Settings 保存两层 primary/fallback 后，standalone TaskForm 和 ChatView 创建的新 Plan
  均固化同一份配置，且最大 2 轮不会出现第三次 Planning。

### 10.3 手工验收

1. 在有 session 的 Task 中同时创建两个 Plan；
2. 规划期间继续与主 Agent 对话，确认 Plan 不占用/唤醒主 session；
3. reject A，确认主 Task 无变化；
4. approve B，确认没有 Agent 回复；
5. 刷新页面，B 仍显示待应用；
6. 发送“按方案实施”并携带 B，确认 Agent 在同一 turn 收到完整方案；
7. 创建 standalone Plan，approve 后创建新的执行 Task；
8. Claude/Codex 各跑一次，并验证规划前后 repo 状态完全一致；
9. 对话和 HEAD 变化后验证 stale warning；
10. Worker Task 重复以上关键路径。
11. 临时令 Planner primary 不可用，确认尝试 fallback；再令两路都不可用，确认 Plan
    直接 failed 且不会重新排队。

## 11. 已落地模块

1. Task 字段、run/step 表、migration、状态机和 API；
2. 受限 `PlanAgentRunner`、两层独立 primary/fallback route 与账号耗尽语义；
3. Claude 一次性只读进程、Codex App Server disposable read-only thread；
4. dispatcher Plan lifecycle，删除旧 `_run_plan_phase` 和 ralph_loop 复制逻辑；
5. approve/reject/revise、Plan attachment、创建执行 Task；
6. 独立 Settings 页两层模型配置、ChatView Plans UI 与 TasksPage PlanPanel；
7. Worker、恢复、并发和 staleness；
8. TEST.md、README、CLAUDE.md/AGENTS.md 同步与 Plan 相关回归。
