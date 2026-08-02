# Auto Mode 对接 Plan Pipeline 交接说明

> 面向 Auto Mode 的实现者。本文描述当前一等 Plan Pipeline 的稳定边界、接入顺序和下游职责。
> Plan 团队只负责提供可调用的 Plan 能力；需求池、Auto 编排、Review Mode、PR 创建与合并均不在
> Plan Pipeline 内实现。

## 1. 结论

Auto Mode 应新增自己的持久化编排聚合，例如 `AutomationRun`，并把 Plan 当作一个可暂停、可恢复的
阶段调用。不要把 Auto 状态写进 `Plan`，也不要把现有 `Task.mode="auto"` 当成 Auto Mode：该值目前只
表示普通执行 Task。

```text
AutomationRun
  -> Plan / PlanAgentRun / PlanInputRequest / PlanVersion
  -> execution Task
  -> ReviewRun
  -> Pull Request
  -> PR merge result
```

Plan 侧已提供：

- 稳定的 `Plan` 聚合身份；
- 不可变的 `PlanVersion`；
- 可暂停/恢复的 `PlanAgentRun`；
- 持久化的 `PlanInputRequest`；
- exact-Version `PlanApplication` 幂等栅栏；
- Manager/Worker Plan relay；
- REST API；
- `PlanVersion -> execution Task` 的进程内服务边界。

## 2. 责任边界

### 2.1 Plan Pipeline 负责

- 冻结 Plan 创建时的 Pipeline 配置、上下文和 repo 指纹；
- 执行 Planner/Plan Reviewer；
- 在确实需要信息时创建 durable input request；
- 回答后恢复同一个 Run；
- 保存 immutable Version、review verdict、反馈和审计 Step；
- 检查目标、上下文与 repo staleness；
- 把一个已批准的 standalone Version 幂等物化为 execution Task。

### 2.2 Auto Mode 负责

- 从需求池 claim/续租/ack/nack；
- 创建并持久化 `AutomationRun`；
- 决定何时需要人工批准、何时允许策略自动批准；
- 把 Plan 的 `waiting_user` 映射到 Auto 的 Human-in-the-Loop；
- 监听并对账 Plan、Task、Review、PR 各阶段；
- 定义 execution provider/model/effort/tier 和交付策略；
- 实现 Review Mode、修复循环、PR 创建和 merge gate；
- 在最终 merged 后确认需求完成；
- 处理全链路取消、超时、重试、幂等和进程重启恢复。

## 3. Plan 权威对象

| 对象 | 作用 | Auto Mode 应保存的引用 |
|---|---|---|
| `Plan` | 稳定聚合根；当前 Version/活跃 Run 的权威指针 | `plan_id` |
| `PlanAgentRun` | 一次 initial/revision/refresh/retry 执行 | `plan_run_id`、`generation` |
| `PlanInputRequest` | 一轮持久化用户问题 | `input_request_id` |
| `PlanVersion` | 一次不可变 Plan 内容及 Reviewer 结果 | `plan_version_id` |
| `PlanApplication` | exact Version 被应用到 chat 或 execution Task 的唯一记录 | `application_id`、`execution_task_id` |

不要只保存 `current_version_id` 的当时值后长期信任。执行任何 decision/application 前，都要同时提交
`expected_current_version_id`，让 Plan 服务对并发 revision/fork 做 CAS 拒绝。

## 4. 推荐接入流程

### 4.1 创建 standalone Plan

跨进程调用使用：

```http
POST /api/plans
Content-Type: application/json

{
  "input": "需求原文",
  "title": "可选标题",
  "project_id": 123,
  "target_branch": "main",
  "worker_id": null,
  "priority": 0
}
```

如果不使用 Project，可传 `target_repo`。Auto 流程需要独立 execution Task，因此必须创建 standalone
Plan，即不要设置 `target_task_id`。related Plan 只能回到原 Task 的 chat application，不能创建新的
execution Task。

`POST /api/plans` 会同时创建 initial `PlanAgentRun`，返回值中的 `active_run` 即本次运行。Pipeline
配置来自当时的全局设置并冻结，Auto 不应在运行中改写。

同进程代码可以复用 `backend.services.plan_service.create_plan_with_run`，但调用者仍需负责 Project/
Worker 选择、权限、附件验证、上下文和 repo snapshot。不要调用 FastAPI endpoint 函数。

### 4.2 等待 Plan 结果

权威读接口：

```http
GET /api/plans/{plan_id}
GET /api/plan-runs/{run_id}
GET /api/plans/{plan_id}/versions
```

Run 状态：

| `PlanAgentRun.status` | 含义 | Auto 动作 |
|---|---|---|
| `queued` | 等待 Dispatcher/Worker | 保持 planning，稍后对账 |
| `running` | 正在执行一个 Planner/Reviewer Step | 保持 planning |
| `waiting_user` | `open_input_request` 已持久化，且执行资源已释放 | 进入 waiting human input |
| `completed` | 本次 Run 已有终态 Version/Reviewer 结果 | 读取 exact `result_version_id` |
| `failed` | 技术失败或交互轮次耗尽 | 按 Auto 重试策略创建 retry Run 或转人工 |
| `cancelled` | 已被明确取消 | 终止或按上层策略重建 |

WebSocket 的 `plans`、`plan:{plan_id}` 以及关联的 `task:{task_id}` channel 只能作为低延迟唤醒信号。
广播是 best-effort，不是 durable queue；收到事件后必须重新 GET aggregate snapshot，遗漏事件时也必须由
周期 reconciler 收敛。

当前常见事件包括：

```text
plan_created
plan_run_created
plan_run_status_changed
plan_input_requested
plan_input_answered
plan_version_created
plan_version_reviewed
plan_version_decided
plan_version_applied
```

### 4.3 处理 Plan 问题

当 Run 为 `waiting_user` 时，从 `PlanResource.open_input_request` 读取完整问题。回答接口：

```http
POST /api/plan-runs/{run_id}/input-requests/{request_id}/answer
Content-Type: application/json

{
  "expected_run_generation": 4,
  "idempotency_key": "auto-run:88:plan-input:301:answer:1",
  "answers": [
    {"question_id": "scope", "value": "selected-value"}
  ],
  "response_text": null
}
```

要求：

- 必须使用读取问题时看到的 exact Run generation；
- 每次逻辑回答使用稳定 idempotency key；HTTP ACK 丢失后重试同一个 key；
- 不要把 Plan 问题转写成主 Task chat 消息；
- 409 表示 generation、open request 或 Plan 状态已变化，应重新读取 aggregate；
- Worker 断线时答案仍保存在 Manager，Auto 不应直接调用任何 `/worker-*` 内部接口。

### 4.4 判断 Version 是否可继续

Run `completed` 不等于已经允许执行。读取 exact `PlanVersion`：

```http
GET /api/plan-versions/{version_id}
GET /api/plan-versions/{version_id}/staleness
```

主要字段：

- `review_verdict=approve`：Plan Reviewer 通过；
- `review_verdict=disabled`：全局配置关闭 Reviewer；
- `review_verdict=exhausted` 或 `review_exhausted=true`：Reviewer 修改轮次耗尽；
- `human_decision=pending|approved|rejected`：当前人工 decision 模型；
- `superseded_by_version_id`：Version 已被新版本替代；
- `applied`：该 Version 已有唯一 application。

当前 Plan 服务没有实现 Auto 的“策略批准来源”。Auto 团队必须明确设计审计模型，不能把系统策略伪装成
真人。完成该设计前，自动化调用应保持 `approve_if_pending=false`，只消费已经显式 approved 的 Version。
`exhausted` 不应默认视为通过。

### 4.5 物化 execution Task

#### 同进程稳定服务

调用：

```python
from backend.services.plan_service import materialize_execution_task

result = await materialize_execution_task(
    db,
    plan_id=plan_id,
    version_id=plan_version_id,
    expected_current_version_id=plan_version_id,
    confirm_stale=False,
    approve_if_pending=False,
    actor_id=system_user_id,
    execution_metadata={
        "auto_run_id": str(auto_run_id),
        "requirement_claim_id": str(requirement_claim_id),
    },
)
execution_task_id = result.task.id
```

`PlanExecutionTaskResult` 返回：

- `plan`；
- exact `version`；
- `application`；
- `task`；
- `created`：本次是否真的创建了 Task。

服务保证：

- 只接受 standalone Plan；
- `expected_current_version_id` 不匹配时拒绝；
- hard conflict 永远拒绝；普通 stale 必须显式确认；
- Version 必须 approved；
- Task 与 `PlanApplication` 同一事务落库；
- `plan_version_id` 是天然幂等键；重复调用返回原 Task；
- 并发进程由数据库 unique fence 收敛到同一个 application；
- `execution_metadata` 允许上层保存关联 ID，但不能覆盖
  `created_from_plan_id/created_from_plan_version_id`。

服务不负责：

- HTTP 用户权限；
- Auto 的 approval policy；
- Dispatcher wake；
- WebSocket/outbox；
- execution provider/model/effort/tier；
- branch/PR delivery policy。

调用成功提交后，上层应唤醒 Dispatcher。若只有本服务调用而漏掉 wake，Dispatcher 的轮询仍会最终发现
pending Task，但不应依赖该延迟。只有 `result.created=true` 时才需要发布新的下游启动事件；重放返回
`created=false` 时只需对账已有 Task。

#### REST adapter

已有 API：

```http
POST /api/plan-versions/{version_id}/create-execution-task
Content-Type: application/json

{
  "expected_current_version_id": 17,
  "confirm_stale": false,
  "approve_if_pending": false
}
```

REST adapter 已调用上述服务，并负责权限、Dispatcher wake 和 `plan_version_applied` 广播。返回：

```json
{
  "plan": {},
  "version": {},
  "execution_task_id": 9001
}
```

外部 Auto 服务可调用 REST；同一后端进程内的 Auto coordinator 应调用 service，不要绕回 HTTP，也不要
复制 ORM 创建逻辑。

### 4.6 错误处理

| 情况 | 典型响应 | Auto 行为 |
|---|---|---|
| current Version 已变化 | 409 `plan_version_changed` | 重读 Plan，禁止执行旧缓存 |
| repo/session/context 普通过期 | 409 `plan_stale` | 按策略 refresh/replan 或请求人工确认 |
| Project/Task/Worker/repo 不可用 | 409 `plan_hard_conflict` | fail closed，转人工/运维修复 |
| Version 尚未批准 | 409 | 等 decision；不要静默绕过 |
| Version 已应用到 chat | 409 | 不得再创建 execution Task |
| execution application 已存在 | 返回原 Task | 继续追踪原 `execution_task_id` |
| application 指向的 Task 丢失 | 409 | 数据完整性问题，转人工；禁止重建第二个 Task |

## 5. Auto Mode 推荐持久化模型

不要用一串内存 callback 串联阶段。推荐至少保存：

```text
AutomationRun
  id
  status / stage
  generation / lock_version
  requirement_claim_id
  plan_id
  plan_run_id
  selected_plan_version_id
  execution_task_id
  execution_generation
  review_run_id
  base_sha / head_sha / branch
  pr_number / pr_url / expected_pr_head_sha
  policy_snapshot
  waiting_reason
  attempt counters
  timestamps / error
```

推荐状态：

```text
claiming
  -> planning
  -> waiting_plan_input
  -> plan_ready
  -> executing
  -> reviewing
  <-> fixing
  -> publishing_pr
  -> awaiting_merge
  -> merged

side terminals: needs_attention / failed / cancelled
```

每个 transition 必须由数据库 CAS 驱动，并能在进程重启后通过 Plan/Task/Review/PR 的权威状态重新对账。

## 6. 下游必须实现但 Plan 团队不实现的内容

### 6.1 Requirement source adapter

至少需要 `claim/renew/ack/nack`、lease owner/expiry、source revision、dedupe key 和 attempt。只能在最终
merged 后 ack；不能在 Plan 或 Task 创建后提前完成需求。

### 6.2 Approval policy

定义并持久化例如：

```text
plan_gate = human_approved | reviewer_approved
decision_source = human | automation_policy
policy_snapshot = exact immutable JSON
```

建议 reviewer `approve` 才允许策略通过；`disabled/exhausted/stale/hard conflict` 均需明确策略，默认
fail closed。需要相应数据库迁移，不能只写 Task metadata。

### 6.3 Durable orchestration signal

当前 Plan WebSocket 是 invalidation hint。Auto 若需要可靠事件，增加 transactional outbox，或让
`AutomationRun` reconciler 定期扫描权威表。不得用“没收到 WS”推断失败，也不得用“收到 completed 事件”
代替读取 exact Version。

### 6.4 Execution configuration 与 delivery strategy

当前物化服务创建普通 pending Task，沿用现有 Task/Dispatcher 行为。Auto 应扩展稳定接口，显式传入并
校验 execution provider/model/effort/tier、超时和：

```text
delivery_strategy = direct_merge | pull_request
```

`pull_request` 模式必须只提交并 push feature branch，持久化 base/head SHA 和 dirty/test 状态，禁止执行
现有“直接合并默认分支”的 Agent 指令。

### 6.5 Review Mode

Plan Reviewer 只评审计划，不是代码 Review Mode。新增 `ReviewRun`，至少绑定：

- exact execution Task id/generation；
- base SHA、head SHA、tree/diff fingerprint；
- verdict：approved/changes_requested/waiting_user/technical_failure；
- feedback、round、fix attempt。

Task 进程 exit 0 不能单独证明可 Review 或可合并，必须先形成确定性的 execution artifact。

### 6.6 PR 创建与 PR Monitor merge gate

- 服务端幂等 create-or-find PR，不交给 LLM 自由决定；
- Review attestation 必须绑定 exact head SHA；
- `synchronize` 后旧 attestation 失效并重新 Review；
- merge 前检查 expected head SHA、required checks、mergeability 和 branch protection；
- 优先使用 GitHub auto-merge/merge queue，并监听 merged/closed 后再完成 AutomationRun；
- requirement ack 只能发生在确认 merged 之后。

## 7. 关键反模式

- 不要在 Auto coordinator 中 import/call `backend.api.plan_resources` endpoint 函数；
- 不要复制 `Task(...)` + `PlanApplication(...)` 创建代码；
- 不要使用可变的 `Plan.current_version_id` 代替已选 exact Version；
- 不要把 WS 当消息队列；
- 不要自动把 `exhausted` 当 approve；
- 不要用 `Task.mode="auto"` 表示 AutomationRun；
- 不要在 Worker 直接改 Manager 权威 Plan/Version/decision/application；
- 不要因 HTTP ACK 丢失而创建第二个 Plan、回答、Task 或 PR；
- 不要在 Review 前让执行 Task 直接 merge 默认分支。

## 8. Auto Mode 最低验收用例

1. 创建 Plan 后进程重启，AutomationRun 能从 `plan_id/run_id` 继续对账。
2. Planner 连续两轮提问，每次都释放执行资源，回答 exact generation 后恢复同一 Run。
3. 回答 HTTP ACK 丢失，重复 idempotency key 不产生第二份回答。
4. Plan 完成后又产生 revision，旧 `expected_current_version_id` 创建 Task 返回 409。
5. repo 变化产生 stale，未确认不能创建 Task；hard conflict 永远不能绕过。
6. 两个 coordinator 并发物化同一 Version，只产生一个 Task/Application。
7. 物化 ACK 丢失后重放，返回相同 `execution_task_id` 且 `created=false`。
8. Manager/Worker 断线恢复后不会重复 Plan Step、Version、input answer 或 application。
9. Auto 取消能精确取消 active PlanRun，并保留历史 Version/Input 审计。
10. Reviewer exhausted、Task exit 0、PR head SHA 改变均不会误触发 merge。

## 9. 代码入口

- 聚合模型：`backend/models/plan.py`
- 请求/响应契约：`backend/schemas/plan_resource.py`
- 领域服务与 execution materializer：`backend/services/plan_service.py`
- REST adapter：`backend/api/plan_resources.py`
- Planner/Reviewer 单步推进：`backend/services/plan_agent_runner.py`
- 本地与 Worker 调度：`backend/services/dispatcher.py`
- Worker relay client：`backend/services/worker_proxy.py`
- staleness：`backend/services/plan_staleness.py`
- best-effort realtime invalidation：`backend/services/plan_events.py`
- 当前完整设计：`docs/plans/interactive-versioned-plan-design.md`
- 接入与幂等测试：`backend/tests/test_plan_resources.py`
