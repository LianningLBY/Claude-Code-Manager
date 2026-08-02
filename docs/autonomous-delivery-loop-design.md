# CCM Autonomous Delivery Loop — Todo 实现方案

- 文档状态：待实施
- 文档版本：v0.2
- 更新日期：2026-08-02
- 文档类型：可领取、可验收、可逐项勾选的实施 Backlog
- 目标：让 CCM 用持久状态机驱动“开发 → PR → CI → Review → Merge → Deployment”的自动交付循环
- 当前安全 MVP：本地执行、一个仓库、一个 Developer Task、一个 PR、GitHub Actions、CI 失败自动修复、人工合并

> 本文中的 `[ ]` 表示尚未实现。只有代码、测试、迁移、文档和回退验证全部完成后，才能改为 `[x]`，并在 Todo 的“证据”栏补充 commit、PR 或测试记录。

---

## 1. 文档使用方式

### 1.1 Todo 状态约定

- `[ ]`：未开始或尚未达到 DoD。
- `[x]`：已实现并通过该项列出的全部验收条件。
- `Blocked`：只能写在 Todo 的说明中，不能把 checkbox 勾上。
- 每个 Todo 的 ID 永久稳定；拆分后保留原 ID，并增加子编号。
- 实施中若改变本文冻结的安全边界，必须先新增 ADR，再修改本文件。

### 1.2 优先级

- `P0`：不先完成就可能绕过 Gate、泄露凭据、重复执行副作用或破坏现有任务。
- `P1`：安全 MVP 必需。
- `P2`：Reviewer、Worker、自动合并等增强能力。
- `P3`：部署、自动回滚与规模化运维。

### 1.3 每个 Todo 的完成定义

每项至少要有：

1. 实现代码和必要迁移。
2. 单元测试与关键竞态测试。
3. SQLite 验证；涉及核心数据模型时补 PostgreSQL/MySQL smoke test。
4. API、WebSocket 或 Worker 协议变更的向后兼容验证。
5. 可操作的关闭或回退路径。
6. 对应文档和 `AGENTS.md` / `CLAUDE.md` 的必要增量更新。

---

## 2. 已冻结的核心决策

以下决策是本实施方案的基础，不在具体 Todo 中临时改变。

### 2.1 控制权

- Agent 负责分析、修改代码、运行本地测试、commit，并把交付分支 push 到远程。
- CCM Delivery Controller 负责长期状态、等待、唤醒、Gate、PR 创建、Review 发布、Merge 和 Deployment 编排。
- GitHub、CI、Merge Queue 和 Deployment Adapter 是外部事实来源。
- Agent 的自然语言结论和结构化 checkpoint 都只是提示，不是 Gate 证据。

### 2.2 生命周期对象

- `Task`：可跨多轮续聊的 Agent 会话，以及它当前这一刻的执行状态。
- `DeliveryRun`：从需求进入到交付完成的一次完整流程。
- `DeliveryTurn`：由一个持久事件触发的一次有限 Agent 回合。
- `Instance`：可复用执行槽，不是 Run 或 Turn 的长期 owner。
- `Task.retry_count` 是 Task 失败重试计数，不是 DeliveryTurn generation。

### 2.3 不复用 Legacy Loop 作为交付循环

现有 `mode=loop` 和 `LoopChatView` 处理的是 todo 文件的连续模型迭代。Delivery Loop 必须：

- 在等待 CI、Review、Merge 或部署时释放模型进程和 Instance。
- 由数据库里的事件与状态重新唤醒。
- 不使用模型轮询 GitHub。
- 不使用 Ralph Loop 或 GoalEvaluator 判断外部 Gate。

### 2.4 Git 副作用边界

Developer Agent 可以：

- 修改受控 worktree。
- commit。
- push 受控交付分支。

Developer Agent 不可以：

- push 默认分支。
- 创建或修改 branch protection。
- merge PR。
- 自己把 CI、Review 或 Deployment 标为通过。

下列动作全部由 CCM Action Outbox 执行：

- 创建 PR。
- 发布 CCM Review / Finding 评论。
- 重新运行 CI。
- 进入 Merge Queue 或执行 merge。
- 触发部署和回滚。
- 发送持久通知。

### 2.5 Checkpoint 协议必须 Provider-neutral

CCM 默认 Provider 是 Codex，而 Codex 不支持 CCM 的 Claude MCP Skills 注入。因此：

- 不把 `report_delivery_checkpoint` MCP 工具作为必需协议。
- Controller 在唤醒前持久化 `DeliveryTurn`。
- Agent 可以在最终输出中附带受限 JSON checkpoint。
- Dispatcher / Worker Relay 解析 checkpoint 后只保存为 advisory evidence。
- 不论 checkpoint 是否存在，回合结束后都必须重新查询 Git 和 GitHub。

### 2.6 验证对象不只有 PR head SHA

统一使用 `VerificationSubject`：

| subject_kind | 身份 | 用途 |
|---|---|---|
| `pr_head` | `head_sha + base_sha` | PR CI、AI Review、mergeability |
| `merge_group` | `merge_group_sha` | Merge Queue 在最新 base 上的验证 |
| `merged_revision` | `merge_commit_sha` | 合并后的代码身份 |
| `deployment_artifact` | `deployment_id + environment + artifact/ref/digest` | 部署与健康验证 |

旧 subject 的成功证据不能推进新 subject 的 Gate。

### 2.7 部署失败不会复活已合并 PR

PR 合并后发生应用错误时，不能回到原 PR 继续 push。系统只能：

- 创建 child remediation DeliveryRun 和新 PR；或
- 进入持久 Human Gate / Incident；或
- 在未来启用的精确环境代次保护下执行回滚。

### 2.8 默认关闭所有有副作用的自动化

默认值：

```text
delivery observe       = off
delivery agent wake    = off
delivery AI review     = off
delivery auto merge    = off
delivery deployment    = off
delivery auto rollback = off
```

首次上线只能按 `off → shadow → ci_repair → review → manual_merge → auto_merge → deploy` 逐级开启。

---

## 3. 首版范围与明确非目标

### 3.1 安全 MVP

首个可上线版本只支持：

```text
本机 Project（有 GitHub remote）
→ CCM 创建固定交付 branch/worktree
→ Developer Task 完成有限回合并退出
→ CCM 创建或绑定一个 PR
→ 等待 exact PR-head CI
→ CI 代码失败时聚合为一个新 DeliveryTurn
→ Developer 修复并 push 新 SHA
→ 所有 required checks 通过
→ 停在“等待人工合并”
```

### 3.2 MVP 明确拒绝

- `worker_id != null` 的 DeliveryRun。
- Shared shadow Task 发起或控制 DeliveryRun。
- 已经有 session 的普通 Task 原地转换成受控 DeliveryRun。
- 一个 Run 管理多个仓库或多个 PR。
- 自动 AI Review Gate。
- 自动 Merge Queue / merge。
- 自动部署或自动回滚。
- 无 remote 的纯本地项目。
- 将现有 `mode=goal`、`mode=loop` 任务直接作为 Developer Task。

这些请求必须返回带稳定错误码的 4xx，不允许静默降级成本机普通 Task。

### 3.3 后续完整目标

- 多角色、独立证据的 Reviewer Panel。
- Worker 上执行 Developer / Reviewer Turn。
- Controller-only Merge Queue。
- 可插拔 Deployment Adapter。
- child remediation Run。
- 持久通知、审计、指标和 dead-letter 运维。

---

## 4. 当前 CCM 基线与连接矩阵

| 模块 | 当前实现 | 可复用 | Delivery 必须补充或修改 |
|---|---|---|---|
| Task | `backend/models/task.py` | session、cwd、provider、model、Worker 归属、续聊 | 一等 Run/role 关联；活跃 Run 的 chat/retry/edit/delete/migrate 规则 |
| TaskQueue | `backend/services/task_queue.py` | pending claim、优先级、Task CAS | `create()` 会自行 commit，不能承担 Run+Task+Turn 原子创建 |
| Dispatcher | `backend/services/dispatcher.py` | Task→Instance 锁序、exact generation CAS、账号轮换、per-task 串行 resume | durable Turn bridge、turn correlation、terminal callback；不能扩大成 Delivery 状态机 |
| InstanceManager | `backend/services/instance_manager.py` | Claude/Codex 进程、PTY、session、限额处理 | 接收 Delivery context 和 credential role；不保存 Delivery 真相 |
| Chat / Inject | `backend/api/chat.py` | 用户消息、同 Task resume、运行中 steer/inject | 活跃 Run 的消息先变成持久 intervention event，再由 Controller 决定 wake |
| Task termination | `backend/services/task_termination.py` | 本机/Worker exact-generation 停止与 Task→Instance 锁序 | pause/cancel/supersede 必须复用；禁止直接写 Task terminal 状态 |
| Worktree | `backend/services/worktree_manager.py`、`backend/models/worktree.py` | create/sync 的基础 git 操作 | 目前未接 Dispatcher；需持久 Run/Task 绑定和非破坏性恢复/清理 |
| Project | `backend/models/project.py`、`backend/api/projects.py` | repo path、remote、默认分支、Git 凭据 | 当前 Agent 文档要求直接 merge/push main；Delivery 必须覆盖并由服务端防线约束 |
| Project Todo | `backend/api/project_todos.py`、`ProjectTodoList.tsx` | 需求模板、Task 溯源 | 新增“以 Delivery 运行”，使用原子 Run 创建接口 |
| Plan Review | `Task.plan_content/plan_approved`、`PlanPanel.tsx` | 人工批准方案 | 计划作为 snapshot/hash 输入 Developer Task；不复用同 session 强行换 cwd |
| PR Monitor | `backend/api/pr_monitor.py` | HMAC webhook、Repo 配置、synchronize 安全终止旧 Task | 持久 delivery-id、更多事件、exact subject、多 Reviewer、ACL/secret 修复 |
| PR Review Service | `backend/services/pr_review_service.py` | Reviewer Task 创建骨架、Dispatcher 完成 hook | 当前允许 Agent merge，且未知/无 Review 可被当作 approved；Delivery 前必须重构 |
| Worker | `worker_proxy.py`、`worker_relay.py`、`task_migrator.py` | 全局 Task ID、relay、权威快照、迁移锁 | capability handshake、Turn receipt/idempotency、Delivery 字段和 workspace 协议 |
| Shared Task | Owner/shadow relay | owner 权威模型 | 仅 owner CCM 可控制 Run；shadow 只读，chat 权限不等于 merge/deploy 权限 |
| Monitor/Sub-agent | 持久子 Agent 与消息回注 | 同一 Turn 内辅助诊断 | 不能成为 Delivery truth；MVP 禁止 Monitor 在主 Turn 结束后自行唤醒 Developer |
| AskUser | `ask_user.py` + 卡片 | 交互样式、未读提醒 | registry 是内存 Future，不能作为数小时 Human Gate；新增持久 Decision |
| WebSocket | `ws.py`、`ws_broadcaster.py`、`api/ws.ts` | channel、重连、Task ACL | 仅作刷新提示；新增 Run/user ACL、sequence/version，REST snapshot 才是事实 |
| Settings | `config.py`、`GlobalSettings` | env + runtime override | 全局 kill switch、lease/reconcile 参数、repo policy snapshot |
| Alembic | `alembic/env.py`、`database.py` | SQLite/PostgreSQL/MySQL 迁移 | 新 model import、单 head、多数据库 smoke；不能依赖 SQLite 行锁/FK 行为 |
| UpdateService | CCM 自身更新与回滚 | lease、exact commit、重启恢复经验 | 不是任意项目 Deployment Adapter，禁止直接复用其 API |
| BackupService | 可选数据库备份 | 运维参考 | SQLite 文件语义不能证明 PostgreSQL/MySQL 可恢复 |
| Frontend | Tasks、Chat、PR Monitor | 现有会话和配置入口 | 独立 Delivery Run 页面、Gate、Timeline、Finding、Decision、Deployment |

### 4.1 当前必须承认的安全缺口

在 Delivery 自动合并前，以下问题必须先处理：

1. PR Monitor 的部分 detail/review GET 未按目标 repo 做 owner ACL。
2. Repo 写操作只检查“用户拥有任一 Worker”，没有验证目标 `repo.worker_id`。
3. 前端通过 `PUT {}` 读取 detail 和完整 webhook secret。
4. Legacy `auto_merge` 让 Reviewer Agent 直接执行 `gh pr merge`。
5. Review 回查在没有可识别 Review 时可能落成 approved。
6. Webhook 没有用 `X-GitHub-Delivery` 做数据库唯一去重。
7. Dispatcher follow-up queue 是内存 `asyncio.PriorityQueue`，重启会丢 wake。
8. 当前 Project Git 凭据没有在 CCM 内证明“只能 push 工作分支”。

---

## 5. 目标架构与数据流

```text
                         GitHub / CI / Merge Queue
                                    │
                                    │ signed webhook
                                    ▼
                         ┌──────────────────────┐
                         │ Delivery Event Inbox │
                         │ verify / dedupe / DB │
                         └──────────┬───────────┘
                                    │ durable wake hint
                                    ▼
┌──────────────┐          ┌──────────────────────┐
│ Human/API/UI │─────────▶│ Delivery Controller  │
└──────────────┘ command  │ reducer / lease / CAS│
                          └──────┬────────┬──────┘
                                 │        │
                         reconcile│        │effect intents
                                 ▼        ▼
                     ┌───────────────┐  ┌────────────────┐
                     │ GitHub Gateway│  │ Action Outbox  │
                     │ exact snapshot│  │ PR/merge/deploy│
                     └───────┬───────┘  └────────────────┘
                             │
                             ▼
                     ┌───────────────┐
                     │ Gate Evaluator│
                     │ pure function │
                     └───────┬───────┘
                             │ actionable failure
                             ▼
                     ┌──────────────────┐
                     │ Durable Turn     │
                     │ Bridge           │
                     └────────┬─────────┘
                              │ turn_id / receipt
                     ┌────────▼─────────┐
                     │ Dispatcher/Worker│
                     │ Developer Task   │
                     └────────┬─────────┘
                              │ terminal hint
                              └──────────────▶ reconcile again
```

### 5.1 Authority Boundary

| 数据 | 权威来源 |
|---|---|
| Task 当前是否有模型进程 | Task + Instance exact generation + InstanceManager/Worker snapshot |
| Run 当前状态 | DeliveryRun reducer 的持久结果 |
| Turn 是否被调度 | DeliveryTurn DB 状态和 receipt |
| 分支/PR/head/base/merge commit | GitHub Gateway 的当前 snapshot |
| CI | 当前 VerificationSubject 的 Check Run / Commit Status snapshot |
| AI Review | 明确完成的 DeliveryReviewRun + structured result |
| Finding 是否解除 | 后续 Reviewer 验证或有权限的人类 waiver |
| 是否可 merge | Gate Evaluator |
| 外部动作是否成功 | 远端对账 + DeliveryAction |
| 部署是否成功 | Deployment Adapter + health evidence |
| UI | GET snapshot；WebSocket 只提示重新拉取 |

---

## 6. 正式状态模型

### 6.1 Phase

```text
preparing
developing
validating
reviewing
merging
deploying
done
```

### 6.2 Activity

```text
ready      # Controller 可以产生下一个 effect
running    # 一个 Agent Turn 或 Controller action 正在执行
waiting    # 等待外部证据、计时器或人工回答
paused     # Operator/安全策略暂停，不自动产生新 effect
terminal   # Run 结束
```

### 6.3 Outcome

Outcome 只允许出现在 `activity=terminal`：

```text
success
failed
cancelled
superseded
```

`blocked` 不再是 terminal outcome。可恢复阻塞表示为：

```text
activity=waiting, wait_reason=human_decision
activity=paused, pause_reason=no_progress
activity=paused, pause_reason=permission_missing
```

### 6.4 合法组合

| Phase | Activity | 含义 |
|---|---|---|
| preparing | ready/running/waiting | 创建 workspace、Task、首 Turn |
| developing | ready/running/waiting | Developer Turn 或等待被验证的 push |
| validating | ready/waiting | 等待或计算 PR-head CI Gate |
| reviewing | ready/running/waiting | Reviewer Turn 与 Finding Gate |
| merging | ready/running/waiting | PR/merge queue action 与 merge-group Gate |
| deploying | ready/running/waiting | exact merged revision 的部署与健康检查 |
| 任意非 done | paused | 人工暂停或安全阻塞 |
| done | terminal | 必须有 outcome 和 completed_at |

除此之外的 tuple 均非法，reducer 必须拒绝。

### 6.5 主要状态流程

```text
preparing
  ├─ workspace / Task 准备失败 → paused(permission|workspace)
  └─ 准备完成 → developing/ready

developing/ready
  → 创建 durable DeliveryTurn
  → developing/running
  → Task 回合结束
  → 对账远程 branch
      ├─ 没有可验证 push → developing/ready 或 paused(no_progress)
      └─ 新 head → validating/waiting

validating/waiting
  ├─ required CI running/missing → 继续等待
  ├─ infrastructure failure → 有界 rerun action
  ├─ code failure → developing/ready
  └─ 全部通过 → reviewing 或 merging

reviewing
  ├─ Reviewer 未完成/失败 → waiting 或 paused
  ├─ blocking Finding → developing/ready
  └─ required Reviewer 全部完成且无 blocking Finding → merging/ready

merging
  ├─ manual policy → waiting(human_merge)
  ├─ merge-group 失败 → developing/ready
  ├─ head/base/Gate 变化 → validating/waiting
  └─ merge 成功 → deploying 或 done/success

deploying
  ├─ transient infra → bounded retry
  ├─ app failure → child remediation / Human Gate / rollback decision
  └─ exact artifact + health passed → done/success
```

### 6.6 reducer 规则

- 只有 `backend/services/delivery_reducer.py` 可以计算 Run 状态变更。
- API、Webhook、Dispatcher、Worker Relay 和 Outbox 不直接写 phase/activity/outcome。
- 每次变更使用 `state_version` CAS，并写一条 timeline transition。
- reducer 只计算状态与 effect intent，不执行网络调用。
- 不持有数据库锁调用 GitHub、Worker、Agent、WebSocket 或 Deployment Adapter。

---

## 7. Gate 与 VerificationSubject

### 7.1 Gate 统一返回值

```json
{
  "subject": {
    "kind": "pr_head",
    "head_sha": "def456",
    "base_sha": "abc123"
  },
  "ready": false,
  "blockers": [
    {
      "code": "ci_failed",
      "logical_id": "github-actions/backend-tests",
      "actionable_by": "developer"
    }
  ],
  "observed_at": "2026-08-02T12:00:00Z"
}
```

### 7.2 PR-head Gate

必须同时满足：

- PR open 且非 draft。
- repo numeric ID、PR number、head SHA 和 base SHA 与 Run 当前 subject 一致。
- required checks 全部达到 policy 允许的终态。
- GitHub branch-protection review 和 CCM AI Review 分别满足各自 policy。
- blocking Finding 为零。
- mergeability 明确为可合并；`unknown` 不能通过。
- 没有 pending Human Decision、active Turn、pause/cancel 或 stale policy。

### 7.3 CI identity

Required check 不能只用展示名称。至少保存：

```text
check kind + logical context/name + source app ID + verification subject
```

统一状态：

```text
missing / queued / running / passed / failed / cancelled / skipped / neutral / stale
```

策略：

- MVP 使用显式 required-check 列表。
- `missing` 默认等待，超时后变成阻塞，不视为成功。
- `skipped`、`neutral` 是否允许由 policy 明确配置。
- 同名但不同 App 的结果不得互相替代。
- GitHub API 不可用只退避，不唤醒 Agent。
- 只有可证明的基础设施失败才能自动 rerun；代码失败交给 Developer。

### 7.4 AI Reviewer Gate

- 每个 `DeliveryReviewRun` 绑定 reviewer role 和 exact subject。
- 空输出、解析失败、Task 失败、没有 completion marker 都是未通过。
- Developer 可以提交修复或 dispute，但不能把 Finding 标记 resolved。
- 新 head 默认 full rerun required reviewers；diff-aware carry-forward 作为后续优化，不能先做。
- 多 Reviewer 不采用简单多数票：所有 required role 必须明确完成，任一 blocking Finding 都阻塞。

### 7.5 Merge Queue Gate

- Merge Queue 使用 `merge_group` subject，而不是 PR head subject。
- merge-group 重建后，旧 merge-group CI 全部 stale。
- 进入队列和最终 merge 前都重新读取 PR、head/base、Gate、active Turn 和 Finding。

### 7.6 Deployment Gate

- 只验证 exact `merged_revision` 或由它生成的 exact artifact。
- Deployment status 成功与 health check 成功是两个独立条件。
- 环境当前 revision 与 rollback expected generation 不一致时，旧 Run 禁止回滚。

---

## 8. 数据模型与迁移方案

建议新增 `backend/models/delivery.py`，初期集中放置模型，稳定后再按领域拆分。

### 8.1 DeliveryRepoPolicy

关键字段：

```text
id
monitored_repo_id
enabled
automation_level
required_checks_mode
required_checks
allowed_terminal_conclusions
reviewer_policy
merge_method
deployment_mode
deployment_adapter
deployment_config        # 只放非敏感配置和 Secret ID
max_turns
max_no_progress
ci_rerun_budget
reconcile_interval_seconds
version
created_at / updated_at
```

规则：

- Run 创建时完整复制为 `policy_snapshot`，运行中不跟随配置漂移。
- Repo policy 后续变化只影响新 Run；旧 Run要升级策略必须显式 command + audit。
- Legacy `MonitoredRepo.auto_merge` 和 Delivery `auto_merge` 不允许同时生效。

### 8.2 DeliveryRun

```text
id
created_by
project_id
monitored_repo_id
source_todo_id
planning_task_id
developer_task_id
parent_run_id
github_repository_id
github_repository_node_id
repo_full_name_snapshot
base_branch
delivery_branch
workspace_id
pr_number / pr_url
head_sha / base_sha
head_generation
merge_group_sha
merge_commit_sha
phase / activity / outcome
wait_reason / wait_set
gate_snapshot
policy_snapshot / policy_hash
requirements_snapshot / requirements_hash
plan_ref / plan_hash
state_version
controller_generation
lease_owner / lease_expires_at
active_turn_id
next_reconcile_at
reconcile_failures
no_progress_count
last_progress_signature
pause_reason / error_code / error_message
created_at / updated_at / completed_at
```

索引：

- `(activity, next_reconcile_at)`。
- `(monitored_repo_id, pr_number)`。
- `developer_task_id`。
- `(lease_expires_at, activity)`。
- `(github_repository_id, delivery_branch)` 唯一。

### 8.3 DeliveryTurn

```text
id
run_id
generation
correlation_id
trigger_type
trigger_event_ids
wake_package
status                  # queued/dispatching/running/reconciling/completed/failed/superseded
task_id
task_retry_count_snapshot
task_started_at_snapshot
worker_id_snapshot
receipt_token_hash
lease_owner / lease_expires_at
checkpoint
checkpoint_status
progress_signature_before / after
attempts / last_error
queued_at / started_at / completed_at
```

约束：

- `(run_id, generation)` 唯一。
- `correlation_id` 唯一。
- 同一 Run 只能存在一个非终态 Developer Turn，由应用层 CAS 和恢复扫描共同保证。

### 8.4 DeliveryEvent

```text
id
run_id nullable
monitored_repo_id
source
source_event_id          # GitHub: X-GitHub-Delivery
event_type / action
repository_id / pr_number
subject_kind / subject_key
normalized_payload
raw_payload_hash
status                  # pending/processing/processed/dead_letter
lease_owner / lease_expires_at
attempts / next_attempt_at / last_error
received_at / processed_at
```

约束：

- `(source, source_event_id)` 唯一。
- Webhook raw body 限长；只持久化重放所需的标准字段、受限 payload 和 hash。

### 8.5 DeliveryAction

```text
id
run_id
action_type
logical_target_id
desired_version
idempotency_key
expected_subject_kind / expected_subject_key
payload
status                  # pending/leased/succeeded/failed/unknown/dead_letter/cancelled
lease_owner / lease_expires_at
attempts / next_attempt_at
remote_id / remote_url / result
last_error
created_at / completed_at
```

幂等键必须是：

```text
run_id + action_type + logical_target_id + desired_version
```

仅用 `run_id + action_type + head_sha` 无法区分多个 Finding 评论或多次合法 rerun。

### 8.6 DeliveryReviewRun

可新增独立表，或安全扩展 `PRReview`。实施前的默认选择是扩展 `PRReview`，减少双套 Reviewer UI，但必须新增：

```text
delivery_run_id
subject_kind / subject_key
head_sha
reviewer_role
review_generation
structured_result
completion_marker
parse_error
```

不得沿用“没有 Review 就 approved”的旧逻辑。

### 8.7 DeliveryFinding

```text
id
run_id
review_run_id
reviewer_role
origin_subject_key
fingerprint
severity / blocking
file / line
title / description / evidence
status                  # open/fix_proposed/disputed/resolved/waived/superseded
resolution / resolution_evidence
proposed_by_turn_id
verified_by_review_run_id
waived_by / waived_reason
created_at / updated_at / resolved_at
```

### 8.8 DeliveryDecision

```text
id
run_id
question_id
question / options / recommended
blocking
status                  # open/answered/expired/cancelled
answer
asked_by_turn_id
answered_by
created_at / answered_at
```

回答不可原地修改；改变决定时创建新 version，并保留前一条记录。

### 8.9 DeliveryTimeline / Notification / Deployment

- `DeliveryTransition`：保存 state_version、前后状态、cause、actor、snapshot。
- `DeliveryNotification`：持久化 attention 和外部通知 outbox。
- `DeliveryDeployment`：environment、revision/artifact、external deployment ID、health 和 rollback generation。

### 8.10 现有模型修改

- `Task` 增加 provider-neutral 的 `delivery_run_id`、`delivery_role` 路由字段；不能只放 `metadata_`。
- 为了 Worker 数据库不必镜像 Manager 的完整 Run，Task 上的 `delivery_run_id` 不作为跨节点 FK；Manager 用 DeliveryRun/Turn 关系做权威校验。
- `Worktree` 增加 `task_id`、`delivery_run_id`、`last_verified_head` 和安全清理状态。
- `MonitoredRepo` 增加 GitHub numeric repository ID/node ID、soft-delete 状态和 policy 关系。
- `PRReview` 按 8.6 扩展。

### 8.11 迁移规则

- 实施时先运行 `uv run alembic heads`；本文审查时唯一 head 为 `a4c8e2f19d77`。
- 新模型必须导入 `alembic/env.py` 和测试 metadata fixture。
- 不使用数据库专属 Enum、partial index 或依赖 JSON 查询完成核心正确性。
- 新列先 nullable，回填后再收紧。
- 当前 SQLite 连接未开启 `PRAGMA foreign_keys=ON`；启用前先审计孤儿数据，并保留应用层 CAS/清理。
- `MonitoredRepo` 不再硬删除关联历史；先 soft-delete/disable。
- Task 删除时，活跃 Run 一律拒绝；终态证据保留并将可选 Task 引用设 NULL。
- Migration 测试覆盖 fresh upgrade、legacy upgrade、重复 upgrade、downgrade→upgrade 和 single-head。

---

## 9. 建议的代码模块

### 9.1 新增后端文件

```text
backend/models/delivery.py
backend/schemas/delivery.py
backend/api/delivery_runs.py
backend/services/delivery_reducer.py
backend/services/delivery_controller.py
backend/services/delivery_reconciler.py
backend/services/delivery_gate.py
backend/services/delivery_outbox.py
backend/services/delivery_task_bridge.py
backend/services/delivery_workspace.py
backend/services/delivery_prompts.py
backend/services/delivery_notifications.py
backend/services/delivery_deployment.py
backend/services/github_delivery_client.py
```

### 9.2 需要修改的后端文件

```text
backend/main.py
backend/config.py
backend/database.py
backend/models/task.py
backend/models/worktree.py
backend/models/pr_monitor.py
backend/models/global_settings.py
backend/schemas/task.py
backend/schemas/pr_monitor.py
backend/schemas/global_settings.py
backend/api/tasks.py
backend/api/chat.py
backend/api/pr_monitor.py
backend/api/project_todos.py
backend/api/settings.py
backend/api/ws.py
backend/services/dispatcher.py
backend/services/instance_manager.py
backend/services/task_queue.py
backend/services/task_termination.py
backend/services/worktree_manager.py
backend/services/pr_review_service.py
backend/services/worker_proxy.py
backend/services/worker_relay.py
backend/services/task_migrator.py
backend/services/ws_broadcaster.py
alembic/env.py
```

### 9.3 前端文件

```text
frontend/src/api/client.ts
frontend/src/api/ws.ts
frontend/src/App.tsx
frontend/src/pages/DeliveryRunsPage.tsx
frontend/src/pages/PRMonitorPage.tsx
frontend/src/pages/TasksPage.tsx
frontend/src/components/Delivery/DeliveryRunView.tsx
frontend/src/components/Delivery/DeliveryTimeline.tsx
frontend/src/components/Delivery/GatePanel.tsx
frontend/src/components/Delivery/FindingPanel.tsx
frontend/src/components/Delivery/HumanDecisionCard.tsx
frontend/src/components/Delivery/DeploymentPanel.tsx
frontend/src/components/Delivery/DeliveryNotifications.tsx
frontend/src/components/Chat/ChatView.tsx
frontend/src/components/Tasks/TaskBadges.tsx
frontend/src/components/Projects/ProjectTodoList.tsx
```

若增加主导航，还必须同步：

```text
frontend/src/components/Layout/AppShell.tsx
frontend/src/config/iconSets.tsx
frontend/src/components/icons.tsx
对应 icon/theme/AppShell 测试
```

---

## 10. API、事件和 checkpoint 合约

### 10.1 Run API

```http
POST   /api/delivery-runs
GET    /api/delivery-runs
GET    /api/delivery-runs/{run_id}
POST   /api/delivery-runs/{run_id}/pause
POST   /api/delivery-runs/{run_id}/resume
POST   /api/delivery-runs/{run_id}/cancel
POST   /api/delivery-runs/{run_id}/take-over
POST   /api/delivery-runs/{run_id}/reconcile
POST   /api/delivery-runs/{run_id}/retry-action
POST   /api/delivery-decisions/{decision_id}/resolve
GET    /api/delivery/attention
GET    /api/delivery/system/status
```

所有 command 请求携带：

```json
{
  "expected_version": 17,
  "reason": "operator supplied reason",
  "idempotency_key": "client-generated-key"
}
```

涉及 merge/deploy/rollback 时还必须携带 expected subject 或 environment generation。旧 version 返回结构化 409：

```json
{
  "code": "delivery_state_stale",
  "detail": "Run changed from version 17 to 18",
  "current_version": 18
}
```

### 10.2 创建 Run

TaskForm 和 ProjectTodo 不应先创建一个可被 Dispatcher 抢走的普通 pending Task，再补 Run。

推荐请求：

```json
{
  "project_id": 5,
  "title": "Support checkpoint voting",
  "requirements": "...",
  "provider": "codex",
  "model": "gpt-5.6-sol",
  "effort_level": "high",
  "policy_id": 3,
  "source_todo_id": 9
}
```

创建顺序：

1. API 事务创建 `DeliveryRun(preparing)` 和 `prepare_workspace` Action。
2. Outbox 幂等创建固定 branch/worktree。
3. 同一事务创建 Developer Task、Run 关联和首个 queued Turn。
4. commit 后唤醒 durable bridge。
5. API 可先返回 `202 preparing`；前端通过 snapshot/WS 跟进。

### 10.3 Webhook

继续使用唯一入口：

```http
POST /api/github/webhook
```

处理顺序：

1. 限制 body 大小。
2. 按候选 repo secret 验证 HMAC。
3. 校验 `X-GitHub-Event` 与 `X-GitHub-Delivery`。
4. 规范化并 insert Event；唯一冲突返回幂等 accepted。
5. commit。
6. 发轻量 wake hint。
7. 快速返回；绝不在 webhook 请求内启动/停止 Agent 或调用 GitHub。

Delivery 管理的 PR 走新 inbox；未被 Delivery 管理的 repo/PR 保留 legacy PR Monitor 路径。两者不能为同一 PR 同时创建 Reviewer Task。

### 10.4 WebSocket

建议 channel：

```text
delivery-run:{run_id}
user:{user_id}
delivery-runs           # admin only
```

事件 envelope：

```json
{
  "event": "delivery.run_changed",
  "event_id": "uuid",
  "run_id": 123,
  "sequence": 42,
  "state_version": 18,
  "occurred_at": "2026-08-02T12:00:00Z",
  "data": {
    "reason": "ci_failed"
  }
}
```

规则：

- WS 只提示客户端重新 GET snapshot。
- 前端忽略低于当前 version/sequence 的旧消息。
- 重连、订阅确认和页面重新可见时必须 GET。
- `backend/api/ws.py` 对 Run channel 复用 Task/Project/Worker owner ACL。
- 当前 `pr-monitor` 的 `type` 字段逐步统一为 `event`，兼容期同时读两者。

### 10.5 Agent checkpoint

Agent 最终回复可以附带：

```text
CCM_DELIVERY_CHECKPOINT_BEGIN
{
  "version": 1,
  "turn_id": 456,
  "correlation_id": "...",
  "result": "work_complete",
  "summary": "Fixed timeout handling",
  "claimed_commits": ["def456"],
  "claimed_branch": "ccm/delivery/123",
  "local_checks": [
    {"command": "pytest tests/test_timeout.py", "outcome": "passed"}
  ],
  "decision_request": null
}
CCM_DELIVERY_CHECKPOINT_END
```

安全规则：

- 最大 32 KiB，Pydantic 严格 schema，未知字段丢弃或拒绝。
- `turn_id + correlation_id` 必须匹配 active Turn。
- 不接受 `gate_passed`、`merge_allowed`、`finding_resolved` 等越权字段。
- claimed commit/branch/push 必须由 Git/GitHub 对账。
- 缺失、截断或解析失败不会直接失败 Run；记录 `unreported/invalid` 后 reconcile。
- Claude MCP 后续可作为同一服务的便利入口，但不能改变 Provider-neutral contract。

### 10.6 Worker internal protocol

```http
POST /api/internal/delivery-turns/{turn_id}/accept
GET  /api/internal/delivery-turns/{turn_id}/receipt
POST /api/internal/delivery-workspaces/prepare
```

请求必须包含：

- protocol version。
- Manager ID、Run ID、Turn ID。
- receipt token。
- expected Task/Worker generation。
- prompt policy hash。
- workspace identity。

Worker 对同一 Turn ID 重复 accept 必须返回同一个 receipt，不得 enqueue 第二次。

---

## 11. 与 Task、Dispatcher、Chat 的具体连接

### 11.1 Durable Wake Bridge

不能把 `dispatcher.enqueue_message()` 当作持久调度源。正确流程：

```text
Controller 写 DeliveryTurn(status=queued)
→ bridge CAS queued→dispatching，生成 receipt
→ local enqueue 或 Worker accept
→ queue consumer CAS dispatching→running
→ Dispatcher 启动 exact Task turn
→ Task terminal 后写 terminal hint
→ Controller reconciliation
```

崩溃恢复：

- 崩溃在 DB commit 前：没有 Turn，不会执行。
- 崩溃在 commit 后、enqueue 前：startup bridge 重新领取。
- enqueue 后 bridge 超时：重复 queue object 在 `dispatching→running` CAS 时被丢弃。
- Turn 已 running 后进程崩溃：先对账 Task/Instance/Worker/GitHub，再决定恢复。

### 11.2 Dispatcher 改动边界

- `QueuedMessage` 增加可选 `delivery_turn_id/correlation_id`。
- 增加 `enqueue_delivery_turn()`，按 Turn ID 做进程内去重和 DB admission。
- Task launch 前回写 Turn running；Task terminal commit 后发送 durable hint。
- 不在 Dispatcher 内实现 GitHub Gate、Run reducer 或 Outbox。
- 保持现有 Task→Instance 锁序；Delivery 数据写入分事务，不能形成 Run→Task→Instance 与旧路径反向锁。

### 11.3 用户聊天

Developer Task 仍允许用户发消息，但行为改为：

- Run 正在 running：普通 chat 记录为 intervention，默认并入当前 Turn 或排在其后；`inject` 只有在现有 provider/transport 支持时才 steer，并记录 audit。
- Run waiting/ready：消息先持久化为 `human_message` event，由 Controller 聚合成一个新 Turn。
- Run paused：保存消息和 attention，不自动 wake，直到 resume。
- Run terminal：允许普通续聊，但这次聊天不自动改变历史 Run；需要修复时创建 child Run。

这样可以避免用户 chat 绕过 DeliveryTurn lease，直接触发第二个并发 resume。

### 11.4 Task 操作语义

| 操作 | 活跃 Run 的规则 |
|---|---|
| stop current turn | 复用 `task_termination`，Turn terminal，Run paused(user_stopped) |
| pause run | 关闭新 admission/action；可选 exact stop 当前 Turn |
| cancel run | 终止 exact Task generation，取消未领取 Action，Run terminal/cancelled |
| retry Task | 禁止直接 Task retry，改为 Run command |
| delete Task | 活跃 Run 返回 409；终态默认 archive，强删要 admin + audit |
| edit repo/project/base/mode | 活跃 Run 禁止 |
| edit model/effort | 只允许 paused/waiting，作用于下一 Turn并写 policy change |
| migrate Worker | MVP 禁止；实现 Worker 协议后只允许 paused 且无 active Turn/Action |
| clone session | 不用于切换到新的 Delivery worktree |

### 11.5 Session 与上下文压缩

- DeliveryRun 不保存 Provider session_id，永远读取当前 Task。
- Claude/Codex 账号轮换和 context compact 继续由 Dispatcher/InstanceManager 管理。
- Session 变化不改变 DeliveryTurn generation 或 GitHub subject。
- 创建受控 worktree 后不得把已有 session 直接 resume 到新 cwd；采用新 Developer Task 或新 session + requirements/plan snapshot。

---

## 12. Workspace、Git 与 PR 所有权

### 12.1 DeliveryWorkspaceService

复用并扩展 `WorktreeManager`：

- branch 固定为 `ccm/delivery/{run_id}` 或经过长度限制的等价名称。
- worktree 固定到 `.claude-manager/worktrees/delivery-{run_id}`。
- create/ensure/recover 幂等。
- 本地 DB 持久化 repo、worktree、branch、base、Run、Task。
- 多个 Turn 使用同一个 cwd。
- 不调用现有 `merge_to_main()`。
- 不使用无条件 `remove --force` 清理可能存在的用户改动。

### 12.2 Workspace 恢复

Controller 启动或 Turn 前验证：

- repo/worktree 仍属于预期 Project。
- worktree `.git` 指针有效。
- 当前 branch 是受控 branch。
- remote URL 的 repository ID 与 Run 相符。
- branch remote head 没有未经识别的变化。
- 没有 active rebase/merge/cherry-pick。

不确定时 pause，不让 Agent“自行猜测恢复”。

### 12.3 现有项目指令冲突

当前项目模板要求 Agent 最后 merge/push main 并删除 worktree。Delivery Task 每一轮都必须注入受信控制块，明确覆盖这些步骤。

但 Prompt 不是安全边界，还必须：

- 要求默认分支 branch protection。
- Developer 使用与 Merge Controller 分离的身份。
- auto-merge readiness 检查凭据能力和保护规则。
- 发现默认分支被 Developer 身份修改时立即 pause 并告警。

### 12.4 PR 创建和绑定

- Agent 只 push branch；Controller 在 reconcile 看到 remote branch 后创建 PR Action。
- PR marker 只能辅助定位，实际绑定必须匹配 repository ID、branch、PR number 和 current head。
- PR create 响应丢失时，Outbox 先按 head branch 查询是否已存在 PR。
- PR 被 rename/transfer 时按 repository numeric ID 跟踪，并更新 display name snapshot。

---

## 13. PR Monitor 与 Reviewer Panel

### 13.1 同一 Webhook，两个模式

- Legacy PR：保留当前 PR Monitor 行为，直到单独迁移。
- Delivery PR：Webhook 只写 DeliveryEvent，由 Controller 创建 exact-subject Reviewer Run。
- Repo 配置必须拒绝“Legacy auto_merge + Delivery enabled”组合。
- 当前 self-PR ignore 逻辑只留在 Legacy；Delivery 使用独立 reviewer identity 后不应跳过。

### 13.2 Reviewer 权限

Reviewer Agent：

- 只读 repo/PR/CI。
- 输出结构化 findings。
- 不修改代码。
- 不 push。
- 不 merge。
- 不直接把自己的 Task 结果解释为 Gate passed。

Controller 通过 Outbox 发布 Review 内容。

### 13.3 Reviewer 角色

建议角色：

```text
principal_engineer  # 架构、并发、跨模块不变量
senior_engineer     # 实现正确性、维护性、边界条件
qa_engineer         # 测试、回归、用户路径、部署验证
```

MVP Reviewer 阶段可以先启用一个 `general` role，验证协议后再扩到三角色。角色、provider、model、effort 都来自 Run policy snapshot。

### 13.4 Finding 生命周期

```text
open
→ Developer 提交修复 → fix_proposed
→ 新 subject Reviewer 验证 → resolved 或 open

open
→ Developer 提交反驳 → disputed
→ 原 Reviewer/裁决 Reviewer → resolved 或 open

open
→ 有权限人类接受风险 → waived（必须 exact subject + reason + audit）
```

新 head 会 supersede 旧 ReviewRun，但 Finding 是否 carry-forward 必须由新 Reviewer 明确确认；第一版默认全部重审。

---

## 14. Worker、Migration 与 Shared Task

### 14.1 权威位置

- Delivery Controller、Event Inbox、Outbox 和 Run DB 只存在 Manager。
- Worker 只执行 workspace 操作和 Agent Turn。
- Worker 的 Task 是执行镜像，不能独立推进 Run。
- GitHub Gateway 默认在 Manager；若凭据只在 Worker，先实现 owner-aware gateway，否则 fail closed。

### 14.2 Capability handshake

Worker 必须声明：

```json
{
  "delivery_protocol_version": 1,
  "delivery_turn_receipts": true,
  "delivery_workspace": true,
  "delivery_checkpoint": true
}
```

旧 Worker 或 capability 不完整时拒绝创建/迁移 Run。

### 14.3 Worker receipt

- Manager 为 Turn 生成不可明文落日志的 receipt token。
- Worker 对 Turn ID + token hash 做唯一 admission。
- HTTP timeout 表示 outcome unknown，不直接重发普通 `/chat`。
- Manager 先查 receipt，再决定是否 retry accept。
- Worker terminal event 携带 Turn ID、Task exact generation 和 protocol version。
- WorkerRelay commit 权威 Task snapshot 后才提示 Delivery Controller reconcile。

### 14.4 Task migration

迁移前必须：

- Run paused。
- 无 active Turn。
- 无 leased merge/deploy Action。
- source workspace 已证明 clean 或全部内容成功复制。
- 目标 Worker capability 兼容。

迁移后：

- 重建/验证 worktree，不搬运失效 `.git` pointer。
- 复用 TaskMigrator 的 session 和项目复制，但加 Delivery workspace receipt。
- 更新 Run 的执行位置 snapshot 和 Task routing fields。
- 任何不确定结果保持 paused，禁止双端同时执行。

### 14.5 Shared Task

- Owner CCM 是唯一 Controller。
- Shadow CCM 只显示 owner snapshot，不创建第二个 Run/Turn/Action。
- Team share 的 `chat` 权限不推导为 waive/merge/deploy/rollback 权限。
- V1 对 shared shadow 创建 DeliveryRun 返回明确 409。

---

## 15. Human Gate、权限、凭据与通知

### 15.1 持久 Human Decision

不复用 `AskUserRegistry` 作为 Delivery 真相，因为它是进程内 Future。

- Agent checkpoint 可以请求一个持久 Decision。
- Controller 保存问题、选项、推荐项、阻塞性和 source Turn。
- Run 进入 `waiting(human_decision)`，Agent 进程已经退出。
- 回答使用 API + state_version CAS。
- 回答后生成新 Turn，所有 Agent/Reviewer 都收到 Decision Record。
- Claude 的实时 AskUser 卡片可以继续用于短交互，但不能代替 DeliveryDecision。

### 15.2 动作级权限

`require_task_access` 不能直接授权高风险操作。Delivery 至少区分：

```text
view
chat/comment
operate
resolve_decision
waive_finding
merge
deploy
rollback
policy_admin
```

UI 只根据服务端返回的 `allowed_actions` 渲染按钮。所有 override/waive/merge/deploy/rollback 保存 actor、reason、old/new value、state_version 和 exact subject。

### 15.3 凭据角色

```text
Developer credential：只需要 clone/fetch/push delivery branch
Reviewer credential：只读 private repo/PR
Merge credential：Controller 专用，允许 merge queue/merge
Deployment credential：Adapter 专用
```

- 不把 Merge/Deployment credential 注入 Agent prompt、env 或 worktree。
- Project 当前单套 Git credential 只能作为 Developer MVP 兼容路径。
- 无法证明身份隔离或 branch protection 时，auto_merge readiness 必须失败。
- Deployment config 只保存 Secret ID；API/WS/log 不返回 secret content。

### 15.4 Prompt injection

以下均是不可信数据：Issue、PR title/body、diff、commit message、review comment、CI log、代码注释和 checkpoint。

不可信内容不能：

- 修改 policy 或 Gate。
- 选择凭据。
- 请求 merge/deploy/rollback。
- 改变 allowed repository/branch/path。
- 将 Finding 标为 resolved。

### 15.5 通知

- 高优先级：Human Decision、安全 Finding、merge/deploy/rollback failure、permission missing。
- 聚合通知：CI 已自动重试、同一轮多个 failure。
- 普通 phase 变化只写 timeline。
- Web UI attention 持久化，不能只依赖 toast/WS。
- Feishu 可复用发送层，但必须经 Notification Outbox。
- 现有邮件模块含不安全的静态凭据配置，完成凭据轮换和安全配置前不得接入 Delivery 通知。

---

## 16. 崩溃恢复、并发与无限循环防护

### 16.1 核心不变量

1. 一个 Run 同时最多一个 active Developer Turn。
2. Event、Turn、Action 必须先持久化再唤醒。
3. 进程内 lock/queue 只作优化，数据库 CAS/lease 才是跨进程正确性来源。
4. 每个证据绑定 exact VerificationSubject。
5. 旧 subject 事件只能标 stale，不能推进新 Gate。
6. WebSocket 丢失不影响状态恢复。
7. 不在数据库锁内执行网络、模型、Git 或广播操作。
8. cancel/supersede 复用 exact-generation Task termination。
9. 所有外部 action 在执行前再次验证 expected subject 和 state_version。
10. `Task.status=completed` 只表示某个 Task turn 结束；完整交付完成只看 `DeliveryRun.outcome=success`。

### 16.2 Controller lease

- 每个 claim 使用随机 owner token、lease expiry 和 `state_version` CAS。
- lease 续约失败立即停止产生新 effect。
- lease 过期后，新 owner 先外部 reconcile，再执行任何 action。
- 多 Uvicorn worker / 多 Manager 不能依赖 `backend.main` 单例保证唯一性。

### 16.3 Lost wakeup

```text
先注册 Event/Turn/Wait Set
→ commit
→ 检查过去已经到达的 Event
→ 主动 reconcile 外部当前状态
→ 条件仍未满足才 waiting
```

### 16.4 Agent 崩溃

Agent 可能在 checkpoint 前已经 commit/push。恢复顺序：

1. 读取 exact Task/Instance/Worker generation。
2. 检查 workspace 状态。
3. 查询 remote branch、PR、CI、Review。
4. 对比 Turn wake package 和上一个 snapshot。
5. 只有仍存在可执行工作时才创建新 Turn。

### 16.5 无进展检测

Progress Signature：

```text
current verification subject
+ failing check fingerprints
+ open finding fingerprints
+ decision version
+ merge queue state
+ deployment state
```

建议默认：

- 第一次相同 signature：允许一次重新诊断。
- 第二次：创建独立 diagnostic Turn/Reviewer。
- 第三次：Run paused(no_progress)，请求人类。
- SHA、Gate、Finding 或 Decision 有实质变化后清零。
- 同时设置 max_turns、CI rerun budget、Action retry budget 和总 wall-clock budget。

### 16.6 Action outcome unknown

对于“远端成功、本地确认前崩溃/超时”：

```text
Action → unknown
→ 读取远端当前状态
→ 已达到 desired state：补记 succeeded
→ 未达到且仍满足 expected subject：重试
→ subject 已变：cancelled/stale
→ 无法判断：paused + attention，不猜测
```

---

## 17. Deployment Adapter 与 post-merge 修复

### 17.1 不复用 UpdateService

`backend/services/update_service.py` 是 CCM 自身的代码、依赖、数据库迁移和 systemd 重启协议，其自动数据库回退也不是三种数据库的通用能力。Delivery 只借鉴 lease、exact commit、恢复和 UI 表达，不直接调用它。

### 17.2 Adapter 接口

```text
resolve_current(environment)
deploy(exact_revision_or_artifact, idempotency_key)
observe(deployment_id)
verify(deployment_id, policy)
rollback(expected_environment_generation, target_deployment_id)
```

### 17.3 分阶段能力

1. `merge_only`：合并即 Run success。
2. `observe`：只观察外部部署，不触发。
3. `deploy`：Controller 触发 exact revision。
4. `health`：部署成功后执行健康验证。
5. `rollback`：最后开放，默认要求人工二次确认。

### 17.4 数据库迁移风险

- 发现 schema migration 时默认 Human Gate。
- 无法证明 backward compatible 时优先 forward-fix，不自动恢复旧数据库。
- rollback UI 显示当前环境 generation、目标 revision 和数据丢失风险。
- 环境已被其他 Run 更新后，旧 Run 不能回滚该环境。

### 17.5 Remediation child Run

部署应用错误时：

- 原 Run 保留 merge/deploy evidence。
- 创建 `parent_run_id` 指向原 Run 的 child Run。
- child 从当前默认分支建立新 branch/PR。
- 原 Run 等待 child/incident resolution，不能复活旧 PR。

---

## 18. 分阶段 Todo Backlog

### Phase 0 — 冻结安全边界与修现有阻断项

#### [ ] DL-000 — 建立 feature flags、policy level 与实现证据纪律

- 优先级：P0
- 依赖：无
- 改动：在 `backend/config.py`、`GlobalSettings`、runtime settings API 中增加 observe/wake/review/merge/deploy/rollback 独立 kill switch；默认全关。
- 文件：`backend/config.py`、`backend/models/global_settings.py`、`backend/schemas/global_settings.py`、`backend/api/settings.py`、Prefs/Delivery policy UI。
- 测试：env/runtime 优先级、重启持久化、各开关互不联动、默认值全部为 off、关闭期间不产生副作用。
- 验收：关闭 action 时仍可持久化事件和读取历史，但绝不创建 Turn/Review/merge/deploy Action。
- 回退：关闭全局开关即可，不删除数据。
- 证据：待填写。

#### [ ] DL-010 — 修复 PR Monitor ACL、secret 和 legacy auto-merge 安全问题

- 优先级：P0
- 依赖：无
- 改动：新增按目标 repo 校验的 `require_pr_repo_access`；detail/review GET、update/delete/toggle/rotate 全覆盖；secret 默认遮罩且只在创建/轮换时一次性展示。
- 改动：Delivery-enabled repo 禁止 legacy Agent auto-merge；Review 未知/无结果不能 approved。
- 文件：`backend/api/pr_monitor.py`、`backend/schemas/pr_monitor.py`、`backend/services/pr_review_service.py`、`frontend/src/pages/PRMonitorPage.tsx`。
- 测试：跨用户 IDOR、Worker owner、secret 不出现在 list/detail/WS/log、unknown review fail closed。
- 验收：Reviewer Agent 无法在 Delivery 路径 merge；用户 A 无法按 ID 读取/修改用户 B repo。
- 回退：保留 legacy review，但强制人工 merge。
- 证据：待填写。

#### [ ] DL-020 — 实现正式 reducer 与状态不变量

- 优先级：P0
- 依赖：DL-000
- 改动：纯函数 reducer、合法 tuple、Command/Event/Guard/Effect 类型、state_version CAS contract。
- 文件：`backend/services/delivery_reducer.py`、`backend/schemas/delivery.py`、`backend/tests/test_delivery_reducer.py`。
- 测试：非法 tuple、stale version、pause/cancel/terminal、head change、merge/deploy transition property table。
- 验收：除 repository/service 层外没有代码直接组合 phase/activity/outcome。
- 回退：feature flag off，模型表保留。
- 证据：待填写。

### Phase 1 — 数据骨架、Inbox、Reconciler 与 Shadow Mode

#### [ ] DL-030 — 新增 Delivery 数据模型与单一 Alembic migration

- 优先级：P1
- 依赖：DL-020
- 改动：实现 Run/Turn/Event/Action/Review/Finding/Decision/Transition/Notification 基础表，扩展 Task/Worktree/MonitoredRepo/PRReview。
- 文件：`backend/models/delivery.py`、现有 models、`alembic/env.py`、`alembic/versions/<revision>.py`、migration tests。
- 测试：fresh、legacy、repeat、downgrade-upgrade、single head；SQLite/PostgreSQL/MySQL 类型 smoke。
- 验收：重复 Event/Turn/Action 被数据库唯一约束拒绝；无第二 Alembic head。
- 回退：仅 schema，无运行副作用；downgrade 不删除无法安全恢复的历史数据前需备份。
- 证据：待填写。

#### [ ] DL-040 — Run read API、ACL 与 stable errors

- 优先级：P1
- 依赖：DL-030
- 改动：list/detail、allowed_actions、结构化 error code、state_version；基于 creator/Project/Worker/share 的 read ACL 和动作级 write ACL。
- 文件：`backend/api/delivery_runs.py`、`backend/schemas/delivery.py`、`backend/api/deps.py`、`backend/main.py`。
- 测试：owner/admin/member/share 的 view/chat/operate/merge/deploy 矩阵。
- 验收：知道 run_id 不能越权读取；Team chat 权限不会出现 merge/rollback action。
- 回退：router/flag off。
- 证据：待填写。

#### [ ] DL-050 — Controller lease、startup recovery 与 shutdown 顺序

- 优先级：P1
- 依赖：DL-020、DL-030
- 改动：数据库 lease/CAS、到期扫描、wakeup event、reducer effect loop。
- 文件：`backend/services/delivery_controller.py`、`backend/main.py`、`backend/tests/test_delivery_controller.py`。
- 启动：migration → Dispatcher ready → Controller/Reconciler/Outbox recovery。
- 关机：先关 Delivery admission/claim → 等 effect settle → 再关 Dispatcher/transport。
- 测试：双 Controller claim、lease 过期接管、持有者崩溃、启动半恢复、shutdown 与新 claim 竞态、SQLite CAS 和 PostgreSQL/MySQL smoke。
- 验收：两个 Controller 竞争只有一个 claim；lease 丢失不再产生 effect。
- 回退：kill switch + stop controller，保留 Run。
- 证据：待填写。

#### [ ] DL-055 — Action Outbox 基础 claim、receipt 与 unknown 状态

- 优先级：P1
- 依赖：DL-030、DL-050
- 改动：实现通用 Action lease、idempotency、retry budget、unknown outcome、远端对账接口和 dead-letter；此阶段先承载 workspace prepare 与 PR create。
- 文件：`backend/services/delivery_outbox.py`、`backend/services/delivery_controller.py`、`backend/main.py`、`backend/tests/test_delivery_outbox.py`。
- 测试：commit/claim/调用/确认各边界崩溃、lease 抢占、重复 idempotency key、unknown query-before-retry。
- 验收：任何外部 effect intent 都先有持久 Action；相同 logical target/desired version 不会并发执行两次。
- 回退：关闭对应 action switch，pending/unknown Action 原样保留。
- 证据：待填写。

#### [ ] DL-060 — Webhook Event Inbox、验签、去重与 dead-letter

- 优先级：P1
- 依赖：DL-010、DL-030
- 改动：现有 `/api/github/webhook` 先持久化；支持 delivery ID、payload size、规范化、重试/dead-letter。
- 文件：`backend/api/pr_monitor.py`、`backend/services/delivery_controller.py`、`backend/tests/test_delivery_webhook.py`。
- 测试：重复 ID 同/不同 payload、乱序、旧 SHA、无 header、错误签名、大 body、DB 冲突。
- 验收：Webhook 不做网络/Agent 操作并快速返回；重复 delivery 不重复 effect。
- 回退：delivery repo policy off，legacy 路径保留。
- 证据：待填写。

#### [ ] DL-070 — GitHub Gateway 与权威 Snapshot contract

- 优先级：P1
- 依赖：DL-030
- 改动：封装 repo/PR/branch/check/status/review/mergeability/merge-group/deployment 查询；超时、限流、auth、输出限长和 redaction。
- 文件：`backend/services/github_delivery_client.py`、fake adapter、fixtures/tests。
- 安全：使用 `create_subprocess_exec` 或正式 API client，不拼 shell；repo identity 用 numeric ID。
- 测试：分页、限流、超时、401/403/404、截断输出、repo rename、同名 check 不同 App、mergeability unknown 和 fixture replay。
- 验收：同一 snapshot 可重放；GitHub unavailable 返回 retryable state，不产生 Agent Turn。
- 回退：shadow 状态显示 gateway unavailable。
- 证据：待填写。

#### [ ] DL-080 — Reconciler、VerificationSubject 与 Wait Set

- 优先级：P1
- 依赖：DL-050、DL-060、DL-070
- 改动：事件触发 + 定时补漏；重建 current subject、PR/CI/review/merge/deploy snapshot；持久 next reconcile/backoff。
- 文件：`backend/services/delivery_reconciler.py`、`backend/tests/test_delivery_reconciler.py`。
- 测试：漏 webhook、Event 早于 wait、A→B→A、repo rename、draft/reopen/base change、GitHub rate limit。
- 验收：数据库清空内存状态并重启后可仅靠 DB+GitHub恢复正确 wait set。
- 回退：停止 reconcile worker，不改变外部状态。
- 证据：待填写。

#### [ ] DL-090 — 纯 Gate Evaluator

- 优先级：P1
- 依赖：DL-020、DL-080
- 改动：PR-head/AI-review/merge-group/deployment gate；返回 blockers 和 actionable_by。
- 文件：`backend/services/delivery_gate.py`、`backend/tests/test_delivery_gate.py`。
- 测试：missing/skipped/neutral、同名不同 App、unknown mergeability、stale subject、pending Turn/Decision。
- 验收：相同 snapshot+policy 得到完全相同结果；Evaluator 无 DB/网络副作用。
- 回退：shadow 仍展示 snapshot，不执行 gate effect。
- 证据：待填写。

#### [ ] DL-100 — Shadow Mode API/UI 与 Gate 对照

- 优先级：P1
- 依赖：DL-040、DL-080、DL-090
- 改动：只读 Run 列表/detail、Timeline、Gate blockers；预测“若开启会执行什么”。
- 文件：`frontend/src/api/client.ts`、`frontend/src/pages/DeliveryRunsPage.tsx`、Delivery components、PR Monitor links。
- 测试：snapshot 渲染、stale WS、409 refresh、旧 head superseded。
- 验收：Shadow 模式 0 Agent wake、0 GitHub write、0 merge/deploy；与 GitHub UI 人工抽样对照。
- 回退：隐藏路由，后端数据保留。
- 证据：待填写。

### Phase 2 — 本地 Developer Turn 与 CI 修复 MVP

#### [ ] DL-110 — DeliveryWorkspaceService 与固定 cwd

- 优先级：P1
- 依赖：DL-030、DL-070
- 改动：幂等 prepare/ensure/recover/cleanup；扩展 Worktree 绑定；禁止 merge_to_main/force cleanup。
- 文件：`backend/services/delivery_workspace.py`、`worktree_manager.py`、`models/worktree.py`、tests。
- 测试：已有 branch/worktree、崩溃半创建、dirty workspace、invalid git pointer、remote changed。
- 验收：多个 Turn 使用同一绝对 cwd；不能覆盖用户原工作区改动。
- 回退：Run paused，worktree 保留供人工恢复。
- 证据：待填写。

#### [ ] DL-120 — 原子 Run 创建、Project Todo 和 TaskForm 接入

- 优先级：P1
- 依赖：DL-040、DL-050、DL-055、DL-110
- 改动：创建 Run+prepare action；workspace ready 后事务创建 Developer Task+首 Turn；commit 后才 wake。
- 文件：`delivery_runs.py`、`delivery_task_bridge.py`、`TaskForm.tsx`、`ProjectTodoList.tsx`、client/tests。
- 测试：API crash points、Task 不会在 Run 关联前被 Dispatcher claim、Todo provenance、无 remote/Worker/shared fail closed。
- 验收：不存在“pending Task 已执行但 Run 尚未创建”的窗口。
- 回退：取消 preparing Run，保留/安全清理 workspace。
- 证据：待填写。

#### [ ] DL-125 — Controller 创建/恢复 PR 并绑定 exact branch

- 优先级：P1
- 依赖：DL-055、DL-070、DL-110、DL-120
- 改动：reconcile 发现 remote delivery branch 后创建 `create_pr` Action；按 repository ID + head branch 查询已有 PR；保存 PR number、URL、head/base subject。
- 文件：`backend/services/github_delivery_client.py`、`backend/services/delivery_outbox.py`、`backend/services/delivery_reconciler.py`、tests。
- 测试：PR create 响应丢失、Webhook 先于 API 响应、已有 PR、branch head 改变、同名 branch 位于错误 repo。
- 验收：MVP 不依赖 Agent 自己创建 PR；未知结果先对账，不会创建第二个 PR。
- 回退：关 PR action switch并请求人工创建，Controller 仍可安全绑定匹配的现有 PR。
- 证据：待填写。

#### [ ] DL-130 — Delivery Prompt Policy 与角色凭据边界

- 优先级：P0
- 依赖：DL-010、DL-110、DL-120
- 改动：每个初始/续聊 Turn 注入可信控制块；明确 branch/cwd/禁止 main/merge；Issue/CI/Review 放入不可信区。
- 改动：InstanceManager 接受 `credential_role`；MVP 不把 merge/deploy凭据给 Agent。
- 文件：`delivery_prompts.py`、`dispatcher.py`、`instance_manager.py`、Project credential builder、tests。
- 测试：Claude/Codex 首轮与 resume、恶意 Issue/Review 注入、项目文档冲突、credential role 不越权、policy hash 不匹配时 fail closed。
- 验收：Claude/Codex prompt 都包含一致 policy hash；Agent 结果不能修改 Gate；auto-merge 尚保持关闭。
- 回退：停止新 Delivery Turn，不把任务降级为普通 auto Task。
- 证据：待填写。

#### [ ] DL-140 — Durable Turn Bridge 与 idempotent local enqueue

- 优先级：P1
- 依赖：DL-050、DL-120、DL-130
- 改动：Turn queued/dispatching/running receipt；QueuedMessage correlation；startup 重领；重复 queue object CAS 丢弃。
- 文件：`delivery_task_bridge.py`、`dispatcher.py`、`task_queue.py`、tests。
- 测试：commit/enqueue/claim/start 每个边界崩溃；重复 bridge；两个进程竞争；取消并发。
- 验收：任何一次崩溃都不丢 Turn，且同一 Run 不会启动两个 Developer Turn。
- 回退：关闭 wake switch，queued Turn 保留。
- 证据：待填写。

#### [ ] DL-150 — Turn started/terminal 回执与 advisory checkpoint

- 优先级：P1
- 依赖：DL-140
- 改动：Dispatcher launch/complete/fail 后通过 exact Turn ID 写 hint；解析 final marker；Worker-ready contract 先留接口。
- 文件：`dispatcher.py`、`delivery_task_bridge.py`、checkpoint parser、WorkerRelay 后续 hook、tests。
- 测试：无 marker、坏 JSON、错 turn ID、超大 marker、Task 完成后 callback 失败、terminal event 乱序。
- 验收：checkpoint 缺失仍能靠 reconciliation 收敛；旧 Turn 结果不能写入新 Turn。
- 回退：禁用 parser，只保留 terminal scan/reconcile。
- 证据：待填写。

#### [ ] DL-160 — 活跃 Run 的 chat/inject/retry/stop/delete/edit 路由

- 优先级：P1
- 依赖：DL-040、DL-140
- 改动：用户消息持久化为 intervention；Controller 决定注入/排队；Task 高风险操作改走 Run command。
- 文件：`backend/api/chat.py`、`backend/api/tasks.py`、`task_termination.py`、ChatView、tests。
- 测试：用户 chat 与自动 wake 并发、运行中 inject、paused chat、Task delete/migrate/retry 409、stop→pause。
- 验收：用户仍能续聊，但不能绕过 Turn lease 或产生并发 resume。
- 回退：pause Run，保留普通历史读取。
- 证据：待填写。

#### [ ] DL-170 — exact PR-head CI 自动修复循环

- 优先级：P1
- 依赖：DL-080、DL-090、DL-125、DL-140、DL-150
- 改动：聚合当前 subject 的 code failures 为一个 wake package；push 后转新 subject并等待新 CI。
- 文件：Controller/Reconciler/Gate/Prompt/tests。
- 测试：多个 job 同时失败、旧 SHA 迟到、CI 在 wait 注册前完成、Agent push 后崩溃、missing check。
- 验收：等待 CI 时无模型进程；同批失败只消耗一个 Turn；旧绿灯不能证明新 head。
- 回退：关 auto-wake，Run 退到 shadow/manual attention。
- 证据：待填写。

#### [ ] DL-180 — 失败分类、预算、no-progress 与持久 Human Gate

- 优先级：P1
- 依赖：DL-150、DL-170
- 改动：code/infra/flaky/auth/permission/ambiguity 分类；bounded rerun；progress signature；Decision API/attention。
- 文件：Controller、Decision model/API、DeliveryNotifications、tests。
- 测试：相同 signature 三次、Decision 等待重启、回答 CAS、GitHub auth failure 不唤醒 Agent。
- 验收：无限循环可确定性停止；Human Gate 不依赖进程内 Future。
- 回退：pause Run + manual takeover。
- 证据：待填写。

#### Phase 2 MVP DoD

- 本地 Run 从创建到 CI 全绿、等待人工合并完整跑通。
- 服务重启不丢 Event/Turn/Wait Set。
- Agent 等待时不占 Instance、不消耗 token。
- Worker/shared 请求明确拒绝。
- Reviewer、merge、deploy switch 全关。

### Phase 3 — Reviewer Panel 与 Finding Gate

#### [ ] DL-200 — exact-subject Reviewer Run 与结构化结果

- 优先级：P2
- 依赖：DL-030、DL-090、DL-150
- 改动：扩展 PRReview 或新 ReviewRun；role/provider/model；明确 completion marker；unknown fail closed。
- 文件：models/schemas、`pr_review_service.py`、dispatcher completion hook、tests。
- 测试：空输出、坏 JSON、Task error/cancel、旧 subject 迟到、重复 completion、required role 缺失和 Provider 差异。
- 验收：每个 required role 对 exact subject 有一个明确 terminal 结果。
- 回退：review switch off，人工 review。
- 证据：待填写。

#### [ ] DL-210 — PR Monitor 变成 Gate Producer

- 优先级：P2
- 依赖：DL-010、DL-060、DL-200
- 改动：Delivery PR 由 Controller 创建 reviewer tasks；synchronize 异步 supersede；移除 self-PR skip/Agent merge 在 Delivery 路径的影响。
- 文件：`pr_monitor.py`、`pr_review_service.py`、`dispatcher.py`、tests。
- 测试：opened/synchronize/reopen/close 乱序、legacy 与 Delivery 重复触发、supersede 竞态、Webhook 快速响应和 Reviewer terminal 回查。
- 验收：同一 PR 不会同时跑 legacy 与 Delivery Reviewer；Webhook 不做 Task termination。
- 回退：delivery review off。
- 证据：待填写。

#### [ ] DL-220 — Finding、dispute、waiver 与重新验证

- 优先级：P2
- 依赖：DL-200、DL-210
- 改动：fingerprint、fix_proposed/disputed/resolved/waived；Developer wake package；人类动作级权限。
- 文件：Delivery models/API/Gate/Prompt、FindingPanel、tests。
- 测试：Developer 不能 resolve、旧 subject finding、waive stale version、Reviewer parse failure。
- 验收：只有新 Reviewer 证据或授权 waiver 能解除 blocking Finding。
- 回退：人工 review/merge。
- 证据：待填写。

### Phase 4 — Worker 与 Shared 对等

#### [ ] DL-230 — Worker capability/version 与完整字段转发

- 优先级：P2
- 依赖：Phase 2 MVP
- 改动：capability endpoint/handshake；Task delivery fields、system prompt policy、workspace identity、credential role 透传。
- 文件：`worker_proxy.py`、Worker system API、Task schemas、tests。
- 测试：缺 capability、版本过旧/过新、字段丢失、错误 credential role、Worker restart 和协商缓存过期。
- 验收：旧 Worker fail closed；Manager/Worker protocol mismatch 有明确 409/attention。
- 回退：仅允许本地 Run。
- 证据：待填写。

#### [ ] DL-240 — Worker Turn receipt、Relay terminal 与断线恢复

- 优先级：P2
- 依赖：DL-230
- 改动：internal accept/receipt；Worker DB unique admission；Relay 携带 Turn ID 并在权威 Task mirror commit 后 reconcile。
- 文件：`worker_proxy.py`、`worker_relay.py`、Worker task API、dispatcher、tests。
- 测试：响应丢失、重复 accept、Worker reboot、迟到 terminal、Manager reconnect、unknown outcome。
- 验收：同一 Turn 不会在 Worker 执行两次；旧 event 不能覆盖新 generation。
- 回退：pause remote Runs，迁回或人工处理。
- 证据：待填写。

#### [ ] DL-250 — Delivery workspace migration 与 Shared owner-only UI

- 优先级：P2
- 依赖：DL-230、DL-240
- 改动：paused-only migration、worktree rehydrate、TaskMigrator receipt；shared shadow 只读/owner command proxy。
- 文件：`task_migrator.py`、relay/proxy/shared API、frontend/tests。
- 测试：active Turn 拒绝迁移、源/目标断线、同 branch 已存在、rehydrate 失败、双 owner 竞争、shadow 越权命令。
- 验收：迁移不会出现双端执行；shadow CCM 无法创建第二个 Controller。
- 回退：禁止迁移，保留 source workspace。
- 证据：待填写。

### Phase 5 — Action Outbox、PR 与 Merge Queue

#### [ ] DL-300 — Merge Action adapter 与执行前 preflight

- 优先级：P2
- 依赖：DL-055、DL-090、DL-220、DL-125
- 改动：在基础 Outbox 上增加 enqueue-merge/direct-merge adapter；每次执行前重新读取 PR、subject、Gate、Finding、Decision、active Turn 和 kill switch。
- 文件：`delivery_outbox.py`、`github_delivery_client.py`、Controller/Gate、tests。
- 测试：远端成功响应丢失、stale subject、pause/cancel race、pending Turn、Finding 在 claim 后新增。
- 验收：旧 snapshot 或已变化的 Run 永远不能通过已领取 Action merge。
- 回退：关各 action switch，pending 保留。
- 证据：待填写。

#### [ ] DL-310 — Merge Queue subject、事件与重新验证

- 优先级：P2
- 依赖：DL-060、DL-080、DL-300
- 改动：支持 merge-group event/snapshot、临时 subject、队列重建、merge-group required checks；队列失败回到同一未合并 PR 的修复循环。
- 文件：`backend/services/github_delivery_client.py`、`backend/services/delivery_reconciler.py`、`backend/services/delivery_gate.py`、`backend/services/delivery_outbox.py`、tests。
- 测试：merge_group created/destroyed 乱序、队列重建、旧 SHA 迟到、required check 缺失、队列失败后新 Developer Turn。
- 验收：merge-group SHA 不与 PR head 混用；队列重建使旧 merge-group evidence stale。
- 回退：禁用 Merge Queue，保留 manual merge policy。
- 证据：待填写。

#### [ ] DL-320 — Controller-only merge、独立凭据与 repo opt-in

- 优先级：P2
- 依赖：DL-300、DL-310
- 改动：Controller merge identity、readiness probe、branch protection 校验、repo opt-in、merge result/commit reconciliation。
- 文件：`backend/services/delivery_outbox.py`、`backend/services/github_delivery_client.py`、repo policy model/API/UI、readiness/status API、tests。
- 安全前置：branch protection、独立 merge identity、repo opt-in 和全局 merge switch。
- 测试：head/base 改变、queue rebuild、pause/cancel race、人工 merge、unknown response。
- 验收：Reviewer Agent 永远不 merge；旧 Run/subject 永远不能 merge 新 head。
- 回退：立刻关 merge switch，退回 manual merge。
- 证据：待填写。

#### [ ] DL-330 — 合并后的 workspace/branch 安全清理

- 优先级：P2
- 依赖：DL-320
- 改动：确认 merge commit、remote branch、worktree clean、无 active session 后标记可清理；延迟/人工保留选项。
- 文件：`delivery_workspace.py`、Worktree model/API/UI、tests。
- 测试：dirty/untracked worktree、active session、branch 未删除、人工保留、重复 cleanup、清理中崩溃和路径越界。
- 验收：不使用无条件 force 删除用户未提交内容；清理失败不改变 Run success evidence。
- 回退：保留 workspace 并告警。
- 证据：待填写。

### Phase 6 — Deployment 与 Remediation

#### [ ] DL-400 — DeploymentAdapter contract 和 observe-only 模式

- 优先级：P3
- 依赖：DL-055、DL-320
- 改动：adapter registry、environment/revision/artifact identity、status/health snapshot。
- 文件：`delivery_deployment.py`、models/schemas/API/UI、tests。
- 测试：未知 adapter、只读凭据、状态映射、旧 revision、snapshot replay、超时/限流和 secret redaction。
- 验收：observe 模式不触发部署，且能证明外部 deployment 对应 exact merge commit。
- 回退：completion policy 设回 merge_only。
- 证据：待填写。

#### [ ] DL-410 — Deploy/health Action 与环境代次保护

- 优先级：P3
- 依赖：DL-400
- 改动：deploy/verify Outbox、expected environment generation、health policy、timeout/backoff。
- 文件：`backend/services/delivery_deployment.py`、`backend/services/delivery_outbox.py`、Controller/Reconciler/Gate、models/tests。
- 测试：旧 deployment event、环境被其他 Run 更新、部署成功但 health 失败、响应 unknown。
- 验收：只有 exact artifact + health passed 才完成 deployment policy Run。
- 回退：关闭 deploy switch，转 Human Gate。
- 证据：待填写。

#### [ ] DL-420 — Child remediation、Incident 与人工 rollback

- 优先级：P3
- 依赖：DL-410
- 改动：parent/child Run、post-merge failure 路由、rollback 二次确认和 exact environment CAS。
- 文件：Delivery models/reducer/API、`backend/services/delivery_deployment.py`、Decision/Incident UI、tests/runbooks。
- 测试：旧 Run rollback 新环境、schema migration risk、child Run success/failure、rollback response unknown。
- 验收：部署失败不会复活已合并 PR；自动 rollback 仍默认关闭。
- 回退：人工 incident/runbook。
- 证据：待填写。

### Phase 7 — 完整 UI、通知、可观测性与发布

#### [ ] DL-500 — 独立 Delivery 页面和跨模块深链接

- 优先级：P1/P2 随阶段渐进
- 依赖：DL-040、各阶段 snapshot
- 改动：Run list/detail、Gate、Timeline、Turns、Findings、Decisions、Deployment、allowed actions。
- 连接：Task Chat banner、Task badge、PR Monitor row、Project Todo provenance。
- 文件：`frontend/src/api/client.ts`、`frontend/src/pages/DeliveryRunsPage.tsx`、Delivery components、`AppShell.tsx`、Task/PR Monitor/Project Todo components。
- 测试：WS gap/reconnect、stale version 409、权限按钮、旧 subject、移动端。
- 验收：用户不读聊天也能知道为何等待、谁可行动、证据属于哪个 subject。
- 回退：保留 API，隐藏导航。
- 证据：待填写。

#### [ ] DL-510 — 持久 attention、WebSocket ACL 与通知 Outbox

- 优先级：P2
- 依赖：DL-040、DL-050
- 改动：Run/user channel、sequence/version、attention GET、DeliveryNotifications、Feishu outbox。
- 文件：`ws.py`、broadcaster、`api/ws.ts`、App/AppShell、notification service/tests。
- 测试：越权订阅、断线重连、sequence gap、重复通知、服务重启、Decision resolve/expire 和 Feishu 失败重试。
- 验收：刷新/重连后不丢 Human Gate；用户不能订阅其他人的 Run/user channel。
- 回退：HTTP polling + attention page。
- 证据：待填写。

#### [ ] DL-520 — 结构化日志、指标、readiness 和 dead-letter 运维

- 优先级：P2
- 依赖：DL-050、DL-055、DL-060
- 改动：统一 Delivery correlation 字段、低基数指标、readiness snapshot、Inbox/Outbox dead-letter list/replay/resolve 管理接口。
- 文件：Controller/Reconciler/Outbox/Inbox、`backend/services/ws_broadcaster.py`、system/status API、metrics exporter、运维 UI/tests。
- 指标：inbox/outbox lag、phase duration、wake latency、CAS conflict、stuck Run、finding、merge/deploy/rollback。
- 日志字段：run/turn/event/action/task/repo/pr/subject/worker/provider；禁止 prompt/diff/secret 作为 metric label。
- API：`/api/delivery/system/status` 展示 leader、最后 reconcile、gateway/adapter 和 dead-letter。
- 测试：敏感字段脱敏、label cardinality、readiness 降级、重复 replay、无权限 replay、dead-letter resolve CAS。
- 验收：能区分外部等待、Agent 工作和系统卡死；积压可告警和人工重放。
- 回退：结构化日志保留，指标 exporter 可关。
- 证据：待填写。

#### [ ] DL-530 — Retention、审计导出与删除策略

- 优先级：P2
- 依赖：DL-030、DL-520
- 改动：payload/Timeline/Log retention；secret/PII 脱敏；Run archive；MonitoredRepo soft-delete；audit export。
- 文件：Delivery models/repository、retention service/job、admin API/UI、audit exporter、tests。
- 测试：保留期限边界、legal hold、归档后读取、Task/repo 软删除、导出权限、失败重试和幂等清理。
- 验收：删除 Task/repo 不破坏 merge/deploy审计；大 payload 不无限增长数据库。
- 回退：停止清理 job，绝不无证据硬删。
- 证据：待填写。

#### [ ] DL-540 — 仓库 CI 与多数据库测试基线

- 优先级：P1
- 依赖：可与 Phase 1 并行
- 改动：新增 `.github/workflows/ci.yml`；`frontend/package.json` 增加 `"test": "vitest run"`；PostgreSQL/MySQL migration/scheduler smoke。
- 文件：`.github/workflows/ci.yml`、`frontend/package.json`、test fixtures/containers、现有 test 配置和开发文档。
- 测试：PR/push 触发、前后端缓存失效、single-head 检查、三数据库 migration、required-check 名称稳定性。
- 验收：backend、frontend、build、lint、single Alembic head 和三数据库关键测试成为 required checks。
- 回退：Workflow 可单独禁用，但合并门槛不能在失败时自动下降。
- 证据：待填写。

#### [ ] DL-600 — Shadow/canary rollout 与 kill-switch 演练

- 优先级：P1→P3
- 依赖：每个待上线阶段
- 改动：canary repo、逐 repo opt-in、gate divergence 记录、启停/恢复/dead-letter/takeover runbook。
- 演练：在 enqueue、push、PR create、merge、deploy 响应前后故障注入并重启。
- 文件：repo policy/API/UI、fault-injection tests、`docs/runbooks/delivery-*.md`、发布清单与 dashboard。
- 测试：shadow divergence、单 repo kill switch、Controller takeover、积压恢复、未知外部结果和逐级降档。
- 验收：每个 automation level 至少稳定观察一个阶段后再开放下一级；kill switch 在不删数据的情况下停止新 effect。
- 回退：降至上一个 automation level。
- 证据：待填写。

---

## 19. 测试矩阵

### 19.1 状态与调度

- [ ] 两个 Controller 同时 claim 同一 Run。
- [ ] Turn commit 后、enqueue 前崩溃。
- [ ] enqueue 后、queue claim 前崩溃。
- [ ] queue claim 后、Agent launch 前崩溃。
- [ ] Agent push 后、checkpoint 前崩溃。
- [ ] terminal callback 丢失，由扫描/reconcile 修复。
- [ ] pause/cancel/takeover 与 Turn/Action claim 并发。
- [ ] 同一 Task 多轮使用相同 retry_count，仍能正确区分 DeliveryTurn。

### 19.2 GitHub 与 Gate

- [ ] 重复 delivery ID，同 payload 与不同 payload。
- [ ] 旧 SHA 迟到 CI/Review。
- [ ] A→B→A force-push。
- [ ] draft、ready、close、reopen、base branch change。
- [ ] repo rename/transfer，numeric repository ID 不变。
- [ ] Check Run 与 Commit Status 同名。
- [ ] 同名 check 来自不同 App。
- [ ] required workflow 从未启动。
- [ ] missing/skipped/neutral/cancelled policy。
- [ ] GitHub API rate limit、auth failure、repo deleted。
- [ ] mergeability unknown。
- [ ] merge-group rebuilt/requeued。
- [ ] 人工绕过 Gate merge。

### 19.3 Reviewer 与 Human Gate

- [ ] Reviewer 空输出、坏 JSON、无 completion marker、Task failed。
- [ ] 新 head supersede 三个 Reviewer 中的部分完成结果。
- [ ] Developer 尝试直接 resolve Finding。
- [ ] stale waiver/decision version。
- [ ] Decision waiting 期间服务重启。
- [ ] 多 Reviewer 冲突和裁决。

### 19.4 Task、Worker 与 Shared

- [ ] 用户 chat 与自动 wake 同时发生。
- [ ] stop/delete/retry/edit/migrate 与 active Turn 并发。
- [ ] Worker accept 响应丢失后重复请求。
- [ ] Worker reboot、Manager relay reconnect、迟到 mirror。
- [ ] 老 Worker capability 不足。
- [ ] 迁移后 source/target 双端竞争。
- [ ] Shared shadow 尝试创建或控制 Run。

### 19.5 Action 与 Deployment

- [ ] PR create/comment/merge 远端成功、本地确认前崩溃。
- [ ] Action expected subject 已 stale。
- [ ] merge 成功但响应丢失。
- [ ] deployment event 早于 Run 进入 deploying。
- [ ] deploy success 但 health fail。
- [ ] 环境已被新 Run 更新，旧 Run 尝试 rollback。
- [ ] 已合并故障创建 child remediation，而不是复活旧 PR。

### 19.6 ACL、Secret 与 UI

- [ ] PR Monitor repo/review IDOR。
- [ ] Webhook secret 不出现在 list/detail/WS/log/error。
- [ ] shared chat 权限无法 merge/waive/rollback。
- [ ] Run/user WebSocket channel 越权与 ACL 撤销。
- [ ] WS 全丢、乱序、重连后由 GET snapshot 恢复。
- [ ] stale action 409 后 UI 自动刷新并解释。
- [ ] 高风险按钮只按 `allowed_actions` 显示。

### 19.7 数据库与迁移

- [ ] SQLite WAL 并发 claim。
- [ ] PostgreSQL/MySQL scheduler claim smoke。
- [ ] foreign-key orphan audit。
- [ ] fresh/legacy/repeat/downgrade-upgrade migration。
- [ ] `uv run alembic heads` 只有一个 head。
- [ ] retention job 不删除 active/audit evidence。

### 19.8 建议验证命令

```bash
uv run pytest backend/tests/test_delivery_*.py -q
uv run pytest backend/tests/test_api_pr_monitor.py backend/tests/test_service_pr_review.py -q
uv run alembic heads
uv run python -m compileall backend
cd frontend && npx vitest run
cd frontend && npm run lint
cd frontend && npm run build
```

全量测试仍须在每个 Phase 合并前运行。

---

## 20. Rollout 与运行手册要求

### 20.1 开启顺序

1. Schema + Controller disabled。
2. Event Inbox + Shadow snapshot。
3. Shadow UI 与人工对照。
4. 单个 canary repo 开 local CI repair。
5. Reviewer `general` role。
6. 多 Reviewer + Finding Gate。
7. Worker canary。
8. Controller PR/comment action。
9. manual merge ready。
10. 单 repo auto-merge。
11. deployment observe。
12. deploy + health。
13. 最后评估人工 rollback；自动 rollback 继续默认关闭。

### 20.2 Kill switch 语义

- `wake off`：不创建/派发新 Agent Turn；当前 Turn 可选择完成或 exact stop。
- `review off`：不创建 Reviewer Turn；现有结果保留。
- `merge off`：未领取 merge Action 保持 pending/cancelled-by-policy；已领取先对账。
- `deploy off`：不触发新 deployment；继续观察已触发 deployment。
- `all pause`：Event 继续入库，Reconciler可只读，所有外部 effect 停止。

### 20.3 必须具备的 runbook

- Controller lease 卡死。
- Event/Action dead-letter 查看与重放。
- GitHub credential/auth failure。
- Run 人工 takeover。
- Worker unknown outcome。
- merge unknown outcome。
- Deployment/health failure。
- 数据库迁移失败。
- 关闭全部自动化但保留审计。

---

## 21. 风险登记

| 优先级 | 风险 | 控制措施 |
|---|---|---|
| P0 | Reviewer Agent 绕过 Gate 直接 merge | DL-010、独立 Merge Controller、merge kill switch |
| P0 | Developer 直接 push main | branch protection、角色凭据、prompt policy、默认分支监测 |
| P0 | PR Monitor IDOR / secret 泄露 | 目标 repo ACL、one-time secret、跨用户测试 |
| P0 | 旧 subject 执行 merge/deploy/rollback | expected subject + state_version + Action preflight |
| P0 | 内存 queue 丢 wake 或重复 Turn | durable Turn + receipt + CAS |
| P1 | 多 Controller 重复 effect | DB lease + idempotency + reconciliation |
| P1 | AskUser 内存状态丢 Human Gate | DeliveryDecision 持久化 |
| P1 | WS 丢事件导致 UI 误判 | WS hint + REST snapshot |
| P1 | Worker timeout 后重复执行 | Turn receipt/query-before-retry |
| P1 | Task 操作绕过 Run | chat/retry/edit/delete/migrate 统一路由 |
| P1 | SQLite/PG/MySQL 语义差异 | 应用层 CAS、三数据库 smoke、无 DB-specific core logic |
| P1 | 删除 repo/task 破坏审计 | soft-delete、active 409、SET NULL + snapshot |
| P2 | Reviewer 越多越保守/误报 | 角色目标、证据、dispute/waiver、误报指标 |
| P2 | Event storm/通知轰炸 | debounce/coalesce、attention 聚合、retention |
| P3 | 错误 rollback 覆盖新环境 | environment generation CAS、二次确认、默认关闭 |

---

## 22. 尚未冻结、实施前需形成 ADR 的问题

以下问题不阻塞 Shadow/CI Repair MVP，但进入对应阶段前必须形成 ADR：

- GitHub Gateway 最终使用 `gh` CLI、PAT 还是 GitHub App。
- 私有仓库 Reviewer read-only credential 的发放方式。
- required checks 从 branch protection 自动读取的权限与 fallback。
- Reviewer full rerun 之后的 diff-aware carry-forward 算法。
- Merge Queue 不可用时是否允许 direct merge。
- 第一批 Deployment Adapter 类型与 artifact identity。
- 数据库 migration 存在时的部署/回滚 policy。
- 多 Manager 部署采用 per-run lease 还是全局 leader + per-run lease。
- Delivery Run 的审计和原始 webhook payload 保留期限。

默认行为永远是 fail closed：未形成 ADR 或无法确认能力时，不开启相关自动化。

---

## 23. 完整项目 DoD

只有以下条件全部满足，Autonomous Delivery Loop 才算完整落地：

- [ ] Agent 等待外部状态时没有模型进程运行。
- [ ] 没有可执行变化时不消耗 Agent token。
- [ ] 每个 Agent Turn 有持久 ID、correlation、receipt 和恢复路径。
- [ ] Task complete 与 Delivery success 在数据和 UI 上明确分离。
- [ ] GitHub Event 重复、乱序、缺失均能由 reconciliation 收敛。
- [ ] PR-head、merge-group、merged revision、deployment artifact 证据不混用。
- [ ] CI code failure 能自动恢复同一 Developer Task。
- [ ] Developer、Reviewer、Merge、Deployment 权限边界已实际落地，不只存在于 Prompt。
- [ ] Reviewer 空结果或失败不会被当作通过。
- [ ] Developer 不能自行 resolve blocking Finding。
- [ ] 所有 merge/deploy/rollback 外部动作可对账、可幂等恢复。
- [ ] 已合并版本故障使用 child remediation 或 incident，不复活旧 PR。
- [ ] Local 与 Worker 的 Turn receipt 行为一致。
- [ ] Shared shadow 不会启动第二个 Controller。
- [ ] Human Decision、attention 和 audit 在服务重启后仍存在。
- [ ] WebSocket 全部丢失时 UI 仍可通过 REST 恢复正确状态。
- [ ] SQLite/PostgreSQL/MySQL 的迁移和核心 claim 路径均通过验证。
- [ ] 每级 automation 都有独立 kill switch、canary 记录和运行手册。

最终原则：

```text
Agent 负责解决当前可执行问题；
CCM 负责持久化意图、等待外部证据、确定下一步、执行受控副作用，
并证明一次交付真正完成。
```
