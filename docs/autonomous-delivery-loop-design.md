# CCM Autonomous Delivery Loop 详细设计

- 状态：设计草案
- 版本：v0.1
- 日期：2026-08-01
- 范围：代码实现、CI、AI Review、自动修复、Merge Queue 与部署验证组成的交付循环
- 暂不包含：Linear 接入细节、代码库架构治理、Reviewer Prompt 全文

## 1. 背景

当前 Coding Agent 通常以一次 Task 为单位工作：

1. 理解需求。
2. 修改代码。
3. 执行测试。
4. 提交结果。
5. Task 完成。

这种模式只能说明 Agent 做出了一版实现，不能证明代码最终满足远程 CI、AI Review、最新 main、Merge Queue 和部署健康等交付条件。

Muse 文章真正展示的不是“一次生成正确代码”，而是一个可以持续数小时、由外部证据推动的软件交付循环：

~~~text
实现
→ 本地验证
→ 创建 PR
→ 等待 CI 和 Review
→ 自动处理失败与意见
→ 再次 Push
→ 重新验证
→ 进入 Merge Queue
→ 验证部署
→ 完成
~~~

这个循环必须由 CCM 的持久状态机控制，不能依赖模型记住流程，也不能依赖一个 Agent 进程持续运行。

## 2. 目标

新模式暂命名为 Autonomous Delivery Loop，中文名为“自动交付循环”。

它需要做到：

- Agent 自己完成代码实现、本地测试、创建 PR 和 Push。
- CCM 等待并解析 CI、Reviewer、Merge Queue 和 Deployment 的外部状态。
- 出现可执行失败时，CCM 恢复同一个 Developer Task。
- Agent 修复、验证、Push 后再次退出。
- 只有全部客观门槛满足，Delivery Run 才能完成。
- 服务重启、Webhook 重复、Agent 崩溃和 GitHub 暂时不可用都不会破坏流程。

人类只在以下情况参与：

- 需求存在关键设计歧义。
- Agent 无法安全选择方案。
- 需要新的权限或外部授权。
- 多轮修复没有进展。
- Reviewer 之间存在无法自动裁决的冲突。
- 涉及不可逆或高风险操作。

## 3. 非目标

第一版不做以下事情：

- 不让一个 Agent 进程持续运行数小时。
- 不让模型按固定间隔轮询 GitHub。
- 不用聊天记录作为唯一状态来源。
- 不把“Agent 说完成了”当成交付完成。
- 不把“没有 Review comment”当成 Review 通过。
- 不允许开发 Agent 自己绕过合并门槛。
- 不允许旧 commit 的 CI 结果证明新 commit 正确。
- 不因为循环次数多就降低质量标准。
- 不支持一个 Delivery Run 同时原子修改多个 Repo。
- 不支持多个 Developer Agent 并行修改同一个分支。

## 4. 核心设计原则

### 4.1 Agent 是执行器，CCM 是状态机

Agent 负责：

- 理解问题。
- 写代码。
- 分析失败。
- 修复问题。
- 编写和运行测试。
- 对 Review 意见提出修复或反驳。

CCM 负责：

- 当前处于哪个阶段。
- 当前 PR 和 exact head SHA 是什么。
- 当前正在等待什么。
- 哪些 CI 已完成。
- Reviewer 是否都已完成。
- 是否存在未解决问题。
- 是否应该唤醒 Agent。
- 是否满足合并门槛。
- 是否应该进入 Merge Queue。
- Loop 是否真正结束。

### 4.2 每次 Agent 只运行一个有限回合

每个回合遵守：

~~~text
Observe → Decide → Act → Prove → Yield
观察       决策      行动    提供证据   退出等待
~~~

Agent 完成当前能做的工作后，提交结构化检查点并退出。外部状态发生可执行变化后，CCM 再恢复同一个 Task 会话。

### 4.3 所有验证绑定 exact head SHA

任何 CI、Review 和 Merge Gate 都必须绑定：

~~~text
repository + PR number + head SHA
~~~

如果 Agent Push 了新 commit：

- 旧 SHA 的 CI 结果失效。
- 旧 SHA 的 Reviewer 通过结果失效。
- 尚未完成的旧事件变成 stale。
- CCM 针对新 SHA 建立新的验证周期。

### 4.4 外部真实状态优先

CCM 不直接信任：

- Agent 声称已经 Push。
- Agent 声称测试通过。
- 单条 Webhook 的局部状态。
- GitHub comment 中的自然语言。
- 前一次缓存的 PR 状态。

发生重要状态转换前，CCM 必须重新查询 GitHub，取得当前权威状态。

### 4.5 确定性控制与模型判断分离

模型可以判断“如何修”，但下列判断必须由程序完成：

- required checks 是否全部通过。
- required reviewers 是否全部完成。
- blocking findings 是否为零。
- PR 当前是否可合并。
- head SHA 是否仍然相同。
- 是否允许进入 Merge Queue。
- Deployment 是否属于目标 merge commit。

## 5. 总体架构

~~~text
                 ┌─────────────────────┐
                 │ GitHub / CI / Review│
                 │ Merge / Deployment  │
                 └──────────┬──────────┘
                            │ Webhook
                            ▼
                 ┌─────────────────────┐
                 │ Event Inbox         │
                 │ 验签、去重、持久化    │
                 └──────────┬──────────┘
                            ▼
                 ┌─────────────────────┐
                 │ State Reconciler    │
                 │ 拉取外部权威状态      │
                 └──────────┬──────────┘
                            ▼
                 ┌─────────────────────┐
                 │ Loop Controller     │
                 │ 状态转换、并发控制    │
                 └───────┬───────┬─────┘
                         │       │
              需要修复    │       │ 满足门槛
                         ▼       ▼
               ┌────────────┐  ┌─────────────┐
               │ Developer  │  │ Merge Queue │
               │ Agent Turn │  │ / Deployment│
               └─────┬──────┘  └──────┬──────┘
                     │                 │
                     └───────┬─────────┘
                             ▼
                       重新计算状态
~~~

主要组件：

1. Delivery Run：整个交付流程的持久化根对象。
2. Event Inbox：接收并保存外部事件。
3. Reconciler：读取 GitHub 当前真实状态。
4. Gate Evaluator：计算 CI、Review 和 Merge 门槛。
5. Wake Scheduler：决定是否启动新的 Agent 回合。
6. Developer Agent：修改代码和处理反馈。
7. Reviewer Panel：产生结构化 Findings。
8. Action Outbox：可靠执行评论、重跑、进入 Merge Queue 等外部动作。

## 6. 为什么不能直接复用 Task 状态

CCM Task 会在每个 Agent 回合结束后进入 completed，后续消息又可能把它恢复运行：

~~~text
completed → executing → completed
~~~

Delivery Loop 可能持续数小时并包含很多回合，因此不能把整个交付状态放在 Task.status 中。

推荐新增独立对象 DeliveryRun：

~~~text
DeliveryRun
  ├── 绑定一个主 Developer Task
  ├── 绑定一个 Issue
  ├── 绑定一个 Repo / Branch / PR
  ├── 绑定多个 Agent Turn
  ├── 绑定多个 Reviewer Run
  ├── 绑定多个 Finding
  └── 绑定多个外部 Event
~~~

职责划分：

- Task 表示 Agent 会话和单个执行回合的运行状态。
- DeliveryRun 表示整个软件交付过程是否完成。

## 7. 状态模型

不建议把所有组合塞进单个巨大 status 枚举，推荐拆成三个正交字段。

### 7.1 Phase

表示流程走到哪里：

~~~text
investigate
implement
validate
pr_review
merge
deploy
done
~~~

### 7.2 Activity

表示当前活动：

~~~text
ready
running
waiting
paused
terminal
~~~

### 7.3 Outcome

只在终态存在：

~~~text
success
failed
cancelled
superseded
blocked
~~~

示例：

~~~json
{
  "phase": "pr_review",
  "activity": "waiting",
  "outcome": null,
  "wait_for": [
    "required_ci",
    "principal_reviewer",
    "senior_reviewer",
    "qa_reviewer"
  ]
}
~~~

UI 根据 phase、activity 和 wait_for 派生用户可读状态。

## 8. 主状态流程

~~~text
创建 DeliveryRun
  ↓
investigate/running
  ├─ 需要决策 → investigate/waiting(human_decision)
  └─ 需求明确
  ↓
implement/running
  ↓
validate/running
  ├─ 本地失败 → implement/running
  └─ 本地通过
  ↓
创建或绑定 PR
  ↓
pr_review/waiting
  ├─ CI 失败 → implement/running
  ├─ Review Finding → implement/running
  ├─ Branch Conflict → implement/running
  └─ 所有门槛通过
  ↓
merge/ready
  ↓
merge/waiting(merge_queue)
  ├─ Merge Queue 失败 → implement/running
  └─ 合并成功
  ↓
deploy/waiting
  ├─ 部署失败 → implement/running 或 blocked
  └─ 健康验证通过
  ↓
done/terminal/success
~~~

## 9. 一次 Agent 回合的生命周期

### 9.1 唤醒前

CCM 构造 Wake Package：

~~~json
{
  "run_id": 123,
  "turn_generation": 7,
  "trigger": {
    "type": "ci_failed",
    "head_sha": "abc123",
    "check_name": "backend-tests"
  },
  "issue": {
    "id": "MES-12345",
    "title": "Support checkpoint voting",
    "requirements": "..."
  },
  "approved_decisions": [],
  "repository": {
    "repo": "org/project",
    "branch": "agent/mes-12345",
    "pr_number": 456,
    "head_sha": "abc123"
  },
  "ci": {
    "failed_checks": [],
    "pending_checks": []
  },
  "review": {
    "open_findings": []
  },
  "previous_checkpoint": {}
}
~~~

这个包只包含本轮所需变化和持久状态。Agent 可以继续通过 gh 查询完整日志。

### 9.2 回合中

Agent 可以：

- 查看失败日志。
- 修改文件。
- 运行测试。
- Commit 和 Push 工作分支。
- 回复 Review thread。
- 提供反驳证据。
- 请求人工决策。

Agent 不能：

- 直接修改 DeliveryRun 状态。
- 宣布远程 CI 已通过。
- 宣布 Reviewer 已完成。
- 自己解除未经复核的 Finding。
- 直接绕过 Merge Gate。
- Push main。
- 擅自修改合并策略。

### 9.3 回合结束

Agent 必须调用结构化工具 report_delivery_checkpoint：

~~~json
{
  "run_id": 123,
  "turn_generation": 7,
  "result": "waiting",
  "summary": "Fixed timeout handling and added regression coverage",
  "actions_taken": [
    {
      "type": "commit",
      "sha": "def456"
    },
    {
      "type": "push",
      "branch": "agent/mes-12345"
    }
  ],
  "local_verification": [
    {
      "command": "pytest tests/test_timeout.py",
      "result": "passed"
    }
  ],
  "finding_resolutions": [
    {
      "finding_id": "senior-abc123-2",
      "resolution": "fixed",
      "evidence": "commit def456"
    }
  ],
  "wait_for": [
    "required_ci",
    "review_panel"
  ]
}
~~~

CCM 验证：

- turn_generation 是否仍然有效。
- commit 是否真实存在。
- branch 是否属于该 DeliveryRun。
- GitHub 当前 head SHA 是否确实变化。
- Agent 是否越权修改 main。
- wait condition 是否合法。

检查点验证成功后，本轮 Agent 才能退出并进入等待。

如果 Agent 没有提交 checkpoint 就退出，Controller 将其视为异常回合，先对账 Git 和 GitHub 状态，再决定是否恢复。

## 10. Event Inbox

所有外部事件必须先持久化，再处理。

逻辑字段：

~~~text
DeliveryEvent
- id
- run_id
- source
- source_event_id
- event_type
- repo
- pr_number
- head_sha
- payload
- payload_hash
- received_at
- processed_at
- processed_generation
~~~

唯一约束：

~~~text
source + source_event_id
~~~

主要事件：

~~~text
pull_request.opened
pull_request.synchronize
pull_request.closed
check_run.completed
check_suite.completed
workflow_run.completed
reviewer.completed
finding.created
finding.resolved
merge_group.created
merge_group.failed
pull_request.merged
deployment.started
deployment.succeeded
deployment.failed
human.answer_received
timer.reconcile
~~~

## 11. 事件处理规则

Webhook 到达后不能直接启动 Agent。

正确路径：

~~~text
Webhook
→ 验签并持久化
→ 去重
→ 拉取 GitHub 当前状态
→ 计算 Gate
→ 判断是否有可执行变化
→ 决定是否唤醒 Agent
~~~

事件处理表：

| 事件 | 是否唤醒 Agent |
|---|---:|
| 一个 CI job 开始运行 | 否 |
| 一个检查成功但其他检查仍在运行 | 否 |
| 任意 required check 最终失败 | 是 |
| 所有 required checks 已完成 | 是，重新计算 Gate |
| 新增 blocking finding | 是 |
| Reviewer 明确完成且 findings 为空 | 不一定，只重新计算 Gate |
| PR 出现冲突 | 是 |
| Merge Queue 失败 | 是 |
| Merge Queue 成功 | 否，进入部署阶段 |
| 部署失败 | 是或请求人类 |
| 重复 Webhook | 否 |
| 旧 SHA 的迟到事件 | 否 |

## 12. 事件合并与单 Agent 原则

Agent 运行期间可能继续收到多个事件。此时不能并行启动第二个修复 Agent。

处理规则：

1. Event Inbox 持续接收事件。
2. 当前 Agent Turn 保持唯一运行。
3. Agent 结束后读取全部未处理事件。
4. 重新查询 GitHub 当前权威状态。
5. 丢弃旧 SHA 和重复事件。
6. 基于最新聚合状态决定下一步。

例如：

~~~text
Agent 正在修复 CI A
→ CI B 也失败
→ Reviewer 新增 Finding
→ Agent Push 新 SHA
~~~

旧 SHA 的 CI 和 Review 已经失效，CCM 不应额外启动三个 Agent，而应直接等待新 SHA 的新一轮验证。

## 13. Wait Set

每次 Agent 退出前，DeliveryRun 都必须保存结构化等待条件：

~~~json
{
  "head_sha": "def456",
  "conditions": [
    {
      "type": "required_checks_terminal"
    },
    {
      "type": "reviewer_completed",
      "reviewer": "principal"
    },
    {
      "type": "reviewer_completed",
      "reviewer": "senior"
    },
    {
      "type": "reviewer_completed",
      "reviewer": "qa"
    }
  ]
}
~~~

Wait Set 不能是自由文本，必须是 CCM 可以确定性求值的条件。

## 14. 防止 Lost Wakeup

典型竞态：

~~~text
Agent Push
→ CI 很快完成
→ Webhook 已经到达
→ Agent 随后才登记等待 CI
→ 系统永远等不到下一条事件
~~~

解决方案：

1. 所有 Webhook 无条件写入 Event Inbox。
2. Agent checkpoint 和 Wait Set 在事务中提交。
3. Wait Set 提交后立即检查是否已有更新事件。
4. 再主动执行一次 GitHub reconciliation。
5. 只有条件仍未满足时，才真正进入 waiting。

核心原则：

~~~text
注册等待
→ 检查过去是否已经发生
→ 再等待未来
~~~

## 15. CI Gate

每个 Repo 需要配置 required checks 的来源：

~~~text
branch_protection
explicit_list
~~~

示例：

~~~json
{
  "required_checks_mode": "explicit_list",
  "required_checks": [
    "guardrails",
    "lint",
    "typecheck",
    "tests",
    "visual",
    "security",
    "container-boot"
  ]
}
~~~

统一状态：

~~~text
pending
running
passed
failed
cancelled
skipped
missing
~~~

规则：

- missing 不能视为通过。
- skipped 是否允许由 Repo 策略决定。
- cancelled 默认视为失败。
- 旧 SHA 的 passed 不能用于当前 SHA。
- 基础设施失败和代码失败应保留不同分类。

## 16. Reviewer Panel Gate

每个 Reviewer Run 必须绑定：

~~~text
run_id
reviewer_role
head_sha
started_at
completed_at
status
findings
~~~

状态：

~~~text
pending
running
completed
failed
superseded
~~~

Reviewer 必须显式返回：

~~~json
{
  "status": "completed",
  "head_sha": "def456",
  "findings": []
}
~~~

没有 GitHub comment 不等于 Reviewer 已完成。

默认 Reviewer：

~~~text
principal_engineer
senior_engineer
qa_engineer
~~~

新 Push 后，旧 Reviewer Run 标记为 superseded，并针对新 SHA 创建新一轮 Review。

## 17. Finding 生命周期

逻辑字段：

~~~text
DeliveryFinding
- id
- run_id
- reviewer_role
- head_sha
- fingerprint
- severity
- blocking
- file
- line
- title
- description
- evidence
- status
- resolution
- resolved_by_review_run_id
~~~

状态：

~~~text
open
fix_proposed
disputed
resolved
superseded
~~~

修复路径：

~~~text
open
→ Agent 修改代码
→ fix_proposed
→ Reviewer 在新 SHA 上验证
→ resolved
~~~

反驳路径：

~~~text
open
→ Agent 提交证据
→ disputed
→ 原 Reviewer 或裁决 Reviewer 复核
→ resolved 或重新 open
~~~

Developer Agent 不能直接把自己的 Finding 标记为 resolved。

## 18. Merge Gate

Merge Gate 必须由 CCM 程序计算。

~~~python
def can_merge(run, github_state, review_state):
    return (
        run.activity != "running"
        and github_state.pr_open
        and github_state.head_sha == run.head_sha
        and github_state.branch_mergeable
        and github_state.required_checks_complete
        and github_state.required_checks_passed
        and review_state.all_required_reviewers_completed
        and review_state.unresolved_blocking_findings == 0
        and not run.pending_human_decisions
        and not run.cancelled
        and not run.paused
    )
~~~

Gate 结果必须同时返回阻断原因：

~~~json
{
  "ready": false,
  "blockers": [
    {
      "type": "ci_running",
      "name": "visual-tests"
    },
    {
      "type": "finding_open",
      "id": "qa-def456-1"
    }
  ]
}
~~~

## 19. Merge Queue 与 Action Outbox

Gate 满足后，CCM 写入可靠 Action Outbox，而不是在 Controller 事务中直接调用 GitHub。

~~~text
DeliveryAction
- id
- run_id
- action_type
- idempotency_key
- expected_head_sha
- payload
- status
- attempts
- last_error
~~~

幂等键：

~~~text
run_id + action_type + expected_head_sha
~~~

执行合并前再次检查：

- PR 仍然 open。
- head SHA 没变。
- Gate 仍然通过。
- 没有新的 Finding。
- 没有新的 Agent Turn。
- 分支仍然可合并。

如果 Merge Queue 基于最新 main 的验证失败：

~~~text
merge/waiting
→ validate/ready
→ 唤醒 Agent
~~~

## 20. 部署完成条件

每个 Repo 可以选择完成策略：

~~~text
merge_only
deployment_success
deployment_and_healthcheck
manual_release
~~~

推荐最终默认 deployment_and_healthcheck。

DeliveryRun 只有在以下条件满足后才成功：

~~~text
exact merge commit 已部署
AND deployment status = success
AND health check = passed
~~~

第一版可以先使用 merge_only，后续再引入部署闭环。

## 21. 并发与代次控制

每个 DeliveryRun 同时只能有一个 Controller 和一个 Developer Agent Turn。

建议字段：

~~~text
controller_generation
active_turn_generation
lease_token
lease_expires_at
expected_head_sha
expected_task_retry_count
~~~

启动 Agent 前：

1. 锁定 DeliveryRun。
2. 确认没有 active turn。
3. 递增 active_turn_generation。
4. 保存触发本轮的 Event IDs。
5. 提交事务。
6. 启动 Agent。

Agent checkpoint 必须携带同一 generation。旧 Agent 的迟到结果直接拒绝。

## 22. 外部动作幂等

下列动作都可能出现“远端成功，本地在确认前崩溃”：

- 创建 PR。
- Push。
- 发布 Review comment。
- 回复 Finding。
- Resolve thread。
- 重新运行 CI。
- 进入 Merge Queue。
- 合并 PR。

恢复时必须先对账外部状态：

~~~text
读取 GitHub 当前状态
→ 查找动作是否已经成功
→ 已成功则补记本地状态
→ 未成功才重新执行
~~~

## 23. Agent 崩溃恢复

Agent Turn 异常退出时，不能立即使用相同 Prompt 重跑，因为它可能已经修改、Commit、Push 或回复了评论。

恢复步骤：

1. 读取本地 Git 状态。
2. 查询远程分支。
3. 查询 PR head SHA。
4. 查询最新 CI 和 Review。
5. 对比上一个 checkpoint。
6. 生成恢复目标。
7. 再决定是否启动 Agent。

恢复 Prompt 必须要求先核对事实，禁止重复副作用。

## 24. 防止无限循环

不能只设置“最多循环五次”。应检测是否产生进展。

Progress Signature：

~~~text
head_sha
+ failing_check_fingerprints
+ open_finding_fingerprints
+ approved_decision_version
+ merge_queue_state
~~~

如果一个 Agent 回合结束后 Signature 没变化：

~~~text
no_progress_count += 1
~~~

建议策略：

- 第一次无进展：允许重新诊断。
- 第二次无进展：启动独立诊断 Agent。
- 第三次相同无进展：暂停并请求人类。
- SHA、测试结果、Finding 或决策发生变化后计数清零。

## 25. 失败分类

| 失败类型 | 行为 |
|---|---|
| 代码或测试失败 | 唤醒 Developer Agent |
| Blocking Finding | 唤醒 Developer Agent |
| 基础设施瞬时失败 | 有界自动重跑 |
| Flaky Test | 重跑一次，重复则诊断 |
| GitHub API 暂时不可用 | 退避，不唤醒 Agent |
| 权限不足 | 请求管理员 |
| 需求歧义 | 请求人类决策 |
| 同一错误多轮无进展 | 诊断 Agent，随后人类 |
| PR 被关闭 | cancelled |
| 新需求取代旧需求 | superseded |
| 未知主体 force-push | 暂停并重新确认所有权 |

## 26. Human Decision Gate

Agent 通过结构化工具提交问题：

~~~json
{
  "question_id": "decision-3",
  "question": "Should existing records be migrated eagerly or lazily?",
  "options": [
    {
      "id": "lazy",
      "impact": "Lower deployment risk, temporary dual-read complexity"
    },
    {
      "id": "eager",
      "impact": "Simpler runtime, higher migration risk"
    }
  ],
  "recommended": "lazy",
  "blocking": true
}
~~~

DeliveryRun 转为：

~~~text
phase=investigate
activity=waiting
wait_for=human_decision:decision-3
~~~

回答收到后：

- 保存为不可变 Decision Record。
- 恢复同一个 Developer Task。
- 后续所有 Agent 和 Reviewer 都收到该决策。
- 如果设计文档更新，记录文档 commit 和 hash。

## 27. CCM 与 Monitor 的关系

文章使用 Claude Code Monitor 唤醒 Agent，但 CCM 不应让 Developer Agent 自己维护唯一 Monitor，因为：

- Agent 可能在创建 Monitor 前崩溃。
- Agent 可能遗漏等待条件。
- Monitor 可能被误停。
- 服务重启后必须恢复。
- 多个 Monitor 可能重复唤醒。
- 无法统一保证 exact SHA。

推荐：

- Delivery Loop Controller 是权威状态机。
- GitHub Webhook 是主要事件来源。
- 后台 reconciliation scheduler 是补漏机制。
- CCM Monitor Skill 可以作为辅助，但不是流程真相来源。

Monitor 只提供“眼睛”，Controller 负责“现在该做什么”。

## 28. 与现有 PR Monitor 的关系

现有 PR Monitor 应成为 Delivery Loop 的 Gate Producer，而不是整个 Loop。

~~~text
Delivery Loop
- 管理 Developer Task
- 管理 CI、Review、修复和合并总流程
- 决定是否唤醒 Developer Agent
- 计算最终 Merge Gate

PR Monitor
- 针对 exact head SHA 创建 Reviewer Tasks
- 收集 Principal、Senior、QA 结果
- 创建结构化 Findings
- 在 GitHub 发布 Review
- 报告 reviewer_completed
~~~

Reviewer Task 与 Developer Task 必须分离：

- Developer 的目标是把代码交付出去。
- Reviewer 的目标是阻止错误代码合并。
- Reviewer 不共享 Developer 的隐藏推理上下文。
- Developer 只能修复或反驳，不能修改 Reviewer 原始结论。

## 29. 凭据与权限分离

### Developer Agent

允许：

- 读取 Repo。
- 修改和 Push 工作分支。
- 回复 Review。
- 读取 CI。

禁止：

- Push main。
- 修改 branch protection。
- 直接合并。
- 删除 Review 证据。

### Reviewer Agent

允许：

- 读取代码和 PR。
- 发布 Review Finding。

禁止：

- 修改代码。
- Push。
- Merge。
- 访问生产凭据。

### CCM Merge Controller

允许：

- 在 Gate 满足后进入 Merge Queue。
- 查询部署状态。

它不执行代码修改。

## 30. Prompt Injection 边界

下列内容均属于不可信输入：

- Issue 正文。
- PR title 和 body。
- Commit message。
- Review comment。
- CI 日志。
- 代码注释。
- PR diff。

Prompt 必须明确区分：

~~~text
可信控制指令
可信项目规范
已批准的人类决策
不可信任务与代码内容
~~~

不可信内容不能改变 Loop 权限、合并门槛和凭据边界。

## 31. API 草案

创建 Delivery Run：

~~~http
POST /api/delivery-runs
~~~

~~~json
{
  "task_id": 123,
  "project_id": 5,
  "issue_ref": "MES-12345",
  "auto_merge": true,
  "completion_policy": "deployment_and_healthcheck"
}
~~~

查询状态：

~~~http
GET /api/delivery-runs/{id}
~~~

控制：

~~~http
POST /api/delivery-runs/{id}/pause
POST /api/delivery-runs/{id}/resume
POST /api/delivery-runs/{id}/cancel
~~~

绑定 PR：

~~~http
POST /api/delivery-runs/{id}/pr
~~~

提交 Agent checkpoint：

~~~http
POST /api/delivery-runs/{id}/checkpoint
~~~

回答设计问题：

~~~http
POST /api/delivery-runs/{id}/decisions/{question_id}
~~~

查看 Gate：

~~~http
GET /api/delivery-runs/{id}/gate
~~~

## 32. 数据模型草案

### DeliveryRun

~~~text
id
task_id
project_id
issue_provider
issue_id
issue_url
phase
activity
outcome
repo_full_name
branch_name
pr_number
pr_url
head_sha
base_sha
plan_ref
plan_hash
wait_set
controller_generation
active_turn_generation
lease_token
lease_expires_at
no_progress_count
last_progress_signature
auto_merge
completion_policy
created_at
updated_at
completed_at
~~~

### DeliveryTurn

~~~text
id
run_id
generation
task_retry_count
trigger_event_ids
objective
started_at
completed_at
result
checkpoint
error
~~~

### DeliveryEvent

保存所有外部事件及其处理代次。

### DeliveryFinding

保存 Reviewer 问题及解决生命周期。

### DeliveryAction

保存所有需要可靠执行的外部副作用。

### DeliveryDecision

保存人类做出的不可变设计决策。

第一版可以将部分低频字段放入 JSON，但事件、Action Outbox 和 Finding 必须具有独立身份与唯一约束。

## 33. UI 设计

页面应优先显示当前门槛，而不是只显示聊天：

~~~text
当前阶段：等待 CI 与 Review
当前 PR：#456
当前 SHA：def456
运行中 Agent：无

等待：
  ✓ lint
  ✓ typecheck
  ✗ backend-tests
  … visual-tests
  ✓ principal review
  1 senior finding
  … QA review

为什么不能合并：
  - backend-tests failed
  - senior finding SEN-3 unresolved
~~~

建议提供：

- Timeline：Push、CI、Review、修复、合并。
- Current Gate：精确阻断原因。
- Findings：open、fixed、disputed、resolved。
- Agent Turns：触发原因和证据。
- Human Decisions：问题和最终答案。
- Pause、Resume、Cancel、Take Over。
- 当前 head SHA 及是否过期。

## 34. 可观测指标

建议记录：

~~~text
从需求到第一次 PR 的时间
第一次 CI 通过率
第一次 Review 通过率
平均修复轮数
每轮新增和关闭 Finding 数
Agent 实际工作时间
外部等待时间
人类介入次数
无进展次数
CI flaky 比例
Merge Queue 失败率
部署失败率
Reviewer 误报和漏报反馈
~~~

最重要的时间拆分：

~~~text
实现时间
验证时间
等待时间
修复时间
~~~

## 35. 崩溃恢复要求

CCM 重启后必须恢复：

- waiting 中的 DeliveryRun。
- 尚未处理的 Webhook。
- 正在 publishing 的 Review。
- 尚未确认的 Merge Action。
- Agent 已 Push 但未 checkpoint 的状态。
- Merge 已成功但本地未记录的状态。
- Deployment 已成功但本地未完成的状态。

启动恢复顺序：

~~~text
恢复 Action Outbox
→ 恢复 active lease
→ 对所有非终态 Run 查询 GitHub
→ 标记 stale SHA
→ 重新计算 Gate
→ 恢复 Wait Set
→ 唤醒真正需要行动的 Run
~~~

重启后禁止无条件重放 Agent Prompt。

## 36. 必须测试的竞态

1. 重复 Webhook 不重复唤醒。
2. 旧 SHA 的 CI 失败迟到不会唤醒。
3. Agent 运行中收到多个失败事件，只启动一个 Turn。
4. Agent Push 后、checkpoint 前崩溃。
5. checkpoint 前 CI 已完成，不丢失唤醒。
6. Reviewer 返回空 findings，但没有 completion marker。
7. 三个 Reviewer 中一个失败，不能合并。
8. Agent 回复 Finding 后不能自行标记 resolved。
9. 新 Push 后旧 Reviewer 通过结果失效。
10. Merge 请求成功但 CCM 在响应前崩溃。
11. Merge Queue 因最新 main 失败后重新进入循环。
12. DeliveryRun waiting 时 CCM 重启。
13. Pause 后事件继续入库，但不能唤醒 Agent。
14. Cancel 与 Agent Turn 启动并发。
15. 同一失败连续多轮没有进展。
16. GitHub API 暂时不可用。
17. PR 被未知用户 force-push。
18. PR 在循环中被人工关闭或合并。
19. Deployment 成功事件早于本地进入 deploy 阶段。
20. 两个 CCM 进程同时尝试领取同一个 Run。

## 37. 分阶段上线

### Phase 0：Shadow Mode

- 观察现有 PR。
- 计算 Gate。
- 记录“如果开启 Loop，现在会做什么”。
- 不唤醒 Agent，不修改 GitHub。

### Phase 1：自动修复 CI

- Agent 创建 PR 后进入 waiting。
- CI 失败自动唤醒。
- 修复、测试、Push。
- 暂不自动处理 Review，不自动 Merge。

### Phase 2：接入 Reviewer Panel

- PR Monitor 输出结构化 Findings。
- 自动唤醒 Developer Agent。
- 支持修复和反驳。
- Finding 清零后仍由人工 Merge。

### Phase 3：Merge Queue

- CCM 确定性计算 Gate。
- 自动进入 Merge Queue。
- 暂不把部署作为完成条件。

### Phase 4：部署闭环

- 监听 deployment status。
- 执行健康检查。
- 部署成功后 DeliveryRun 才完成。
- 部署失败时自动创建修复循环或请求人类。

## 38. 第一版产品边界

第一版支持：

- GitHub 仓库。
- 一个 Developer Task。
- 一个 PR。
- 固定三个 Reviewer。
- GitHub Actions required checks。
- exact head SHA。
- CI 和 Review 失败自动唤醒。
- 人工 Merge 或可选 Merge Queue。
- 服务重启恢复。
- Pause、Resume、Cancel。

第一版暂不支持：

- 一个 DeliveryRun 同时管理多个 PR。
- 跨多个 Repo 的原子交付。
- 自动生产回滚。
- 动态增减 Reviewer。
- Agent 自动拆分多个独立 Issue。
- 多个 Developer Agent 并行修改同一分支。

## 39. 核心不变量

1. 一个 DeliveryRun 同时最多一个 Developer Agent Turn。
2. 每次验证只证明一个 exact head SHA。
3. 旧 SHA 事件不能改变新 SHA 状态。
4. Agent 退出不等于 DeliveryRun 完成。
5. 没有 Finding 不等于 Reviewer 已完成。
6. Developer 不能自行解除 Reviewer Finding。
7. 所有外部副作用可恢复、可对账、幂等。
8. 所有等待状态必须持久化。
9. 所有 Webhook 先落库、后处理。
10. 合并前必须重新读取 GitHub 权威状态。
11. 新事件到达时先计算是否可执行，再决定是否唤醒。
12. CCM 重启不能重复 Push、评论或 Merge。
13. completed 的唯一含义是完整交付条件已经满足。

## 40. 验收标准

第一版满足以下条件才可认为设计落地：

- Agent 等待 CI 时不保持模型进程运行。
- 没有外部状态变化时不消耗 Agent Token。
- CI 失败可以自动恢复同一 Developer Task。
- Review Finding 可以自动进入修复或反驳流程。
- 新 Push 会使旧 SHA 验证自动失效。
- 重复和乱序 Webhook 不会重复执行。
- 服务重启后可以从数据库和 GitHub 恢复。
- Gate 能明确显示所有未满足条件。
- Developer Agent 无权绕过 Gate 或直接合并 main。
- Merge Queue 失败会重新进入修复循环。
- 所有 required checks 和 reviewers 完成且 blocking findings 为零时才允许合并。

## 41. 最终结论

这个 Loop 的本质是：

~~~text
Agent 负责解决下一步问题，
CCM 负责保存状态、等待证据、判断下一步以及确认真正完成。
~~~

真正长期运行的是：

~~~text
持久状态机
+ GitHub 事件
+ 确定性 Gate
+ 有界 Agent Turn
+ 可恢复外部动作
~~~

它与 Ralph Loop 的根本区别：

- Ralph Loop 持续询问模型“任务完成了吗”。
- Delivery Loop 根据 CI、Review、Merge 和 Deployment 的客观证据，决定什么时候需要再次调用模型。
