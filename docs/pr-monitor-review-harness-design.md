# CCM PR Monitor Review Harness 设计

- 文档状态：设计完成，分阶段实施
- 版本：v0.1
- 更新日期：2026-08-02
- 适用范围：Legacy PR Monitor 的近期演进，以及 Autonomous Delivery Loop 的 Reviewer Panel
- 上位设计：[`autonomous-delivery-loop-design.md`](./autonomous-delivery-loop-design.md)
- 参考文章：[《揭秘 Meshy 内部模型评测系统：如何让软件开发自己跑起来》](https://mp.weixin.qq.com/s/5_q5rATl-RA_KgwrZgbCEQ)

> 本文借鉴文章展示的 Agent 易读仓库、分角色评审和 monitor loop，结合 CCM
> 现有 immutable snapshot、Task、Worker、GitHub publication outbox 与安全边界重新设计，
> 不是对原文 Prompt 或内部实现的逐字复刻。

## 1. 目标

PR Monitor 不应只是“收到 webhook 后让一个模型看 diff”。目标是建立一个可以被验证、
可以恢复、不会由模型自己宣告成功的 Review Harness：

1. Reviewer 只评审一个明确的 `(base_sha, head_sha)`。
2. Reviewer 在评审前得到由后端验证、与 base commit 同版本的工程指南。
3. 架构、实现和 QA 风险由不同视角独立检查。
4. 每个阻断项必须有代码证据、影响、最小修复建议和验证方法。
5. GitHub、CI 和数据库状态是 Gate 事实；模型输出只是候选 Finding。
6. 新 head 到来后旧结论立即失效，必须针对新 subject 重新验证。
7. 等待 CI、Review、Merge Queue 时不占用模型进程或 Instance。

## 2. 从参考文章提炼的原则

### 2.1 Agent 易读仓库

文章中的效率不只来自模型能力，还来自仓库对 Agent 友好：功能边界清楚，目录与职责稳定，
外部 I/O 通过少量 adapter 收敛，构建和测试能尽早暴露错误。

在 CCM 中，这对应两件事：

- 代码结构本身提供局部性，Reviewer 不需要猜测同一能力的第二套入口。
- 仓库维护明确的 Guides，让 Reviewer 能判断“这段改动是否放在正确位置、是否复用了已有
  能力、是否破坏跨模块不变量”。

### 2.2 分角色评审

单一泛化 Reviewer 容易在架构、实现细节和用户行为之间丢失焦点。目标 Reviewer Panel 包含：

- `principal_engineer`：架构适配、并发、安全边界、跨模块不变量。
- `senior_engineer`：实现正确性、错误路径、可维护性、边界条件。
- `qa_engineer`：用户路径、回归、生产陷阱、测试与发布证明。

角色不是三种语气，而是三套不同的检查责任。一个角色的“通过”不能抵消另一个角色有证据的
阻断 Finding。

### 2.3 Monitor loop

Agent 不应持续轮询 GitHub，也不应在等待期间保持会话运行。Controller 持久化当前等待集合，
由 webhook 和定时 Reconciler 更新事实：

```text
new head
→ CI pending
→ CI failed：唤醒 Developer 修复
→ CI passed：启动 exact-subject Reviewer
→ blocking Finding：唤醒 Developer 修复
→ 新 head：重新跑 CI 和 Reviewer
→ required Reviewer 全通过
→ 人工合并，或后续进入 Merge Queue
```

循环退出条件由 Gate 计算，不由 Agent 的“完成了”“LGTM”或 checkpoint 决定。

## 3. 当前实现基线

当前 Legacy PR Monitor 已具备以下能力：

- 校验 GitHub webhook HMAC，只处理目标分支上的 `opened` 和 `synchronize`。
- 以 `repo_id + PR + base_sha + head_sha` 和 `X-GitHub-Delivery` 做持久去重。
- 在创建 Task 前由后端读取并验证 exact base 的 `CLAUDE.md`、`PROGRESS.md`。
- 在前后 snapshot guard 之间读取完整 PR metadata、files 和 compare patch。
- 将所有模型输入作为 JSON 注入；Reviewer 没有 filesystem、shell、network、GitHub 或 MCP 工具。
- 终态输出使用严格的 body/result block；空输出、冲突输出和未知结果 fail closed。
- GitHub Review/merge 由后端执行，带 exact head、随机 nonce、持久 publication outbox 和跨进程 lease。
- `synchronize` 先持久化替换意图，再安全终止旧 Task；启动恢复可继续未完成替换。
- Review Task 支持 Claude/Codex、独立 model、effort，以及本机或 Worker 执行。

当前测试分支进一步把 Legacy 单 Reviewer Prompt 改为 Principal/Senior/QA 三视角综合评审。
这只是兼容阶段的单模型 harness，不等于三个独立 Reviewer，也不能成为 Delivery Gate 的最终实现。

## 4. 当前问题与边界

### 4.1 单次三视角不是独立 Panel

同一个模型在同一上下文中依次扮演三个角色，会共享注意力和先入结论，无法提供真正独立证据。
近期可用它提升 Legacy Review 质量，但必须明确标记为 `general` Reviewer。

### 4.2 Legacy Review 仍然直接产生 GitHub Action

Legacy 流程把终态推荐映射为 `REQUEST_CHANGES`、`APPROVE`，并可选自动 merge。Delivery 流程不得
照搬：Reviewer 只能产生 Finding，Controller 计算 Gate，Action Outbox 才能发布或合并。

### 4.3 当前 Guide Pack 太窄

`CLAUDE.md` 和 `PROGRESS.md` 能提供总体规则与经验，但大型仓库还需要结构、测试、运行和评审规则。
直接让 Reviewer 自行遍历全部文档会扩大提示注入面、造成不确定输入，并增加 token 成本。

### 4.4 当前流程不是完整 CI loop

Legacy PR Monitor 在 PR 事件后直接发起 Review；它不是先等待 exact-head required checks，再进入
Reviewer Panel 的持久状态机。完整闭环属于 Delivery Controller，不扩写进 Legacy webhook handler。

## 5. Review Guide Pack

### 5.1 目的

Guide Pack 是 Reviewer 对仓库约定的唯一权威输入。每份文档都必须从 captured base commit 读取，
经后端校验后注入，不能从本地工作区、默认分支或 PR head 替代。

第一阶段继续支持：

```text
CLAUDE.md    # 规范性项目指南
PROGRESS.md  # 历史教训、已知陷阱和当前约束
```

目标仓库可逐步整理以下文档：

```text
docs/architecture/overview.md   # 模块边界、依赖方向、状态所有权
docs/architecture/invariants.md # 并发、事务、身份、幂等与安全不变量
docs/testing/strategy.md        # 测试层级、关键 fixture、必跑验证
docs/operations/runbook.md      # 部署、恢复、观测和回滚约束
docs/review/checklist.md        # 仓库特有的 Review 检查项
```

这些是建议的语义位置，不要求所有仓库使用同一目录。最终由 base commit 根目录中的显式 manifest
声明允许注入的路径，Reviewer 不进行自由文档发现。

### 5.2 Manifest 提案

后续增加可选的 `.ccm/review-guides.json`：

```json
{
  "version": 1,
  "documents": [
    {"path": "docs/architecture/overview.md", "roles": ["principal_engineer"]},
    {"path": "docs/architecture/invariants.md", "roles": ["principal_engineer", "senior_engineer"]},
    {"path": "docs/testing/strategy.md", "roles": ["senior_engineer", "qa_engineer"]},
    {"path": "docs/operations/runbook.md", "roles": ["qa_engineer"]},
    {"path": "docs/review/checklist.md", "roles": ["principal_engineer", "senior_engineer", "qa_engineer"]}
  ]
}
```

约束：

- manifest 与文档均从 exact base root tree 读取。
- 只接受相对路径、普通 UTF-8 文件，不接受 symlink、`..`、NUL 或重复路径。
- 固定文件数、单文件和总字节上限；tree/blob 截断、大小不一致或解码失败时 fail closed。
- 每个注入记录包含 path、byte length、SHA-256 和完整 content。
- 文档内容低于固定 Review Contract，高于不受信任的 PR title/body/diff。
- PR 对 Guide 的修改只作为普通 diff 被审查，合并前不能改变本次规则。
- 公共 Review 不应逐字泄露私有 Guide，只引用必要的规则名称和行为后果。

### 5.3 文档维护原则

- Guide 描述稳定边界和理由，不记录容易过期的逐函数清单。
- 新的跨模块不变量必须同时进入代码测试和相应 Guide，不能只写 Prompt。
- `AGENTS.md` 与 `CLAUDE.md` 的关键项目规则保持同步；若为 symlink 则只维护源文件。
- `PROGRESS.md` 用于经验和事故教训，不取代规范性架构文档。
- Guide 变更必须像代码一样经过 Review；Reviewer 应检查它是否试图放宽自身权限或 Gate。

## 6. Reviewer Prompt Harness

### 6.1 Prompt 分层

每个 Reviewer Task 的 Prompt 由 Controller 构造，按固定顺序组成：

```text
1. Fixed Review Contract       # 服务端代码，最高优先级
2. Verification Subject       # repo/pr/base/head 或 merge-group identity
3. Backend-verified Guide Pack
4. Backend-verified PR Material
5. Role Contract              # principal/senior/qa
6. Finding Output Schema
7. Strict Completion Marker
```

PR 内容、Guide 内容和代码注释都可能包含提示注入。任何一层输入都不能修改 subject、Reviewer 权限、
结果 schema、发布权限或 Gate 规则。

### 6.2 共享 Contract

所有角色必须遵守：

- 只审查注入的 exact subject，不假设默认分支或本地 checkout 与其相同。
- 不调用 GitHub 写接口，不修改代码，不 push，不 merge。
- 不以缺少证据为理由猜测通过；安全关键事实无法确认时产生 `unknown` 或阻断 Finding。
- Finding 必须指向注入 patch 的文件与行/区块，不得伪造路径和行号。
- 偏好、命名和非必要重构不得伪装成阻断项。
- 先独立完成本角色判断，再输出结构化结果；不读取其他角色的自然语言结论。

### 6.3 Principal Engineer Role Contract

核心问题：

- 改动是否放在正确模块，是否复用现有 capability，而不是引入第二条实现路径？
- 状态所有权、锁序、事务边界、幂等、取消、恢复和跨进程 fencing 是否仍成立？
- 身份、ACL、credential、Worker/Manager 权威边界是否保持？
- 改动是否局部、可回退，并与 base Guides 中的架构一致？
- 是否存在 diff 看似正确、系统组合后才出现的失败模式？

Principal 不负责逐行挑风格；只有架构或系统行为风险才形成 Finding。

### 6.4 Senior Engineer Role Contract

核心问题：

- 控制流、状态转换、异常、取消、重试和资源释放是否正确？
- 输入校验是否覆盖类型混淆、边界值、空值和不可信数据？
- 数据库更新与外部副作用之间是否存在丢失、重复或竞态窗口？
- 实现是否重复现有 helper，或者绕过统一入口和安全门？
- 测试是否覆盖改动的关键成功、失败与并发路径？

Senior 可以指出维护性问题，但只有可能造成实际错误的项目才能阻断。

### 6.5 QA Engineer Role Contract

核心问题：

- 需求声称的用户行为是否真的成立，是否遗漏主要用户路径？
- 旧功能、权限差异、Worker、本机、Claude/Codex 和重启场景会不会回归？
- 生产中的网络失败、重复 webhook、乱序事件、超时和部分成功会怎样？
- 测试是证明行为，还是只断言实现细节和字符串存在？
- 如果 QA 为上线签字，是否会因为功能不符合声明或无法安全验证而阻断？

QA 不根据“测试文件存在”判定通过，必须核对测试是否能证明风险已被约束。

## 7. Finding Contract

### 7.1 目标结构

Delivery Reviewer 使用结构化 JSON；Legacy 阶段可以先渲染为等价文本：

```json
{
  "schema_version": 1,
  "subject": {
    "kind": "pr_head",
    "base_sha": "<40-hex>",
    "head_sha": "<40-hex>"
  },
  "role": "qa_engineer",
  "verdict": "changes_required",
  "findings": [
    {
      "severity": "medium",
      "category": "regression",
      "path": "backend/api/example.py",
      "line": 123,
      "hunk": null,
      "title": "Retry path loses the pending event",
      "evidence": "The failure branch commits state before preserving the event.",
      "impact": "A transient failure can permanently stall the delivery run.",
      "required_fix": "Persist the retry intent in the same transaction.",
      "test": "Kill the handler after commit and verify startup recovery reclaims it."
    }
  ],
  "completion_marker": "CCM_REVIEW_COMPLETE_V1"
}
```

### 7.2 严重度

| Severity | 含义 | Gate |
|---|---|---|
| `critical` | 可导致严重安全、数据完整性或不可恢复事故 | 阻断 |
| `high` | 高概率产生错误行为、越权、重复副作用或主要功能失效 | 阻断 |
| `medium` | 有具体证据的回归、边界错误或不可接受的验证缺口 | 阻断 |
| `low` | 有价值但不影响当前 subject 安全交付的建议 | 不阻断 |

`unknown` 不是低严重度。必需 Reviewer 解析失败、Task 失败、输出冲突或 subject 不匹配时，角色状态为
`unknown`，Gate 必须 fail closed。

### 7.3 Finding 身份与生命周期

Finding fingerprint 至少包含：

```text
repo numeric id + role + category + normalized path + normalized root cause
```

不能只用行号，因为修复会移动代码。每个 Finding 绑定发现它的 exact subject：

```text
open
→ Developer 新提交：fix_proposed
→ 新 subject Reviewer：resolved 或 open

open
→ Developer 提交证据化反驳：disputed
→ Reviewer/裁决角色：resolved 或 open

open
→ 授权人 exact-subject waiver：waived
```

Developer、Reviewer Task 和自然语言评论都不能直接把 Finding 标为 `resolved` 或 `waived`。

## 8. Reviewer Panel 与 Gate

### 8.1 exact-subject Review Run

一个 Review Run 必须冻结：

- repository numeric identity、PR number。
- `VerificationSubject(kind=pr_head, base_sha, head_sha)`。
- required roles。
- 每个角色的 provider、model、effort。
- Prompt policy version/hash 和 Guide Pack hash。

角色 Task 可以并行执行，但彼此不共享输出。Controller 只在所有 required roles 达到明确终态后计算
Review Gate。

### 8.2 Gate 规则

```text
PASS =
  subject 仍是当前 PR head
  AND required CI checks 对该 subject 全部成功
  AND 每个 required role 都有一个可解析、subject 匹配的 terminal result
  AND 没有 open/fix_proposed/disputed 的 blocking Finding
  AND 没有 unknown role result
  AND 没有待处理的人类 Decision
```

任何 Reviewer 的 `verdict=pass` 都只是输入；只有纯 Gate Evaluator 可以得出 PASS。

### 8.3 新 head 与迟到结果

- `synchronize` 创建新 subject，并将旧 Review Run 标为 superseded。
- 旧 Task 可以被请求停止，但 Controller 不能依赖停止成功来建立新事实。
- 旧 subject 的迟到结果只保留审计，不参与新 Gate，也不能发布到新 head。
- 第一版不自动 carry forward Finding；新 Panel 对新 patch 全量重审。
- 相同 delivery、相同 subject 和相同 role 的重复 completion 必须幂等。

## 9. Monitor Loop 数据流

```text
GitHub webhook / Reconciler
        │
        ▼
durable Event Inbox ──去重/验签/规范化──► authoritative Snapshot
        │                                      │
        ▼                                      ▼
Delivery Controller ──► Wait Set ──► pure Gate Evaluator
        │                                      │
        ├─ CI failed ──► Developer Turn        │
        ├─ CI passed ──► Reviewer Panel        │
        ├─ Finding ────► Developer Turn        │
        └─ Gate passed ─► Action Outbox ◄──────┘
                              │
                              ▼
                    Review / Merge Queue / Merge
```

关键约束：

- Webhook 只持久化事实并快速返回，不在请求中等待 Task 或杀进程。
- Reconciler 定期从 GitHub 拉取权威状态，修复丢 webhook 和乱序事件。
- 相同 root cause 的多个 CI/Review 失败合并为一次 Developer wake，避免 token 风暴。
- 每次 Developer Turn 有预算和 no-progress signature；连续无进展进入 Human Gate。
- 等待状态不占 Instance，服务重启后由 DB 恢复 Wait Set。

## 10. GitHub 发布与权限边界

### 10.1 Reviewer 身份

Reviewer 只获得读取 Review 输入所需的数据，不获得 GitHub write credential。后端 Gateway：

- 在写入前重新确认 exact open subject。
- 以数据库 outbox 记录 action、actor、subject、nonce 和 payload hash。
- 使用 `commit_id=head_sha` 发布 Review。
- 超时后先按 nonce 对账，不能直接重发。
- actor 改变、subject 改变或 generation 失效时 fail closed。

### 10.2 Merge 身份

Delivery 模式下 Reviewer 永远不能 merge。Merge Controller 只有在 Gate PASS、repo opt-in、merge switch
开启、credential capability 满足以及执行前 preflight 全部成功时，才能创建 Merge Action。

Legacy `auto_merge` 在迁移期间保留，但不能与同一 repo 的 Delivery Review 同时启用。

## 11. Legacy 到 Delivery 的实施阶段

### Stage A — Legacy prompt harness（当前测试分支）

- 单 Reviewer Task 内执行 Principal/Senior/QA 三视角。
- 文本 Finding 使用统一严重度和证据格式。
- 保留当前严格 terminal block 和后端 publication 协议。
- 不声称具备独立 Reviewer、Finding DB 或 CI Gate。

### Stage B — Guide Pack

- 增加 `.ccm/review-guides.json` 的 exact-base 读取与校验。
- Prompt 按 role 只注入必要文档，同时保留共享 normative Guides。
- 保存 Guide Pack hash 和 Prompt policy hash，支持审计与重放。
- 对 manifest 注入、symlink、截断、超限和 head 修改做 fail-closed 测试。

### Stage C — General exact-subject Reviewer

- 在 Delivery 数据模型中实现一个 `general` role 和结构化结果。
- Review switch 默认关闭；只在 shadow 模式与 Legacy 结果对照。
- Task/parse/subject 任一未知都不能放行。

### Stage D — 独立三角色 Panel

- 扩展为三个独立 Task/Review Run，冻结 provider/model/effort。
- 增加 Finding 表、fingerprint、生命周期和 UI。
- 验证角色缺失、重复、迟到、坏 JSON 与 Provider 差异。

### Stage E — Monitor loop

- CI 全绿后才创建 Reviewer Panel。
- blocking Finding 聚合成 Developer wake package。
- 新 head 自动 supersede 旧 subject 并重新验证。
- 首先停在人工 merge；Merge Queue 与自动 merge 后续单独开启。

## 12. 测试与验收

### 12.1 Prompt/Guide

- exact base Guide 与 exact compare patch 注入。
- PR head 修改 Guide 不影响当前 policy。
- title/body/diff 中的指令不能改变 subject、权限或输出协议。
- 三角色检查项、严重度、证据字段和 completion marker 均存在。
- 超限、坏 UTF-8、NUL、symlink、截断和 hash/size 不一致 fail closed。

### 12.2 Reviewer terminal

- 空输出、坏 JSON、多个 completion、未知 verdict、缺 role 和 subject 不匹配。
- Task failed/cancelled/conflict、retry generation 变化和旧 Task 迟到。
- 同 role 同 subject 重复 completion 幂等；不同结果进入冲突状态。

### 12.3 Gate/loop

- CI pending/failed/flaky/auth/permission 分类。
- webhook 丢失、重复、乱序以及 Reconciler 修复。
- review 执行期间新 head、Panel 部分完成时新 head。
- open/fix_proposed/disputed/waived Finding 的 Gate 表。
- 服务在 event commit、turn enqueue、review completion、outbox write 各边界崩溃后恢复。
- 等待 CI/Review/Merge 时没有运行模型进程和占用 Instance。

### 12.4 发布

- GitHub 写入 pin 到 exact head，nonce 对账防止重复副作用。
- self-review fallback 不会被误认成 approval。
- actor、repo、PR、head 或 action policy 改变时拒绝发布。
- Reviewer credential 无 push/merge 能力；Developer credential 无默认分支 push 能力。

## 13. 可观测性

结构化日志和审计至少包含：

```text
delivery_run_id, review_run_id, finding_id, task_id,
repo_id, pr_number, subject_kind, base_sha, head_sha,
role, provider, model, effort, prompt_policy_hash,
event_id, action_id, state, reason
```

Prompt、完整 diff、Guide 内容、credential 和 webhook secret 不能作为 metric label，也不应默认进入普通日志。

UI 需要展示：当前 subject、required roles、每个角色状态、blocking Findings、CI Wait Set、下一步动作、
最近一次 Reconciler 时间以及人工接管入口。WebSocket 只发刷新提示，REST snapshot 是事实来源。

## 14. 非目标

- 不让 Reviewer 自己修代码或执行测试。
- 不让 Agent 轮询 GitHub、CI 或 Merge Queue。
- 不用三个角色的多数票覆盖有证据的阻断项。
- 不把自然语言 `LGTM` 当成 Gate PASS。
- 不在 Legacy webhook 中继续堆叠完整 Delivery 状态机。
- 不在第一版自动跨 subject 继承 Finding。
- 不因模型或 Provider 不可用而静默降级为少一个 required role。

## 15. 与上位 Backlog 的对应关系

- Guide Pack 和 Prompt policy：连接 DL-130、DL-200。
- exact-subject Reviewer 与结构化结果：DL-200。
- PR Monitor 转 Gate Producer：DL-210。
- Finding、dispute、waiver 与重验证：DL-220。
- CI 失败修复与 Wait Set：DL-170、DL-180。
- Review/Merge Action Outbox：DL-055、DL-300、DL-310、DL-320。

本文细化 Review Harness 的输入、角色、输出和 Gate contract；状态机、数据表、migration、Worker 对等
和完整 rollout 顺序仍以 `autonomous-delivery-loop-design.md` 为准。
