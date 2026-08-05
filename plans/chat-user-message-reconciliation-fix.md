# CCM 聊天旧消息重复/移位修复计划

## 1. 背景与现象

用户在 Task Chat 的运行中 turn 里通过“💉 注入”发送补充指令。消息最初显示在正确位置，但 CCM 完成回复后，部分旧消息又出现在对话底部；加载更早记录后，还可能同时看到原位置和底部副本。

该问题也可能影响普通用户消息，只是长 turn 中的注入消息更容易触发。

## 2. 已确认根因

当前链路为：

1. `/api/tasks/{id}/inject` 先通过 Claude PTY 或 Codex `turn/steer` 完成真实注入。
2. 注入成功后，后端创建并提交一条 `LogEntry(event_type="user_message")`。
3. 后端向 `task:{id}` 广播该消息，但广播体没有携带已提交日志的 `id` 和权威 `timestamp`。
4. 前端收到事件后只能生成临时 ID，并把它视为未持久化的实时消息。
5. turn 结束时，`ChatView` 重新请求最近 200 条历史。
6. 长 turn 的 thinking/tool/message 事件可能把较早的注入日志挤出最新一页。
7. `mergeChatHistory()` 会保留这条“未持久化实时消息”，并将它追加在所有持久化消息之后，因此旧消息看起来被再次发送。
8. 加载更早分页时，真实持久化行又可能被插入，形成两个可见副本。

这属于展示层身份和分页对账错误，不会再次调用 `/inject`、`turn/steer` 或 Dispatcher enqueue。原始指令只在首次成功注入时影响当前 turn。

## 3. 修复目标

- 每条已持久化 Task Chat 消息在 WebSocket 和 HTTP 历史中使用同一个数据库 `LogEntry.id`。
- 已持久化实时消息即使不在最新 200 条快照中，也必须保持原有时间顺序。
- WebSocket 重复投递、重连回填、turn 结束刷新和向前分页均不得产生重复气泡。
- 相同文本可以由用户真实发送多次，不能按文本全局去重。
- 注入消息的 `source="inject"`、发送者前缀和附件元数据保持不变。
- 修复普通消息及其他同类 Task `user_message` 入口，避免只修截图中的单一路径。
- 不引入数据库迁移，不改变模型收到的 prompt，也不改变任务调度语义。

## 4. 非目标

- 不重写完整聊天状态管理架构。
- 不修改 Discussion 独立消息系统。
- 不改变历史分页大小或删除已有聊天记录。
- 不把 UI 去重当作发送幂等机制；本计划只处理一次成功发送后的展示一致性。
- 不用文本内容、时间窗口或发送者名字作为持久消息的最终身份。

## 5. 消息身份契约

所有“先写入本地 `log_entries`，再广播到本地 Task Chat”的事件必须满足：

```json
{
  "event_type": "user_message",
  "id": 12345,
  "task_id": 88,
  "timestamp": "2026-07-30T11:41:00.123456Z",
  "role": "user",
  "content": "[Admin] ...",
  "raw_content": "...",
  "source": "inject"
}
```

约束：

- `id` 必须是当前 CCM 数据库中已提交的 `LogEntry.id`。
- `timestamp` 必须来自同一条 `LogEntry`，并使用统一的 UTC ISO-8601 格式。
- 广播只能发生在数据库提交成功之后。
- 远程 Worker/Shared 事件若复制到本地数据库，必须使用本地副本 ID；不能把远端 ID 冒充本地 ID。
- 未持久化的 delta、权限卡片等临时事件不强制具有 `LogEntry.id`。
- 兼容旧端事件：缺少 `id` 时保留现有 occurrence-count/fingerprint 降级逻辑，但不得用它覆盖新契约。

## 6. 实施阶段

### Phase 1：先补回归测试，固定故障

在修改实现前增加最小可复现测试：

1. 当前消息列表包含一条实时注入消息。
2. 随后加入超过 200 条持久化回复事件，使注入消息不在最新 HTTP 快照内。
3. 模拟 `process_exit` 后刷新历史。
4. 断言注入消息只出现一次，且仍排在对应回复之前。
5. 再加载包含该注入日志的较早分页。
6. 断言仍只有一条，`source`、附件和时间不丢失。

同时增加以下边界用例：

- 同一个持久化 ID 经 WebSocket 投递两次，只显示一次。
- WebSocket 与 HTTP 快照并发返回同一个 ID，只显示一次。
- 两条内容完全相同但 ID 不同的用户消息，两条都保留。
- 普通 optimistic 用户气泡被权威 WebSocket 行替换，不出现一临时一持久两份。
- 历史刷新只调用 GET，不再次调用 `injectTaskMessage` 或 `sendTaskChat`。

### Phase 2：后端广播权威持久化身份

审计所有 Task `user_message` 的“落库 + 广播”入口：

| 路径 | 预期处理 |
|------|----------|
| `backend/api/chat.py::send_chat_message` | 广播 `user_log.id/timestamp` |
| `backend/api/chat.py::_store_injected_message` | 保留 `LogEntry` 对象并广播其 ID/时间 |
| `backend/api/chat.py::_send_shared_chat` | 广播本地 `user_log` 身份 |
| `backend/api/chat.py::_send_worker_chat` | 不再匿名 `db.add()`，保留本地日志对象 |
| `backend/api/shared_access.py` | owner 端共享消息广播本地日志身份 |
| `backend/services/dispatcher.py` | Monitor/Sub-Agent 注入日志提交后补充 ID/时间 |
| `backend/services/shared_relay.py` | 复制远端事件后，用本地新行身份覆盖远端身份再广播 |

实现要求：

- 提取一个窄范围的 payload helper，统一添加 `id`、`task_id`、UTC `timestamp`，避免各入口再次遗漏。
- helper 只序列化已经 flush/commit 的 `LogEntry`；不负责数据库写入或广播。
- 保留各入口已有的 `raw_content`、`source`、`attachments`、`image_urls`、`sender_name` 等展示字段。
- 继续保持“提交成功后广播”；若广播失败，HTTP 历史仍是最终事实源。
- 不修改 Codex/Claude transport 调用次数和顺序。

### Phase 3：前端改为按持久化 ID upsert

调整 `ChatView` 和消息合并逻辑：

1. WebSocket 消息包含合法正整数 `id` 时，立即标记 `persisted=true`。
2. 当前列表已存在相同持久化 ID 时执行原位更新，不追加新气泡。
3. 普通发送的 optimistic 气泡与权威行对账后，替换临时气泡并继承权威 ID。
4. 对账不能只检查数组最后一项；状态/系统事件插入时仍应找到对应 optimistic 用户气泡。
5. 内容相同但 ID 不同的持久化消息不得折叠。
6. 最新历史快照不包含的旧持久化实时消息继续保留，并与快照行按数据库 ID 排序。
7. “加载更早消息”也通过 ID 合并，不再直接无条件 prepend。
8. 分页游标从已加载的持久化历史行取得；不得使用 `Date.now()` 生成的临时 UI ID。
9. 对无 ID 的旧后端事件保留 occurrence-count fingerprint 兼容逻辑，且只作为降级路径。

如现有 optimistic 对账测试证明仅靠 `raw_content` 无法区分并发的相同文本消息，再单独引入可选 `client_message_id`；首轮修复不主动扩大到发送幂等协议。

### Phase 4：跨路径验证

后端测试至少覆盖：

- 普通 local chat：数据库行 ID、广播 ID、历史 ID 三者相同。
- Claude PTY 注入：transport 一次、日志一条、广播一次且带 ID。
- Codex steer 注入：transport 一次、日志一条、广播一次且带 ID。
- 注入失败：不落日志、不广播成功消息。
- Worker local display copy：使用 Manager 本地日志 ID。
- Shared owner/shadow/relay：本地广播使用本地 ID，不复用远端 ID。
- Monitor/Sub-Agent 来源消息：保留 source 并带持久化 ID。

前端测试至少覆盖：

- 最新 200 条快照排除旧注入消息。
- `process_exit`、订阅确认、WebSocket 重连分别触发历史刷新。
- WebSocket 先到、HTTP 后到和 HTTP 先到、WebSocket 后到两种竞态。
- 向前分页包含已在内存中的持久化消息。
- 重复 ID、相同文本不同 ID、附件注入、发送者前缀。
- 普通消息与注入消息都不会移动到回复之后。

## 7. 预计修改文件

- `backend/api/chat.py`
- `backend/api/shared_access.py`
- `backend/services/dispatcher.py`
- `backend/services/shared_relay.py`
- 一个小型后端 chat-event payload helper（若现有模块没有合适位置）
- `frontend/src/components/Chat/ChatView.tsx`
- `frontend/src/components/Chat/messageMerge.ts`
- `backend/tests/test_api_chat_plan.py`
- `backend/tests/test_message_pipeline.py`
- Shared/Worker/Dispatcher 对应测试文件
- `frontend/src/components/Chat/ChatView.test.tsx`
- `frontend/src/components/Chat/messageMerge.test.ts`

本修复不需要 Alembic migration；项目架构不变时不更新 `AGENTS.md`。

## 8. 验收标准

### 功能验收

- 在一个产生超过 200 条可见日志的 turn 中连续注入 3 条消息。
- turn 运行中，3 条消息显示在各自注入时的位置。
- turn 完成后，3 条消息仍各出现一次，且没有移动到回复底部。
- 刷新页面、断开并恢复 WebSocket、重新进入 Task 后结果不变。
- 点击“Load older messages”直到包含这些消息，仍各出现一次。
- 消息的“💉 注入”标签、发送者前缀、附件和原时间保持正确。
- Network/后端 mock 证明每次用户操作只有一次 `/inject` 或 `/chat` 写请求；历史刷新不会产生新的模型输入。

### 数据验收

- 每次成功注入对应一条 `log_entries.event_type="user_message"`。
- WebSocket `id` 与历史 API 返回的该行 `id` 一致。
- 相同文本连续发送两次会生成两个不同 ID，并显示两次。
- Shared/Worker 场景中的 ID 属于当前展示该 Task 的本地数据库。

### 回归验收

- ChatView、messageMerge、chat API、注入、Worker/Shared relay 和 Dispatcher 定向测试全部通过。
- 前端 build/typecheck 通过。
- 不改变 Claude/Codex 收到的实际消息文本、附件输入和发送次数。
- 不引入数据库迁移或历史数据清理脚本。

## 9. 风险与控制

| 风险 | 控制措施 |
|------|----------|
| 将远端 ID 当成本地 ID | relay 落库后强制用本地 `LogEntry.id` 覆盖 payload |
| 相同文本被错误折叠 | 持久化消息只按 ID 去重；fingerprint 仅用于无 ID 兼容 |
| optimistic 气泡残留 | 对账搜索未持久化候选，不只检查最后一条 |
| 分页重复 | older-page 合并统一按 ID upsert |
| 时间显示偏移 | 广播与历史统一 UTC ISO-8601 格式 |
| 广播早于提交形成幽灵消息 | 明确保持 commit-before-broadcast |
| 修复扩大到其他消息系统 | 限定 Task Chat；Discussion 单独评估 |

## 10. 发布与回滚

发布顺序建议：

1. 后端先部署，使新 WebSocket 事件开始携带权威 ID。
2. 再部署前端 ID-based upsert；前端仍兼容短时间内的无 ID 事件。
3. 用一个长 Codex turn 和一个 Claude PTY turn 做生产烟测。
4. 观察是否仍出现底部旧消息、重复分页或异常消息缺失。

回滚不涉及数据库：

- 前后端代码可独立回滚。
- 已写入的 `LogEntry` 无需清理。
- 新增 WebSocket 字段对旧前端应为向后兼容字段。

## 11. 完成定义

- 根因测试在旧实现上稳定失败、在新实现上通过。
- 所有 Task `user_message` 本地持久化广播入口均完成审计。
- 长 turn、重连、刷新、分页和相同文本边界均通过验收。
- 代码评审确认不存在二次 transport/enqueue。
- 生产烟测完成且无重复/移位消息。
