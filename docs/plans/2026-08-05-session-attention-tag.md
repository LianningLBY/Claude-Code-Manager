# Session Attention Tag Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use executing-plans to implement this plan task-by-task.

**Goal:** Add one independent, editable attention tag to every CCM Task/session and show it prominently in the Task list and Chat header without touching existing system tags.

**Architecture:** Persist a nullable, validated `Task.attention_tag` string through a single Alembic migration and the existing Task CRUD contract. Preserve it across Worker transport and Fork creation. Reuse one controlled React editor component in TaskList and ChatView.

**Tech Stack:** FastAPI, Pydantic v2, SQLAlchemy 2, Alembic, React 19, TypeScript, Tailwind CSS v4, Vitest, Testing Library, pytest.

---

### Task 1: Lock the backend contract with failing tests

**Files:**
- Modify: `backend/tests/test_api_tasks.py`

**Step 1: Add API tests**

Add focused tests that create a Task with a whitespace-padded attention tag,
verify the trimmed response, update it, clear it with whitespace, reject an
81-character value, and prove a simultaneous `tags=["pr-review"]` value is
unchanged.

**Step 2: Run the focused tests and verify failure**

Run: `uv run python -m pytest backend/tests/test_api_tasks.py -k attention_tag -q`

Expected: FAIL because the Task schemas do not accept or return
`attention_tag` yet.

### Task 2: Add the model, schema, and Alembic migration

**Files:**
- Modify: `backend/models/task.py`
- Modify: `backend/schemas/task.py`
- Create: `alembic/versions/2f6c8a1d4e90_add_attention_tag_to_tasks.py`
- Test: `backend/tests/test_api_tasks.py`
- Test: `backend/tests/test_alembic_migrations.py`

**Step 1: Add the ORM column**

Add `attention_tag: Mapped[str | None]` as nullable `String(80)` next to the
existing Task presentation metadata.

**Step 2: Add schema normalization**

Add a shared before-validator that accepts `None`, strips a string, maps an
empty result to `None`, and lets `Field(max_length=80)` reject an oversized
normalized value. Expose the field on create, update, and response schemas.

**Step 3: Add the migration**

Create revision `2f6c8a1d4e90` with down revision `7a1d4e9c2b60`. Upgrade adds
the nullable column and downgrade drops it using Alembic batch operations where
required for SQLite.

**Step 4: Run focused API and migration tests**

Run:

```bash
uv run python -m pytest backend/tests/test_api_tasks.py -k attention_tag -q
uv run python -m pytest backend/tests/test_alembic_migrations.py -q
```

Expected: PASS, with exactly one Alembic head.

### Task 3: Preserve attention tags across distributed Task flows and Forks

**Files:**
- Modify: `backend/services/task_migrator.py`
- Modify: `backend/services/worker_proxy.py`
- Modify: Worker authoritative mirror field declarations discovered by tests
- Modify: `backend/api/tasks.py`
- Test: `backend/tests/test_task_migrator.py`
- Test: `backend/tests/test_worker_proxy.py`
- Test: Fork API tests under `backend/tests/`

**Step 1: Write transport tests**

Assert Manager-to-Worker create/import payloads and authoritative mirror
updates retain `attention_tag` without changing `tags`.

**Step 2: Add the field to transport allowlists and payloads**

Extend only the Task copy surfaces that already carry user Task metadata.

**Step 3: Add Fork inheritance**

When a source Task is Forked or cloned, copy its attention tag unless the new
Task request explicitly supplies another value.

**Step 4: Run focused tests**

Run the exact tests added plus the existing migrator/worker suites affected by
the allowlist changes. Expected: PASS.

### Task 4: Build the reusable attention-tag editor test-first

**Files:**
- Create: `frontend/src/components/Tasks/AttentionTag.tsx`
- Create: `frontend/src/components/Tasks/AttentionTag.test.tsx`
- Modify: `frontend/src/api/client.ts`

**Step 1: Extend the client contract**

Add `attention_tag: string | null` to `Task`, allow it on create/update, and
keep the existing `tags` type untouched.

**Step 2: Write component tests**

Cover pill display, start edit, trimmed save, empty removal, Enter/Escape,
saving state, failed-request draft retention, and the hidden empty state.

**Step 3: Implement the component**

Use the central icon module, an amber annotation pill, a controlled inline
input with `maxLength=80`, explicit Save/Cancel controls, and accessible labels.

**Step 4: Run the component tests**

Run: `cd frontend && npx vitest run src/components/Tasks/AttentionTag.test.tsx`

Expected: PASS.

### Task 5: Integrate the editor into TaskList, the split sidebar, and ChatView

**Files:**
- Modify: `frontend/src/components/Tasks/TaskList.tsx`
- Modify: `frontend/src/components/Tasks/TaskList.test.tsx`
- Modify: `frontend/src/pages/TasksPage.tsx`
- Modify: `frontend/src/pages/TasksPage.test.tsx`
- Modify: `frontend/src/components/Chat/ChatView.tsx`
- Modify: `frontend/src/components/Chat/ChatView.test.tsx`

**Step 1: Add failing surface tests**

Assert TaskList displays an existing tag, opens an empty editor from the
overflow menu, and refreshes after save. Assert the split-view sidebar displays
the tag without replacing the task title or project badge. Assert ChatView
displays and edits the same value and exposes a compact add action when empty.

**Step 2: Integrate TaskList**

Track `editingAttentionTagId`, render the pill among the Task badges, and add
“Add attention tag” to the overflow menu only when the value is absent.

**Step 3: Integrate the split-view sidebar**

Render a read-only, truncated amber attention pill beneath the session title in
the split-view sidebar. Do not add height when no tag exists.

**Step 4: Integrate ChatView**

Render the compact component in header row two and call `onTaskUpdated` after
success. Keep the composer and visual-viewport behavior unchanged.

**Step 5: Run focused frontend tests**

Run:

```bash
cd frontend
npx vitest run src/components/Tasks/AttentionTag.test.tsx \
  src/components/Tasks/TaskList.test.tsx \
  src/pages/TasksPage.test.tsx \
  src/components/Chat/ChatView.test.tsx
```

Expected: PASS.

### Task 6: Document, verify, and publish

**Files:**
- Modify: `README.md`
- Modify: `TEST.md`
- Modify: `CLAUDE.md` (and therefore the `AGENTS.md` symlink)

**Step 1: Update documentation**

Document the user workflow, the independent field contract, and manual test
cases for desktop and iPhone in-app browsers.

**Step 2: Run the final verification matrix**

Run:

```bash
uv run alembic heads
uv run python -m pytest backend/tests/test_api_tasks.py -q
uv run python -m pytest <focused worker and migration test files> -q
cd frontend && npx tsc --noEmit
cd frontend && npx vitest run
cd frontend && npm run build
```

Expected: one Alembic head, all tests green, no TypeScript errors, and a
successful production build.

**Step 3: Commit and push**

```bash
git add alembic backend frontend docs README.md TEST.md CLAUDE.md
git commit -m "feat(tasks): add session attention tags"
git push -u fork codex/session-attention-tag
```

Create a ready PR from `zxiangx:codex/session-attention-tag` to upstream
`main`, then deploy only after CCM reports zero active tasks and the update dry
run proves the migration path.
