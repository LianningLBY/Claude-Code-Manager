# Native Codex Goal Lifecycle Implementation Plan

**Goal:** Keep CCM's task lifecycle and UI authoritative while a native Codex Goal continues across turns, and safely recover detached active Goals.

**Architecture:** Extend the existing `CodexAppServer` turn context instead of adding a parallel background runtime. A deferred terminal guard queries Goal state, reuses the same process across native continuation turns, and closes it only when the Goal is terminal. Resume admission adopts only proven active Standard-tier Goals through exact `turn/steer` reconciliation.

**Tech stack:** Python 3.13, asyncio, Codex app-server JSON-RPC, FastAPI/SQLAlchemy lifecycle consumers, pytest, React/TypeScript/Vitest.

---

### Task 1: Specify lifecycle behavior with failing tests

- Add app-server tests covering active Goal continuation, final completion, output routing, detached adoption, turn-boundary stop, and non-Goal/Fast rejection.
- Run only the new tests and confirm they fail for the expected missing behavior.

### Task 2: Follow Goals across native turns

- Extend `_TurnContext` with deferred Goal lifecycle state.
- Defer root terminal publication while `thread/goal/get` proves the Goal active.
- Reset turn identity without detaching the process, and bind the next native turn.
- Preserve existing descendant and terminal-error handling.

### Task 3: Adopt an already-detached active Goal

- Validate active thread plus active Goal on Standard resume.
- Prepare the ordinary CCM owner/process before steering input.
- Reconcile the exact active turn id and fail closed on ambiguous races.
- Keep Fast and non-Goal active resumes rejected.

### Task 4: Make stop deterministic between Goal turns

- Pause the Goal first.
- Interrupt a proven active turn, or close on authoritative idle proof.
- Test both active-turn and between-turn cases.

### Task 5: Verify and deliver

- Run focused backend tests and affected InstanceManager/API tests.
- Run frontend tests, TypeScript check, and production build.
- Rebase/integrate with the formula-rendering production branch, push, deploy through CCM's guarded updater, and verify health plus a live Goal scenario.
