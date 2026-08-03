# Native Codex Goal Lifecycle Design

## Problem

Codex Goals can start another native turn immediately after the CCM-owned turn completes. CCM currently closes its `CodexTurnProcess` at the first `turn/completed`, marks the Task terminal, and detaches the thread context. The shared app-server still sees later Goal turns, but with no context it only updates an internal runtime cache and drops their output. The UI therefore shows a green terminal task while the native thread is active. A normal follow-up is repeatedly deferred by the authoritative idle check and can only enter during a narrow turn-boundary race.

## Chosen approach

Treat an active native Goal as part of the same CCM process generation.

After a root `turn/completed`, defer process EOF long enough to query `thread/goal/get`. If the Goal is still `active`, keep the context and process attached, reset only the completed turn identity, and allow the next `turn/started` on the same thread to bind to that context. Goal continuation output then follows the existing parser, persistence, WebSocket, Task status, interrupt, and UI paths. When the Goal is absent or terminal, publish the deferred terminal event and close the process normally.

The completed-turn snapshot remains retained while the Goal is between turns.
A transient Goal RPC failure therefore retries without releasing the exact CCM
generation, while a later non-active or cleared Goal notification can close the
idle context without waiting for another turn. A newer `turn/started`
atomically supersedes the older snapshot.

For a Goal that became detached before this fix, a Standard-tier `thread/resume` reporting `active` may be adopted. CCM first proves `thread/goal/get.status == active`, creates and durably prepares the normal process context, then uses race-safe `turn/steer` identity reconciliation to deliver the user's pending message into the exact active turn. Fast-tier adoption remains forbidden because the already-running request's service tier cannot be proven.

Stopping an adopted/followed Goal pauses it before interrupting the authoritative active turn. If the Goal is between turns, an idle proof closes the CCM process without guessing a turn id.

## Safety boundaries

- Only the exact resumed thread and account home may be adopted.
- Only an explicitly active Goal is adoptable; arbitrary foreign active turns remain busy errors.
- Process ownership is prepared before steering user input.
- Turn ids are accepted only from notifications, `thread/read`, or the app-server mismatch response.
- Normal turns, Fast turns, tool-free review turns, descendants, and routing migration keep their existing fail-closed behavior.
- Goal continuation may overlap descendant shutdown. The completed root turn
  releases its turn identity immediately, while cached and newly discovered
  descendants remain fenced to the same context until a later Goal turn and
  descendant reconciliation both reach terminal state.

## Verification

Add protocol tests for multi-turn Goal following, Goal-read retries, terminal
between-turn notifications, Goal/descendant overlap, detached active-Goal
adoption (including cached descendants), non-Goal rejection, turn-boundary
stop, and Fast rejection. Run focused app-server and InstanceManager suites,
then the frontend tests, typecheck, and production build.
