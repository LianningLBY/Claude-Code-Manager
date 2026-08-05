# Session Attention Tag Design

## Goal

Give every CCM Task/session one optional, user-authored attention tag so a busy
task list immediately communicates when or why the user should return. Typical
values are “等它结束后再看”, “今晚继续”, and “需要人工确认”.

## Product behavior

- A Task has zero or one attention tag.
- The value is plain text, trimmed on save, with a maximum length of 80
  characters. Saving an empty value removes the tag.
- The tag is visible from the full Task list card, the compact split-view Task
  sidebar, and the Chat header. It is editable from the full card and header.
- A Task card without a tag stays visually unchanged. Its overflow menu gains
  an “Add attention tag” action.
- An existing tag is rendered as a high-signal amber annotation pill. Clicking
  it opens an inline editor. The editor uses explicit save/cancel controls so
  mobile in-app browsers do not depend on fragile popovers or blur behavior.
- The Chat header renders the same component and exposes a compact add control
  when no tag exists.
- The edit draft survives a failed request and displays an inline error. Saving
  never changes Task status, unread state, ordering, or the running session.

## Data model and compatibility

Add a nullable `tasks.attention_tag VARCHAR(80)` column through Alembic and map
it as `Task.attention_tag`. This field is deliberately separate from the
existing JSON `Task.tags`, which contains machine-owned markers such as
`pr-review`; attention-tag code must never read, rewrite, filter, or derive from
that field.

`TaskCreate`, `TaskUpdate`, and `TaskResponse` expose the new property. A shared
normalizer trims whitespace and maps the empty string to `None`; Pydantic
rejects values longer than 80 characters. The existing `PUT /api/tasks/{id}`
endpoint performs edits under the current Task ACL and PR-review mutation
guards.

The value is part of the Task/session rather than a per-user preference. Worker
creation, Manager/Worker migration, and authoritative mirroring preserve it.
Forked sessions inherit the source value at creation and can then diverge.
Existing rows migrate to `NULL`, so current behavior and system tags remain
unchanged.

## Frontend architecture

Create a reusable controlled `AttentionTag` component. It owns the draft,
request state, error text, and immediate saved display value; parents own only
whether the editor is open. The component uses CCM's central icon module and
existing theme tokens. Its pill is amber, single-line, truncated, keyboard
accessible, and includes a Pin icon. The editor provides an 80-character input,
Save and Cancel buttons, Enter/Escape shortcuts, and a visible saving state.

`TaskList` tracks the Task currently editing, opens the editor either from an
existing pill or the overflow action, and refreshes after success. The compact
split-view list in `TasksPage` renders a read-only amber pill on its own line so
the tag remains legible without squeezing the session title or project badge.
`ChatView` places the compact form in header row two and calls its existing
`onTaskUpdated` callback after success.

## Verification

Backend tests cover create, trim, update, clear, over-length rejection, and
non-interference with `tags`. Migration tests cover upgrade/downgrade. Worker
transport tests verify the field is copied. Frontend tests cover display, add,
edit, remove, error retention, and both integration surfaces. Final verification
runs focused tests, Alembic head/round-trip checks, TypeScript, frontend tests,
the production build, and the relevant backend suite.
