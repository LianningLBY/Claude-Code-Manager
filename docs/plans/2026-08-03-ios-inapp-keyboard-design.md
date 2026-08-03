# iOS In-App Keyboard Viewport Design

## Problem

The full-screen task chat uses a fixed overlay and also locks `body` with
`position: fixed`. In iOS Feishu's WKWebView, opening the software keyboard
shrinks the visual viewport without reliably changing the layout viewport.
The chat therefore keeps its pre-keyboard height and its composer can remain
behind the keyboard. The fact that one task sometimes works is incidental
WebKit focus scrolling; all affected tasks use the same `ChatView`.

## Chosen approach

Keep the existing body scroll lock, but make the non-inline chat root follow
`window.visualViewport`. A small reusable hook writes the current visual
viewport height and top offset directly to the root element, listens for
`resize` and `scroll`, and restores the original CSS fallback on cleanup. The
message list is already a shrinking flex child, so reducing the root height
naturally places the composer immediately above the keyboard without moving
or reparenting the composer itself.

The hook is enabled only for the full-screen chat. Desktop split view keeps
its parent-controlled `h-full` layout. Browsers without `visualViewport`
retain the current `fixed inset-0` behavior.

## Alternatives rejected

- Calling `scrollIntoView()` when the textarea receives focus is unreliable
  when the document body is fixed and can move conversation content rather
  than resize the usable chat area.
- Removing the body scroll lock would let WebKit scroll the task list behind
  the full-screen chat and reintroduce the issue that the lock was added to
  solve.
- Relying only on `100dvh` is not sufficient for iOS in-app webviews because
  software-keyboard resizing support varies by WebKit version and embedding
  configuration.

## Verification

Unit tests will simulate visual viewport resize and scroll events, verify the
root receives the new height/top bounds, verify inline mode is untouched, and
verify the fallback when the API is absent. The existing ChatView suite,
TypeScript check, and production build must remain green. Final manual
acceptance is opening a task in Feishu on iPhone, focusing the composer, and
confirming the composer stays directly above the keyboard through typing,
keyboard dismissal, and orientation changes.
