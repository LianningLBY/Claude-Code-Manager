# iOS In-App Keyboard Viewport Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Keep the full-screen task chat composer visible above the software keyboard in Feishu's iPhone WKWebView.

**Architecture:** Add a focused React hook that synchronizes a fixed chat root with `window.visualViewport`, then connect it to the non-inline `ChatView`. Preserve the existing body scroll lock and CSS fallback.

**Tech Stack:** React 19, TypeScript, Vitest, Testing Library, Tailwind CSS

---

### Task 1: Specify visual viewport synchronization

**Files:**
- Create: `frontend/src/hooks/useVisualViewportBounds.test.tsx`
- Create: `frontend/src/hooks/useVisualViewportBounds.ts`

**Step 1: Write the failing tests**

Create a harness with a root ref and cover initial height/top synchronization,
`resize`, `scroll`, cleanup, disabled mode, and a missing
`window.visualViewport` fallback.

**Step 2: Run the focused test and verify it fails**

Run: `npx vitest run src/hooks/useVisualViewportBounds.test.tsx`

Expected: FAIL because `useVisualViewportBounds` does not exist.

**Step 3: Implement the minimal hook**

Synchronize `height`, `top`, and `bottom` directly on the referenced element.
Subscribe to visual viewport `resize`/`scroll` and window
`orientationchange`; remove listeners and inline overrides on cleanup.

**Step 4: Run the focused test and verify it passes**

Run: `npx vitest run src/hooks/useVisualViewportBounds.test.tsx`

Expected: PASS.

### Task 2: Connect the full-screen task chat

**Files:**
- Modify: `frontend/src/components/Chat/ChatView.tsx`
- Modify: `frontend/src/components/Chat/ChatView.test.tsx`

**Step 1: Write the failing integration assertion**

Render a non-inline ChatView with a mocked visual viewport and assert that its
fixed root follows the visual viewport. Render inline mode and assert it does
not receive viewport overrides.

**Step 2: Run the ChatView tests and verify the new assertion fails**

Run: `npx vitest run src/components/Chat/ChatView.test.tsx`

Expected: the new viewport assertion fails.

**Step 3: Attach the hook to ChatView**

Add a root ref, enable synchronization only when `inline` is false, and keep
the current fixed/full-height and body-lock fallbacks unchanged.

**Step 4: Run the focused suites**

Run: `npx vitest run src/hooks/useVisualViewportBounds.test.tsx src/components/Chat/ChatView.test.tsx`

Expected: PASS.

### Task 3: Verify and commit

**Files:**
- Verify all modified frontend files and documentation.

**Step 1: Run TypeScript and production build**

Run: `npx tsc -b && npm run build`

Expected: PASS.

**Step 2: Run the broader chat regression suite**

Run: `npx vitest run src/components/Chat/ChatView.test.tsx src/components/Chat/SharedChatView.test.tsx src/components/Chat/LoopChatView.test.tsx`

Expected: PASS.

**Step 3: Inspect the diff and commit**

Run: `git diff --check && git status --short`

Commit message: `fix(chat): keep mobile composer above keyboard`
