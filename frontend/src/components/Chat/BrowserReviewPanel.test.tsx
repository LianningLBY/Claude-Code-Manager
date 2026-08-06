import { afterEach, describe, expect, it, vi } from 'vitest';
import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react';

import type { BrowserReviewJob, TestHarnessRun, WorkspaceReviewRun } from '../../api/client';
import { BrowserReviewPanel } from './BrowserReviewPanel';

vi.mock('../../api/client', () => ({
  api: {
    listTestRuns: vi.fn(),
    getTestRunEvidence: vi.fn(),
    cancelTestRun: vi.fn(),
    repeatTestRun: vi.fn(),
  },
}));

import { api } from '../../api/client';

const completedJob: BrowserReviewJob = {
  id: 'inline-review-1',
  task_id: 73,
  owner_task_id: 73,
  harness_run_id: 'harness-review-1',
  inline_tool: true,
  status: 'completed',
  stage: 'completed',
  url: 'http://127.0.0.1:5173',
  goal: '检查 Task 页面布局和运行错误',
  provider: 'claude',
  model: 'claude-opus-4-6',
  reasoning_effort: 'medium',
  codex_service_tier: 'default',
  allow_actions: false,
  capture_only: false,
  browser_channel: 'chrome',
  viewport_width: 1440,
  viewport_height: 900,
  max_steps: 20,
  max_actions: 60,
  created_at: '2026-08-05T00:00:00Z',
  started_at: '2026-08-05T00:00:01Z',
  completed_at: '2026-08-05T00:00:05Z',
  error: null,
  response_id: null,
  steps: 3,
  actions: 2,
  latest_screenshot: null,
  telemetry: { page_errors: [{ message: 'render exploded' }] },
  action_batches: [],
  trace: [
    {
      id: 10,
      kind: 'decision',
      title: '模型观察与决策',
      detail: '首屏存在横向溢出，继续检查错误面板。',
      timestamp: '2026-08-05T00:00:02Z',
    },
    {
      id: 11,
      kind: 'tool',
      title: '滚动查看页面',
      detail: '{"delta_y": 600}',
      tool_name: 'browser_scroll',
      timestamp: '2026-08-05T00:00:03Z',
    },
  ],
  verdict: 'failed',
  findings: [],
  coverage: { scenarios: ['primary-flow'] },
  artifacts: ['report.md'],
  report: '# 审查结论\n\n发现一个布局问题。',
};

function makeWorkspaceRun(overrides: Partial<WorkspaceReviewRun> = {}): WorkspaceReviewRun {
  return {
    id: 'workspace-run-default',
    task_id: 73,
    project_id: 4,
    agent_task_id: null,
    browser_review_job_id: null,
    mode: 'review_only',
    profile: 'standard',
    goal: '验证当前分支页面',
    status: 'completed',
    stage: 'completed',
    workspace_path: '/repo',
    git_head: '1234567890abcdef1234567890abcdef12345678',
    workspace_fingerprint: 'abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890',
    preview_config: {
      version: 1,
      name: 'Vite preview',
      setup: [],
      processes: [],
      url: 'http://127.0.0.1:{preview_port}/',
      health_url: 'http://127.0.0.1:{preview_port}/',
      startup_timeout_seconds: 90,
    },
    preview_url: null,
    stale: false,
    report: '# 完成',
    error: null,
    cleanup_status: 'completed',
    cleanup_error: null,
    created_at: '2026-08-06T00:00:00Z',
    started_at: '2026-08-06T00:00:01Z',
    completed_at: '2026-08-06T00:00:05Z',
    ...overrides,
  };
}

function makeHarnessRun(overrides: Partial<TestHarnessRun> = {}): TestHarnessRun {
  const workspace = overrides.workspace_review ?? null;
  const browser = overrides.browser_review ?? null;
  const objective = String(overrides.test_plan?.objective || workspace?.goal || browser?.goal || '验证前端页面');
  return {
    id: 'harness-run-default',
    task_id: 73,
    project_id: workspace?.project_id ?? null,
    workspace_review_run_id: workspace?.id ?? null,
    browser_review_job_id: browser?.id ?? null,
    agent_task_id: workspace?.agent_task_id ?? browser?.task_id ?? null,
    target_kind: workspace ? 'current_workspace' : 'fixed_url',
    target: workspace ? {} : { url: browser?.url || 'http://127.0.0.1:5173' },
    test_plan: { version: 1, objective, scenarios: [] },
    runtime: { context_policy: 'isolated_black_box_v1' },
    request_fingerprint: 'f'.repeat(64),
    parent_run_id: null,
    root_run_id: 'harness-run-default',
    attempt_number: 1,
    status: browser?.status ?? (workspace?.status === 'preparing' ? 'preparing_environment' : workspace?.status ?? 'completed'),
    stage: browser?.stage ?? workspace?.stage ?? 'completed',
    verdict: browser?.verdict ?? null,
    source_git_head: workspace?.git_head ?? null,
    source_fingerprint: workspace?.workspace_fingerprint ?? null,
    stale: workspace?.stale ?? false,
    report: browser?.report ?? workspace?.report ?? null,
    error: browser?.error ?? workspace?.error ?? null,
    cleanup_status: workspace?.cleanup_status ?? (browser && browser.status === 'completed' ? 'completed' : 'pending'),
    cleanup_error: workspace?.cleanup_error ?? null,
    created_at: workspace?.created_at ?? browser?.created_at ?? '2026-08-06T00:00:00Z',
    started_at: workspace?.started_at ?? browser?.started_at ?? null,
    completed_at: workspace?.completed_at ?? browser?.completed_at ?? null,
    attempts: [],
    events: (browser?.trace || []).map((event, index) => ({
      id: event.id,
      sequence: index + 1,
      event_type: event.kind,
      stage: browser?.stage ?? null,
      title: event.title,
      detail: event.detail,
      data: { tool_name: event.tool_name },
      created_at: event.timestamp || '2026-08-06T00:00:00Z',
    })),
    evidence: (browser?.artifacts || []).map((name, index) => ({
      id: `evidence-${index}`,
      kind: name.endsWith('.png') ? 'screenshot' : 'report',
      name,
      content_type: name.endsWith('.png') ? 'image/png' : 'text/markdown',
      sha256: 'a'.repeat(64),
      byte_size: 10,
      metadata: {},
      created_at: '2026-08-06T00:00:00Z',
    })),
    findings: browser?.findings || [],
    workspace_review: workspace,
    browser_review: browser,
    ...overrides,
  };
}

afterEach(() => {
  cleanup();
  vi.clearAllMocks();
  vi.mocked(api.listTestRuns).mockResolvedValue([]);
  localStorage.clear();
});

describe('BrowserReviewPanel', () => {
  it('shows durable workspace preparation, fingerprint, and cancellation before the browser job exists', async () => {
    const workspaceRun: WorkspaceReviewRun = {
      id: 'workspace-run-1',
      task_id: 73,
      project_id: 4,
      agent_task_id: null,
      browser_review_job_id: null,
      mode: 'review_only',
      profile: 'standard',
      goal: '验证当前分支的设置页',
      status: 'preparing',
      stage: 'starting_preview',
      workspace_path: '/repo',
      git_head: '1234567890abcdef1234567890abcdef12345678',
      workspace_fingerprint: 'abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890',
      preview_config: {
        version: 1,
        name: 'Vite preview',
        setup: [],
        processes: [],
        url: 'http://127.0.0.1:{preview_port}/',
        health_url: 'http://127.0.0.1:{preview_port}/',
        startup_timeout_seconds: 90,
      },
      preview_url: null,
      stale: false,
      report: null,
      error: null,
      cleanup_status: 'pending',
      cleanup_error: null,
      created_at: '2026-08-06T00:00:00Z',
      started_at: '2026-08-06T00:00:01Z',
      completed_at: null,
    };
    const harnessRun = makeHarnessRun({
      id: 'harness-workspace-1',
      root_run_id: 'harness-workspace-1',
      status: 'preparing_environment',
      stage: 'starting_preview',
      workspace_review: workspaceRun,
    });
    const cancelled = makeHarnessRun({
      ...harnessRun,
      status: 'cancelled',
      stage: 'cancelled',
      cleanup_status: 'completed',
      workspace_review: { ...workspaceRun, status: 'cancelled', stage: 'cancelled', cleanup_status: 'completed' },
    });
    vi.mocked(api.listTestRuns)
      .mockResolvedValueOnce([harnessRun])
      .mockResolvedValue([cancelled]);
    vi.mocked(api.cancelTestRun).mockResolvedValue(cancelled);

    render(
      <BrowserReviewPanel
        taskId={73}
        taskActive={false}
        open
        displayMode="docked"
        onAvailableChange={vi.fn()}
        onClose={vi.fn()}
        onDisplayModeChange={vi.fn()}
        onNewReview={vi.fn()}
      />,
    );

    expect(await screen.findByText('当前分支黑盒测试')).toBeInTheDocument();
    expect(screen.getAllByText('正在启动隔离预览').length).toBeGreaterThan(0);
    expect(screen.getAllByText(/HEAD 1234567890/).length).toBeGreaterThan(0);
    fireEvent.click(screen.getByRole('button', { name: 'Stop test run' }));
    await waitFor(() => expect(api.cancelTestRun).toHaveBeenCalledWith(73, harnessRun.id));
  });

  it('shows same-Task progress, trace, telemetry, and report', async () => {
    vi.mocked(api.listTestRuns).mockResolvedValue([
      makeHarnessRun({ id: 'harness-review-1', root_run_id: 'harness-review-1', browser_review: completedJob }),
    ]);
    const onAvailableChange = vi.fn();

    render(
      <BrowserReviewPanel
        taskId={73}
        taskActive={false}
        open
        displayMode="docked"
        onAvailableChange={onAvailableChange}
        onClose={vi.fn()}
        onDisplayModeChange={vi.fn()}
        onNewReview={vi.fn()}
        goalProgress={{ turn: 1, maxTurns: 5, lastReason: '还需要复查窄屏页面', active: true }}
      />,
    );

    expect(await screen.findByText('前端运行审查')).toBeInTheDocument();
    expect(screen.getByText('模型观察与操作轨迹')).toBeInTheDocument();
    expect(screen.getByText(/首屏存在横向溢出/)).toBeInTheDocument();
    expect(screen.getByText('page errors: 1')).toBeInTheDocument();
    expect(screen.getByText('审查结论')).toBeInTheDocument();
    expect(screen.getByText('Goal 循环审查 · 模型自动判断')).toBeInTheDocument();
    expect(screen.getByText(/还需要复查窄屏页面/)).toBeInTheDocument();
    await waitFor(() => expect(onAvailableChange).toHaveBeenCalledWith(true));
  });

  it('stays hidden until the task has called the review tool', async () => {
    vi.mocked(api.listTestRuns).mockResolvedValue([]);
    const onAvailableChange = vi.fn();

    const { container } = render(
      <BrowserReviewPanel
        taskId={73}
        taskActive={false}
        open
        displayMode="docked"
        onAvailableChange={onAvailableChange}
        onClose={vi.fn()}
        onDisplayModeChange={vi.fn()}
        onNewReview={vi.fn()}
      />,
    );

    await waitFor(() => expect(onAvailableChange).toHaveBeenCalledWith(false));
    expect(container).toBeEmptyDOMElement();
  });

  it('supports floating, minimizing, restoring, and docking the review window', async () => {
    const runningJob: BrowserReviewJob = {
      ...completedJob,
      status: 'running',
      stage: 'executing_actions',
      completed_at: null,
      latest_screenshot: 'step-02.png',
      report: null,
    };
    const runningRun = makeHarnessRun({
      id: 'harness-running',
      root_run_id: 'harness-running',
      status: 'running',
      stage: 'executing_actions',
      browser_review: runningJob,
      evidence: [{
        id: 'evidence-screenshot',
        kind: 'screenshot',
        name: 'step-02.png',
        content_type: 'image/png',
        sha256: 'a'.repeat(64),
        byte_size: 10,
        metadata: {},
        created_at: '2026-08-05T00:00:02Z',
      }],
    });
    vi.mocked(api.listTestRuns).mockResolvedValue([runningRun]);
    vi.mocked(api.getTestRunEvidence).mockResolvedValue(new Blob(['screenshot'], { type: 'image/png' }));
    Object.defineProperty(URL, 'createObjectURL', {
      configurable: true,
      value: vi.fn(() => 'blob:frontend-review-screenshot'),
    });
    Object.defineProperty(URL, 'revokeObjectURL', {
      configurable: true,
      value: vi.fn(),
    });
    const onDisplayModeChange = vi.fn();

    render(
      <BrowserReviewPanel
        taskId={73}
        taskActive={false}
        open
        displayMode="floating"
        onAvailableChange={vi.fn()}
        onClose={vi.fn()}
        onDisplayModeChange={onDisplayModeChange}
        onNewReview={vi.fn()}
      />,
    );

    const panel = await screen.findByLabelText('Frontend Review progress');
    expect(panel).toHaveAttribute('data-display-mode', 'floating');
    expect(screen.getAllByText('正在验证页面状态').length).toBeGreaterThan(0);
    expect(await screen.findByAltText('Latest frontend review screenshot')).toHaveAttribute(
      'src',
      'blob:frontend-review-screenshot',
    );
    expect(screen.getByText('模型观察与操作轨迹')).toBeInTheDocument();

    const dragHandle = panel.querySelector('[data-floating-drag-handle="true"]');
    expect(dragHandle).not.toBeNull();
    vi.spyOn(panel, 'getBoundingClientRect').mockReturnValue({
      x: 100,
      y: 100,
      left: 100,
      top: 100,
      right: 530,
      bottom: 700,
      width: 430,
      height: 600,
      toJSON: () => ({}),
    });
    fireEvent.pointerDown(dragHandle!, { button: 0, clientX: 120, clientY: 120 });
    fireEvent.pointerMove(window, { clientX: 300, clientY: 260 });
    fireEvent.pointerUp(window);
    await waitFor(() => {
      expect(panel).toHaveStyle({ left: '280px', top: '240px' });
    });

    fireEvent.click(screen.getByRole('button', { name: 'Minimize Frontend Review window' }));
    expect(screen.queryByText('模型观察与操作轨迹')).not.toBeInTheDocument();
    expect(screen.getByText('前端运行审查')).toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: 'Restore Frontend Review window' }));
    expect(screen.getByText('模型观察与操作轨迹')).toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: 'Dock Frontend Review panel' }));
    expect(onDisplayModeChange).toHaveBeenCalledWith('docked');
  });

  it('stops an active review from the Task progress panel', async () => {
    const runningJob: BrowserReviewJob = {
      ...completedJob,
      status: 'running',
      stage: 'executing_actions',
      completed_at: null,
      report: null,
    };
    const runningRun = makeHarnessRun({
      id: 'harness-running-stop',
      root_run_id: 'harness-running-stop',
      status: 'running',
      stage: 'executing_actions',
      browser_review: runningJob,
    });
    const cancelledRun = makeHarnessRun({
      ...runningRun,
      status: 'cancelled',
      stage: 'cancelled',
      completed_at: '2026-08-05T00:00:06Z',
      browser_review: { ...runningJob, status: 'cancelled', stage: 'cancelled', completed_at: '2026-08-05T00:00:06Z' },
    });
    vi.mocked(api.listTestRuns).mockResolvedValue([runningRun]);
    vi.mocked(api.cancelTestRun).mockResolvedValue(cancelledRun);

    render(
      <BrowserReviewPanel
        taskId={73}
        taskActive
        open
        displayMode="docked"
        onAvailableChange={vi.fn()}
        onClose={vi.fn()}
        onDisplayModeChange={vi.fn()}
        onNewReview={vi.fn()}
      />,
    );

    fireEvent.click(await screen.findByRole('button', { name: 'Stop test run' }));

    await waitFor(() => expect(api.cancelTestRun).toHaveBeenCalledWith(73, runningRun.id));
    expect((await screen.findAllByText('已停止')).length).toBeGreaterThan(0);
    expect(screen.queryByRole('button', { name: 'Stop test run' })).not.toBeInTheDocument();
  });

  it('repeats a terminal harness run and switches to the new run', async () => {
    const completedRun = makeHarnessRun({
      id: 'harness-repeat-source',
      root_run_id: 'harness-repeat-source',
      browser_review: completedJob,
    });
    const repeatedRun = makeHarnessRun({
      id: 'harness-repeat-next',
      root_run_id: 'harness-repeat-source',
      parent_run_id: completedRun.id,
      attempt_number: 2,
      status: 'queued',
      stage: 'waiting_for_browser',
      test_plan: { version: 1, objective: '第二轮前端复查', scenarios: [] },
      browser_review: null,
    });
    vi.mocked(api.listTestRuns).mockResolvedValue([completedRun]);
    vi.mocked(api.repeatTestRun).mockResolvedValue(repeatedRun);
    const onNewReview = vi.fn();

    render(
      <BrowserReviewPanel
        taskId={73}
        taskActive={false}
        open
        displayMode="docked"
        onAvailableChange={vi.fn()}
        onClose={vi.fn()}
        onDisplayModeChange={vi.fn()}
        onNewReview={onNewReview}
      />,
    );

    fireEvent.click(await screen.findByRole('button', { name: 'Repeat test run' }));

    await waitFor(() => expect(api.repeatTestRun).toHaveBeenCalledWith(73, completedRun.id));
    expect(await screen.findByText('第二轮前端复查')).toBeInTheDocument();
    expect(onNewReview).toHaveBeenCalledTimes(1);
  });

  it('automatically switches to a newly started review in the same task', async () => {
    const olderJob: BrowserReviewJob = {
      ...completedJob,
      id: 'inline-review-older',
      goal: '历史审查页面',
      created_at: '2026-08-04T23:00:00Z',
    };
    const newJob: BrowserReviewJob = {
      ...completedJob,
      id: 'inline-review-new',
      status: 'running',
      stage: 'browser_ready',
      goal: '新的审查页面',
      created_at: '2026-08-05T01:00:00Z',
      completed_at: null,
      report: null,
      trace: [],
    };
    const completedRun = makeHarnessRun({ id: 'harness-completed', root_run_id: 'harness-completed', browser_review: completedJob });
    const olderRun = makeHarnessRun({ id: 'harness-older', root_run_id: 'harness-older', browser_review: olderJob });
    const newRun = makeHarnessRun({ id: 'harness-new', root_run_id: 'harness-new', status: 'running', stage: 'browser_ready', browser_review: newJob });
    vi.mocked(api.listTestRuns)
      .mockResolvedValueOnce([completedRun, olderRun])
      .mockResolvedValueOnce([newRun, completedRun, olderRun]);
    const onNewReview = vi.fn();

    render(
      <BrowserReviewPanel
        taskId={73}
        taskActive={false}
        open
        displayMode="docked"
        onAvailableChange={vi.fn()}
        onClose={vi.fn()}
        onDisplayModeChange={vi.fn()}
        onNewReview={onNewReview}
      />,
    );

    const reviewPicker = await screen.findByRole('combobox');
    fireEvent.change(reviewPicker, { target: { value: olderRun.id } });
    expect(screen.getAllByText('历史审查页面').length).toBeGreaterThan(0);

    fireEvent.click(screen.getByTitle('刷新审查进度'));
    expect((await screen.findAllByText('新的审查页面')).length).toBeGreaterThan(0);
    expect(reviewPicker).toHaveValue(newRun.id);
    expect(onNewReview).toHaveBeenCalledTimes(1);
  });

  it('shows ordinary-chat expectation and switches from its exact baseline to the new workspace run', async () => {
    const oldRun = makeWorkspaceRun({
      id: 'workspace-run-old',
      goal: '历史前端验收',
    });
    const newRun = makeWorkspaceRun({
      id: 'workspace-run-new',
      goal: '本轮 PR99 前端验收',
      status: 'preparing',
      stage: 'starting_preview',
      report: null,
      cleanup_status: 'pending',
      created_at: '2026-08-06T01:00:00Z',
      completed_at: null,
    });
    const oldHarnessRun = makeHarnessRun({ id: 'harness-old', root_run_id: 'harness-old', workspace_review: oldRun });
    const newHarnessRun = makeHarnessRun({
      id: 'harness-new-pr99',
      root_run_id: 'harness-new-pr99',
      status: 'preparing_environment',
      stage: 'starting_preview',
      workspace_review: newRun,
    });
    vi.mocked(api.listTestRuns)
      .mockResolvedValueOnce([oldHarnessRun])
      .mockResolvedValueOnce([oldHarnessRun])
      .mockResolvedValue([newHarnessRun, oldHarnessRun]);
    const onExpectedWorkspaceReviewFound = vi.fn();
    const onNewReview = vi.fn();

    render(
      <BrowserReviewPanel
        taskId={73}
        taskActive
        open
        displayMode="docked"
        onAvailableChange={vi.fn()}
        onClose={vi.fn()}
        onDisplayModeChange={vi.fn()}
        onNewReview={onNewReview}
        expectedWorkspaceReviewBaseline={oldHarnessRun.id}
        onExpectedWorkspaceReviewFound={onExpectedWorkspaceReviewFound}
      />,
    );

    expect(await screen.findByText('等待父 Agent 调用浏览器审查工具')).toBeInTheDocument();
    fireEvent.click(screen.getByTitle('刷新审查进度'));

    expect((await screen.findAllByText('本轮 PR99 前端验收')).length).toBeGreaterThan(0);
    expect(onExpectedWorkspaceReviewFound).toHaveBeenCalledTimes(1);
    expect(onNewReview).toHaveBeenCalledTimes(1);
  });
});
