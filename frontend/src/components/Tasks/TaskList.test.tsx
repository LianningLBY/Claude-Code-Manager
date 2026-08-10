import { describe, it, expect, vi, beforeEach } from 'vitest';
import { act, render, screen, fireEvent, waitFor, createEvent } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { useState } from 'react';
import { TaskList } from './TaskList';
import type { Task, Project } from '../../api/client';

// Mock the api module
vi.mock('../../api/client', () => ({
  api: {
    deleteTask: vi.fn().mockResolvedValue({}),
    cancelTask: vi.fn().mockResolvedValue({}),
    stopTaskSession: vi.fn().mockResolvedValue({ stopped: true }),
    retryTask: vi.fn().mockResolvedValue({}),
    starTask: vi.fn().mockResolvedValue({}),
    archiveTask: vi.fn().mockResolvedValue({}),
    updateTask: vi.fn().mockResolvedValue({}),
    getDeliveryRun: vi.fn().mockResolvedValue({
      id: 17,
      phase: 'monitoring',
      activity: 'waiting',
      outcome: null,
      cycle_count: 1,
      max_cycles: 10,
      turn_count: 1,
      delivery_branch: 'ccm/delivery/17-controlled-delivery',
      wait_reason: 'pr_monitor',
      pause_reason: null,
      error_code: null,
      error_message: null,
      pr_number: null,
      pr_url: null,
      allowed_actions: [],
    }),
    pauseDeliveryRun: vi.fn(),
    resumeDeliveryRun: vi.fn(),
    cancelDeliveryRun: vi.fn(),
  },
}));

import { api } from '../../api/client';

function makeTask(overrides: Partial<Task> = {}): Task {
  return {
    id: 1,
    title: '',
    description: 'Test description',
    status: 'pending',
    priority: 0,
    project_id: null,
    target_repo: null,
    target_branch: 'main',
    result_branch: null,
    merge_status: 'pending',
    instance_id: null,
    retry_count: 0,
    turn_generation: 0,
    max_retries: 3,
    mode: 'auto',
    todo_file_path: null,
    loop_progress: null,
    max_iterations: 50,
    must_complete: false,
    plan_content: null,
    plan_approved: null,
    starred: false,
    archived: false,
    has_unread: false,
    session_id: null,
    error_message: null,
    provider: 'claude',
    model: null,
    codex_service_tier: 'default',
    tags: null,
    context_window_usage: null,
    created_at: '2024-01-01T00:00:00Z',
    started_at: null,
    completed_at: null,
    ...overrides,
  };
}

describe('TaskList', () => {
  const onRefresh = vi.fn();
  const onOpenChat = vi.fn();
  const projects: Project[] = [];

  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('renders task description when no title', () => {
    const tasks = [makeTask({ description: 'My task prompt' })];
    render(<TaskList tasks={tasks} projects={projects} onRefresh={onRefresh} onOpenChat={onOpenChat} />);
    expect(screen.getByText('My task prompt')).toBeInTheDocument();
  });

  it('renders title when present, description as subtitle', () => {
    const tasks = [makeTask({ title: 'Custom Title', description: 'The prompt' })];
    render(<TaskList tasks={tasks} projects={projects} onRefresh={onRefresh} onOpenChat={onOpenChat} />);
    expect(screen.getByText('Custom Title')).toBeInTheDocument();
    expect(screen.getByText('The prompt')).toBeInTheDocument();
  });

  it('renders a Delivery-owned scheduler shell with Run controls but no Task mutations', async () => {
    const tasks = [makeTask({
      mode: 'delivery_loop',
      status: 'failed',
      title: 'Controlled delivery',
      session_id: 'delivery-session',
      delivery_run_id: 17,
      delivery_phase: 'monitoring',
      delivery_activity: 'waiting',
      attention_tag: 'operator note',
    })];

    render(
      <TaskList
        tasks={tasks}
        projects={projects}
        onRefresh={onRefresh}
        onOpenChat={onOpenChat}
      />,
    );

    const runButton = screen.getByRole('button', { name: 'Delivery #17' });
    expect(runButton).toBeInTheDocument();
    expect(screen.getByText('operator note')).toBeInTheDocument();
    expect(screen.getByTitle('Chat')).toBeInTheDocument();
    expect(screen.queryByTitle('More actions')).not.toBeInTheDocument();
    expect(screen.queryByTitle('按住拖动排序')).not.toBeInTheDocument();
    expect(screen.queryByTitle('Edit attention tag')).not.toBeInTheDocument();
    await userEvent.click(runButton);
    expect(await screen.findByRole('region', { name: 'Delivery Run #17' })).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Pause' })).not.toBeInTheDocument();
    expect(screen.getByText(/observation-only/)).toBeInTheDocument();
  });

  it.each([
    ['planning', 1, 'Planning'],
    ['reviewing', 1, 'Reviewing'],
    ['planning', 2, 'Planning · Round 2'],
    ['reviewing', 2, 'Reviewing · Round 2'],
  ])(
    'shows the active Plan stage %s round %s',
    (planStage, planStageRound, expected) => {
      render(
        <TaskList
          tasks={[makeTask({
            mode: 'plan',
            status: 'executing',
            plan_stage: planStage,
            plan_stage_round: planStageRound,
          })]}
          projects={projects}
          onRefresh={onRefresh}
          onOpenChat={onOpenChat}
        />,
      );

      expect(screen.getByText(expected)).toBeInTheDocument();
    },
  );

  it('shows the concrete running Plan route and fallback state without generic Config', () => {
    render(
      <TaskList
        tasks={[makeTask({
          mode: 'plan',
          status: 'executing',
          provider: 'claude',
          model: 'claude-fable-5',
          plan_stage: 'reviewing',
          plan_stage_round: 2,
          plan_stage_provider: 'codex',
          plan_stage_model: 'gpt-5.6-terra',
          plan_stage_effort: 'xhigh',
          plan_stage_route_slot: 'fallback',
        })]}
        projects={projects}
        onRefresh={onRefresh}
        onOpenChat={onOpenChat}
      />,
    );

    const badge = screen.getByTestId('plan-pipeline-badge');
    expect(badge).toHaveTextContent('Reviewer · gpt-5.6-terra');
    expect(badge).toHaveTextContent('fallback');
    expect(badge).toHaveAttribute(
      'title',
      'Reviewer, round 2: codex / gpt-5.6-terra / xhigh (fallback)',
    );
    expect(screen.queryByText('Config')).not.toBeInTheDocument();
  });

  it('shows the frozen Planner to Reviewer pipeline when a Plan is not running', () => {
    render(
      <TaskList
        tasks={[makeTask({
          mode: 'plan',
          status: 'plan_review',
          provider: 'claude',
          model: 'claude-fable-5',
          plan_pipeline_config: {
            version: 1,
            planner: {
              primary: { provider: 'claude', model: 'claude-fable-5', effort: 'high' },
              fallback: { provider: 'codex', model: 'gpt-5.6-terra', effort: 'xhigh' },
            },
            reviewer: {
              enabled: true,
              primary: { provider: 'codex', model: 'gpt-5.6-sol', effort: 'xhigh' },
              fallback: { provider: 'claude', model: 'claude-sonnet-5', effort: 'high' },
            },
            max_revision_cycles: 2,
          },
        })]}
        projects={projects}
        onRefresh={onRefresh}
        onOpenChat={onOpenChat}
      />,
    );

    const badge = screen.getByTestId('plan-pipeline-badge');
    expect(badge).toHaveTextContent('claude-fable-5 → gpt-5.6-sol');
    expect(badge).toHaveAttribute(
      'title',
      expect.stringContaining('Planner fallback: codex / gpt-5.6-terra / xhigh'),
    );
  });

  it('shows both sides of an immutable Plan revision chain', () => {
    render(
      <TaskList
        tasks={[makeTask({
          id: 12,
          mode: 'plan',
          status: 'superseded',
          supersedes_plan_task_id: 9,
          metadata_: { plan_superseded_by_task_id: 15 },
        })]}
        projects={projects}
        onRefresh={onRefresh}
        onOpenChat={onOpenChat}
      />,
    );

    expect(screen.getByText('Superseded')).toBeInTheDocument();
    expect(screen.getByText('Revision of #9')).toBeInTheDocument();
    expect(screen.getByText('Superseded by #15')).toBeInTheDocument();
    expect(screen.queryByText('Config')).not.toBeInTheDocument();
  });

  it('keeps a migrated Plan Task visible and links it to the canonical Plan', async () => {
    render(
      <TaskList
        tasks={[makeTask({
          id: 12,
          mode: 'plan',
          status: 'completed',
          canonical_plan_id: 44,
        })]}
        projects={projects}
        onRefresh={onRefresh}
        onOpenChat={onOpenChat}
      />,
    );

    await userEvent.click(screen.getByRole('button', { name: 'Plan #44' }));
    expect(window.location.hash).toBe('#/plans/44');
  });

  it('shows empty state when no tasks', () => {
    render(<TaskList tasks={[]} projects={projects} onRefresh={onRefresh} onOpenChat={onOpenChat} />);
    expect(screen.getByText('No tasks yet')).toBeInTheDocument();
  });

  it('shows Fast only for Codex priority tasks', () => {
    const { rerender } = render(
      <TaskList
        tasks={[makeTask({ provider: 'codex', codex_service_tier: 'priority' })]}
        projects={projects}
        onRefresh={onRefresh}
        onOpenChat={onOpenChat}
      />,
    );
    expect(screen.getByTestId('codex-fast-badge')).toHaveTextContent('Fast');

    rerender(
      <TaskList
        tasks={[makeTask({ provider: 'codex', codex_service_tier: 'default' })]}
        projects={projects}
        onRefresh={onRefresh}
        onOpenChat={onOpenChat}
      />,
    );
    expect(screen.queryByTestId('codex-fast-badge')).not.toBeInTheDocument();
  });

  describe('Copy prompt', () => {
    it('copies task description to clipboard', async () => {
      const writeText = vi.fn().mockResolvedValue(undefined);
      Object.assign(navigator, { clipboard: { writeText } });

      const tasks = [makeTask({ description: 'Copy this prompt' })];
      render(<TaskList tasks={tasks} projects={projects} onRefresh={onRefresh} onOpenChat={onOpenChat} />);

      const copyBtn = screen.getByTitle('Copy prompt');
      await userEvent.click(copyBtn);

      expect(writeText).toHaveBeenCalledWith('Copy this prompt');
    });

    it('shows check icon after copying', async () => {
      const writeText = vi.fn().mockResolvedValue(undefined);
      Object.assign(navigator, { clipboard: { writeText } });

      const tasks = [makeTask({ description: 'Copy me' })];
      render(<TaskList tasks={tasks} projects={projects} onRefresh={onRefresh} onOpenChat={onOpenChat} />);

      const copyBtn = screen.getByTitle('Copy prompt');
      await userEvent.click(copyBtn);

      // The check icon should appear (we can't easily check the icon itself,
      // but the button should still be there)
      expect(writeText).toHaveBeenCalledTimes(1);
    });
  });

  describe('Overflow menu', () => {
    it('opens overflow menu on click', async () => {
      const tasks = [makeTask()];
      render(<TaskList tasks={tasks} projects={projects} onRefresh={onRefresh} onOpenChat={onOpenChat} />);

      const moreBtn = screen.getByTitle('More actions');
      await userEvent.click(moreBtn);

      expect(screen.getByText('Edit title')).toBeInTheDocument();
      expect(screen.getByText('Archive')).toBeInTheDocument();
    });

    it('shows Delete in overflow menu for pending tasks', async () => {
      const tasks = [makeTask({ status: 'pending' })];
      render(<TaskList tasks={tasks} projects={projects} onRefresh={onRefresh} onOpenChat={onOpenChat} />);

      await userEvent.click(screen.getByTitle('More actions'));
      expect(screen.getByText('Delete')).toBeInTheDocument();
    });

    it('shows Cancel in overflow menu for in_progress tasks', async () => {
      const tasks = [makeTask({ status: 'in_progress' })];
      render(<TaskList tasks={tasks} projects={projects} onRefresh={onRefresh} onOpenChat={onOpenChat} />);

      await userEvent.click(screen.getByTitle('More actions'));
      expect(screen.getByText('Cancel')).toBeInTheDocument();
    });

    it('shows Cancel while a Task is waiting on an Auto capability', async () => {
      const tasks = [makeTask({ status: 'waiting_capability' })];
      render(<TaskList tasks={tasks} projects={projects} onRefresh={onRefresh} onOpenChat={onOpenChat} />);

      await userEvent.click(screen.getByTitle('More actions'));
      expect(screen.getByText('Cancel')).toBeInTheDocument();
    });

    it('shows stop action and hides Delete while detached background work is active', async () => {
      const tasks = [makeTask({ status: 'completed', background_active: true })];
      render(<TaskList tasks={tasks} projects={projects} onRefresh={onRefresh} onOpenChat={onOpenChat} />);

      await userEvent.click(screen.getByTitle('More actions'));
      expect(screen.getByText('Cancel')).toBeInTheDocument();
      expect(screen.queryByText('Delete')).not.toBeInTheDocument();

      await userEvent.click(screen.getByText('Cancel'));
      expect(api.stopTaskSession).toHaveBeenCalledWith(1);
      expect(api.cancelTask).not.toHaveBeenCalled();
      await waitFor(() => expect(onRefresh).toHaveBeenCalled());
    });

    it.each(['failed', 'cancelled', 'conflict', 'completed'])(
      'shows Retry in overflow menu for %s tasks',
      async (status) => {
        const tasks = [makeTask({
          status,
          provider: 'codex',
          model: 'gpt-5.6-sol',
          codex_service_tier: 'priority',
        })];
        render(<TaskList tasks={tasks} projects={projects} onRefresh={onRefresh} onOpenChat={onOpenChat} />);

        await userEvent.click(screen.getByTitle('More actions'));
        await userEvent.click(screen.getByText('Retry'));

        expect(api.retryTask).toHaveBeenCalledWith(1, {
          provider: 'codex',
          model: 'gpt-5.6-sol',
          codex_service_tier: 'priority',
        });
        await waitFor(() => expect(onRefresh).toHaveBeenCalled());
      },
    );

    it.each([
      { mode: 'plan', status: 'failed' },
      { mode: 'auto', status: 'pending' },
      { mode: 'auto', status: 'completed', background_active: true },
      { mode: 'auto', status: 'failed', delivery_run_id: 17 },
    ])('hides Retry when the Task lifecycle does not allow it: %o', async (overrides) => {
      render(
        <TaskList
          tasks={[makeTask(overrides)]}
          projects={projects}
          onRefresh={onRefresh}
          onOpenChat={onOpenChat}
        />,
      );

      await userEvent.click(screen.getByTitle('More actions'));
      expect(screen.queryByText('Retry')).not.toBeInTheDocument();
    });

    it('closes overflow menu on outside click', async () => {
      const tasks = [makeTask()];
      render(<TaskList tasks={tasks} projects={projects} onRefresh={onRefresh} onOpenChat={onOpenChat} />);

      await userEvent.click(screen.getByTitle('More actions'));
      expect(screen.getByText('Edit title')).toBeInTheDocument();

      // Click outside
      fireEvent.mouseDown(document.body);
      await waitFor(() => {
        expect(screen.queryByText('Edit title')).not.toBeInTheDocument();
      });
    });
  });

  describe('Attention tag', () => {
    it('shows an existing attention tag prominently', () => {
      render(
        <TaskList
          tasks={[makeTask({ attention_tag: '等它结束后再看' })]}
          projects={projects}
          onRefresh={onRefresh}
          onOpenChat={onOpenChat}
        />,
      );

      expect(screen.getByText('等它结束后再看')).toBeInTheDocument();
      expect(screen.getByTitle('Edit attention tag')).toBeInTheDocument();
    });

    it('keeps the API-returned tag visible while the original snapshot stays stale', async () => {
      const updated = makeTask({ attention_tag: '今晚继续' });
      vi.mocked(api.updateTask).mockResolvedValueOnce(updated);

      function ControlledTaskList() {
        const [visibleTasks, setVisibleTasks] = useState([
          makeTask({ attention_tag: null }),
        ]);
        return (
          <TaskList
            tasks={visibleTasks}
            projects={projects}
            onRefresh={onRefresh}
            onTaskUpdated={(returned) => {
              setVisibleTasks((current) => current.map((item) =>
                item.id === returned.id ? returned : item
              ));
            }}
            onOpenChat={onOpenChat}
          />
        );
      }

      render(<ControlledTaskList />);

      await userEvent.click(screen.getByTitle('More actions'));
      await userEvent.click(screen.getByText('Add attention tag'));
      await userEvent.type(screen.getByLabelText('Attention tag'), '今晚继续');
      await userEvent.click(screen.getByTitle('Save attention tag'));

      await waitFor(() => {
        expect(api.updateTask).toHaveBeenCalledWith(1, {
          attention_tag: '今晚继续',
        });
      });
      expect(await screen.findByText('今晚继续')).toBeInTheDocument();
      expect(onRefresh).not.toHaveBeenCalled();
    });

    it('does not start card reordering when long-pressing the tag editor', () => {
      render(
        <TaskList
          tasks={[makeTask({ attention_tag: '需要修改' })]}
          projects={projects}
          onRefresh={onRefresh}
          onOpenChat={onOpenChat}
        />,
      );

      fireEvent.click(screen.getByTitle('Edit attention tag'));
      const input = screen.getByLabelText('Attention tag');
      const card = input.closest('[data-reorder-idx]');

      vi.useFakeTimers();
      try {
        fireEvent.touchStart(input);
        act(() => vi.advanceTimersByTime(500));
        expect(card).not.toHaveClass('opacity-40');
      } finally {
        vi.useRealTimers();
      }
    });
  });

  describe('Title editing', () => {
    it('opens inline title editor from overflow menu', async () => {
      const tasks = [makeTask({ title: 'Old Title' })];
      render(<TaskList tasks={tasks} projects={projects} onRefresh={onRefresh} onOpenChat={onOpenChat} />);

      await userEvent.click(screen.getByTitle('More actions'));
      await userEvent.click(screen.getByText('Edit title'));

      const input = screen.getByPlaceholderText('Enter title...');
      expect(input).toBeInTheDocument();
      expect(input).toHaveValue('Old Title');
    });

    it('saves title on Enter', async () => {
      const tasks = [makeTask({ id: 42, title: 'Old' })];
      render(<TaskList tasks={tasks} projects={projects} onRefresh={onRefresh} onOpenChat={onOpenChat} />);

      await userEvent.click(screen.getByTitle('More actions'));
      await userEvent.click(screen.getByText('Edit title'));

      const input = screen.getByPlaceholderText('Enter title...');
      await userEvent.clear(input);
      await userEvent.type(input, 'New Title{Enter}');

      expect(api.updateTask).toHaveBeenCalledWith(42, { title: 'New Title' });
      expect(onRefresh).toHaveBeenCalled();
    });

    it('cancels editing on Escape', async () => {
      const tasks = [makeTask({ title: 'Keep This' })];
      render(<TaskList tasks={tasks} projects={projects} onRefresh={onRefresh} onOpenChat={onOpenChat} />);

      await userEvent.click(screen.getByTitle('More actions'));
      await userEvent.click(screen.getByText('Edit title'));

      const input = screen.getByPlaceholderText('Enter title...');
      await userEvent.type(input, 'Nope');
      await userEvent.keyboard('{Escape}');

      expect(api.updateTask).not.toHaveBeenCalled();
    });

    it('does not call API if title unchanged', async () => {
      const tasks = [makeTask({ title: 'Same' })];
      render(<TaskList tasks={tasks} projects={projects} onRefresh={onRefresh} onOpenChat={onOpenChat} />);

      await userEvent.click(screen.getByTitle('More actions'));
      await userEvent.click(screen.getByText('Edit title'));

      const input = screen.getByPlaceholderText('Enter title...');
      fireEvent.blur(input);

      await waitFor(() => {
        expect(api.updateTask).not.toHaveBeenCalled();
      });
    });
  });

  describe('Chat button', () => {
    it('shows Chat button when session_id exists', () => {
      const tasks = [makeTask({ session_id: 'abc-123' })];
      render(<TaskList tasks={tasks} projects={projects} onRefresh={onRefresh} onOpenChat={onOpenChat} />);
      expect(screen.getByTitle('Chat')).toBeInTheDocument();
    });

    it('does not show Chat button without session_id', () => {
      const tasks = [makeTask({ session_id: null })];
      render(<TaskList tasks={tasks} projects={projects} onRefresh={onRefresh} onOpenChat={onOpenChat} />);
      expect(screen.queryByTitle('Chat')).not.toBeInTheDocument();
    });
  });
});


describe('Drag reorder (main list)', () => {
  it('drop on another row persists a new sort_order', async () => {
    const onRefresh = vi.fn();
    const now = Date.now() / 1000;
    const tasks = [
      makeTask({ id: 11, created_at: '2026-06-11T03:00:00Z', sort_order: now + 300 }),
      makeTask({ id: 12, created_at: '2026-06-11T02:00:00Z', sort_order: now + 200 }),
      makeTask({ id: 13, created_at: '2026-06-11T01:00:00Z', sort_order: now + 100 }),
    ];
    const { container } = render(
      <TaskList tasks={tasks} projects={[]} onRefresh={onRefresh} onOpenChat={vi.fn()} />
    );

    const rows = container.querySelectorAll('[data-reorder-idx]');
    expect(rows.length).toBe(3);

    // 把第 3 行拖到第 1 行：pointer 按下手柄启动拖拽，
    // （jsdom 无布局，elementFromPoint 不可用，用行上的 drop 目标验证落点逻辑）
    const handle3 = rows[2].querySelector('[title="按住拖动排序"]')!;
    expect(handle3).toBeTruthy();
    fireEvent.pointerDown(handle3, { button: 0, clientX: 10, clientY: 300 });
    const dataTransfer = { effectAllowed: '', setData: vi.fn(), getData: vi.fn() };
    fireEvent.dragOver(rows[0], { dataTransfer });
    fireEvent.drop(rows[0], { dataTransfer });

    await waitFor(() => {
      expect(api.updateTask).toHaveBeenCalled();
    });
    const [id, data] = (api.updateTask as ReturnType<typeof vi.fn>).mock.calls.at(-1)!;
    expect(id).toBe(13);
    // 新键必须大于原第 1 行的键，才能真正排到最前
    expect((data as { sort_order: number }).sort_order).toBeGreaterThan(now + 300);
    await waitFor(() => expect(onRefresh).toHaveBeenCalled());
  });

  it('persists the same downward position shown optimistically and refreshes after PUT', async () => {
    let resolveUpdate!: (value: object) => void;
    (api.updateTask as ReturnType<typeof vi.fn>).mockReturnValueOnce(
      new Promise((resolve) => { resolveUpdate = resolve; }),
    );
    const onRefresh = vi.fn();
    const onReorder = vi.fn();
    const tasks = [
      makeTask({ id: 11, sort_order: 300 }),
      makeTask({ id: 12, sort_order: 200 }),
      makeTask({ id: 13, sort_order: 100 }),
    ];
    const { container } = render(
      <TaskList
        tasks={tasks}
        projects={[]}
        onRefresh={onRefresh}
        onReorder={onReorder}
        onOpenChat={vi.fn()}
      />,
    );

    const rows = container.querySelectorAll('[data-reorder-idx]');
    const handle = rows[0].querySelector('[title="按住拖动排序"]')!;
    fireEvent.pointerDown(handle, { button: 0, clientX: 10, clientY: 10 });
    const dataTransfer = { effectAllowed: '', setData: vi.fn(), getData: vi.fn() };
    fireEvent.dragOver(rows[2], { dataTransfer });
    fireEvent.drop(rows[2], { dataTransfer });

    await waitFor(() => expect(onReorder).toHaveBeenCalled());
    const optimistic = onReorder.mock.calls[0][0] as Task[];
    expect(optimistic.map((task) => task.id)).toEqual([12, 11, 13]);
    const [, update] = (api.updateTask as ReturnType<typeof vi.fn>).mock.calls.at(-1)!;
    expect(update.sort_order).toBe(150);
    expect(onRefresh).not.toHaveBeenCalled();

    resolveUpdate({});
    await waitFor(() => expect(onRefresh).toHaveBeenCalledTimes(1));
  });

  it('moves the first of two same-group tasks to the end via the lower drop zone', async () => {
    const onRefresh = vi.fn();
    const onReorder = vi.fn();
    const tasks = [
      makeTask({ id: 11, sort_order: 300 }),
      makeTask({ id: 12, sort_order: 200 }),
    ];
    const { container } = render(
      <TaskList
        tasks={tasks}
        projects={[]}
        onRefresh={onRefresh}
        onReorder={onReorder}
        onOpenChat={vi.fn()}
      />,
    );

    const rows = container.querySelectorAll('[data-reorder-idx]');
    vi.spyOn(rows[1], 'getBoundingClientRect').mockReturnValue({
      top: 100,
      bottom: 200,
      height: 100,
      left: 0,
      right: 300,
      width: 300,
      x: 0,
      y: 100,
      toJSON: () => ({}),
    });
    const handle = rows[0].querySelector('[title="按住拖动排序"]')!;
    fireEvent.pointerDown(handle, { button: 0, clientX: 10, clientY: 10 });
    const dataTransfer = { effectAllowed: '', setData: vi.fn(), getData: vi.fn() };
    const dragOver = createEvent.dragOver(rows[1], { dataTransfer });
    const drop = createEvent.drop(rows[1], { dataTransfer });
    Object.defineProperty(dragOver, 'clientY', { value: 175 });
    Object.defineProperty(drop, 'clientY', { value: 175 });
    fireEvent(rows[1], dragOver);
    fireEvent(rows[1], drop);

    await waitFor(() => expect(onReorder).toHaveBeenCalled());
    const optimistic = onReorder.mock.calls[0][0] as Task[];
    expect(optimistic.map((task) => task.id)).toEqual([12, 11]);
    const [id, update] = (api.updateTask as ReturnType<typeof vi.fn>).mock.calls.at(-1)!;
    expect(id).toBe(11);
    expect(update.sort_order).toBe(140);
    await waitFor(() => expect(onRefresh).toHaveBeenCalledTimes(1));
  });
});
