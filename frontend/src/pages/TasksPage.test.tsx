import { act, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { TasksPage } from './TasksPage';
import { api } from '../api/client';

let capturedGlobalWs:
  | ((message: Record<string, unknown>) => void)
  | undefined;

vi.mock('../api/client', () => ({
  api: {
    getRuntimeSettings: vi.fn().mockResolvedValue({
      auto_sort_on_access: true,
    }),
    listTasks: vi.fn(),
    countTasks: vi.fn(),
    listProjects: vi.fn(),
    listTags: vi.fn(),
    getTask: vi.fn(),
    markTaskRead: vi.fn().mockResolvedValue({}),
    starTask: vi.fn().mockResolvedValue({}),
    archiveTask: vi.fn().mockResolvedValue({}),
  },
}));

vi.mock('../hooks/useWebSocket', () => ({
  useWebSocket: vi.fn((
    _channels: string[],
    onMessage?: (message: Record<string, unknown>) => void,
  ) => {
    capturedGlobalWs = onMessage;
    return { lastMessage: null, isConnected: true };
  }),
}));

vi.mock('../hooks/useTaskSearch', () => ({
  useTaskSearch: () => [null, vi.fn()],
}));

vi.mock('../hooks/useTaskReorder', () => ({
  mergeVisibleTaskOrder: (
    _current: Record<string, unknown>[],
    optimistic: Record<string, unknown>[],
  ) => optimistic,
  useTaskReorder: () => ({
    itemProps: () => ({}),
    draggingId: null,
    overIndex: null,
  }),
}));

vi.mock('../components/Tasks/TaskForm', () => ({
  TaskForm: () => null,
}));
vi.mock('../components/PlanReview/PlanPanel', () => ({
  PlanPanel: ({ tasks }: { tasks: Array<{ id: number }> }) => (
    <div data-testid="plan-panel-tasks">
      {tasks.map((plan) => plan.id).join(',')}
    </div>
  ),
}));
vi.mock('../components/Tasks/TaskList', () => ({
  TaskList: ({
    tasks,
  }: {
    tasks: Array<{
      id: number;
      status: string;
      background_active?: boolean;
      plan_stage?: string | null;
      plan_stage_round?: number | null;
      plan_stage_provider?: string | null;
      plan_stage_model?: string | null;
      plan_stage_route_slot?: string | null;
    }>;
  }) => (
    <div data-testid="task-snapshots">
      {tasks.map((task) => (
        <div key={task.id}>
          <span>
            {task.id}:{task.status}:{String(task.background_active === true)}
          </span>
          <span data-testid={`plan-stage-${task.id}`}>
            {task.plan_stage || 'none'}:{task.plan_stage_round ?? 'none'}
          </span>
          <span data-testid={`plan-route-${task.id}`}>
            {task.plan_stage_provider || 'none'}:{task.plan_stage_model || 'none'}:{task.plan_stage_route_slot || 'none'}
          </span>
        </div>
      ))}
    </div>
  ),
}));
vi.mock('../components/Chat/ChatView', () => ({
  ChatView: () => null,
}));
vi.mock('../components/Chat/LoopChatView', () => ({
  LoopChatView: () => null,
}));
vi.mock('../components/ProjectSelect', () => ({
  ProjectSelect: () => null,
}));
vi.mock('../components/Tasks/TaskBadges', () => ({
  PluginsBadge: () => null,
  SubAgentsBadge: () => null,
}));
vi.mock('../components/TeamShareModal', () => ({
  TeamShareModal: () => null,
}));

const task = {
  id: 7,
  title: 'Realtime task',
  description: 'd',
  status: 'pending',
  background_active: false,
  priority: 0,
  project_id: null,
  starred: false,
  archived: false,
  has_unread: false,
  mode: 'auto',
};

describe('TasksPage realtime reconciliation', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    capturedGlobalWs = undefined;
    vi.mocked(api.listTasks).mockResolvedValue([task] as never);
    vi.mocked(api.countTasks).mockResolvedValue({ total: 1 });
    vi.mocked(api.listProjects).mockResolvedValue([]);
    vi.mocked(api.listTags).mockResolvedValue([]);
    Object.defineProperty(window, 'matchMedia', {
      configurable: true,
      value: vi.fn().mockReturnValue({
        matches: false,
        addEventListener: vi.fn(),
        removeEventListener: vi.fn(),
      }),
    });
  });

  it('refreshes counts/filter membership after status_change but not a marker-only event', async () => {
    render(
      <TasksPage
        chatTaskId={null}
        onChatTaskChange={vi.fn()}
      />,
    );

    await waitFor(() => {
      expect(api.countTasks).toHaveBeenCalledTimes(1);
      expect(capturedGlobalWs).toBeTypeOf('function');
    });

    act(() => {
      capturedGlobalWs?.({
        channel: 'tasks',
        data: {
          event: 'status_change',
          task_id: 7,
          new_status: 'completed',
          background_active: false,
        },
      });
    });
    await waitFor(() => {
      expect(api.countTasks).toHaveBeenCalledTimes(2);
    });

    act(() => {
      capturedGlobalWs?.({
        channel: 'tasks',
        data: {
          event: 'background_activity',
          task_id: 7,
          background_active: true,
        },
      });
    });

    expect(await screen.findByText('7:pending:true')).toBeInTheDocument();
    await act(async () => {
      await Promise.resolve();
    });
    expect(api.countTasks).toHaveBeenCalledTimes(2);
  });

  it('applies Plan stage changes without waiting for polling', async () => {
    vi.mocked(api.listTasks).mockResolvedValue([{
      ...task,
      mode: 'plan',
      status: 'executing',
      plan_stage: 'planning',
      plan_stage_round: 1,
    }] as never);

    render(
      <TasksPage
        chatTaskId={null}
        onChatTaskChange={vi.fn()}
      />,
    );

    expect(await screen.findByTestId('plan-stage-7')).toHaveTextContent(
      'planning:1',
    );
    const countCalls = vi.mocked(api.countTasks).mock.calls.length;

    act(() => {
      capturedGlobalWs?.({
        channel: 'tasks',
        data: {
          event: 'plan_stage_change',
          task_id: 7,
          plan_stage: 'reviewing',
          plan_stage_round: 2,
          plan_stage_provider: 'codex',
          plan_stage_model: 'gpt-5.6-terra',
          plan_stage_effort: 'xhigh',
          plan_stage_route_slot: 'fallback',
        },
      });
    });

    expect(await screen.findByTestId('plan-stage-7')).toHaveTextContent(
      'reviewing:2',
    );
    expect(await screen.findByTestId('plan-route-7')).toHaveTextContent(
      'codex:gpt-5.6-terra:fallback',
    );
    expect(api.countTasks).toHaveBeenCalledTimes(countCalls);

    act(() => {
      capturedGlobalWs?.({
        channel: 'tasks',
        data: {
          event: 'plan_ready',
          task_id: 7,
        },
      });
    });

    expect(await screen.findByText('7:plan_review:false')).toBeInTheDocument();
    expect(api.countTasks).toHaveBeenCalledTimes(countCalls);
  });

  it('filters related Plans in TaskList without filtering PlanPanel', async () => {
    const main = { ...task, id: 1, mode: 'auto' };
    const standalone = {
      ...task,
      id: 2,
      mode: 'plan',
      plan_target_task_id: null,
    };
    const related = {
      ...task,
      id: 3,
      mode: 'plan',
      plan_target_task_id: 1,
    };
    vi.mocked(api.listTasks).mockImplementation((...args) => (
      args[8] === 'related_plan'
        ? Promise.resolve([related] as never)
        : Promise.resolve([main, standalone, related] as never)
    ));
    vi.mocked(api.countTasks).mockResolvedValue({ total: 1 });

    render(
      <TasksPage
        chatTaskId={null}
        onChatTaskChange={vi.fn()}
      />,
    );

    await userEvent.click(await screen.findByRole('button', { name: /Filter/ }));
    await userEvent.click(screen.getByRole('button', { name: 'Related Plans' }));

    await waitFor(() => expect(api.listTasks).toHaveBeenCalledWith(
      undefined,
      false,
      undefined,
      undefined,
      20,
      0,
      false,
      undefined,
      'related_plan',
    ));
    expect(await screen.findByText('3:pending:false')).toBeInTheDocument();
    expect(screen.queryByText('1:pending:false')).not.toBeInTheDocument();
    expect(screen.getByTestId('plan-panel-tasks')).toHaveTextContent('1,2,3');
  });
});
