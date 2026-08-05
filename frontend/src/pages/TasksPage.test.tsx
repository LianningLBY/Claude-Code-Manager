import { act, render, screen, waitFor } from '@testing-library/react';
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
  PlanPanel: () => null,
}));
vi.mock('../components/Tasks/TaskList', () => ({
  TaskList: ({
    tasks,
  }: {
    tasks: Array<{
      id: number;
      status: string;
      background_active?: boolean;
    }>;
  }) => (
    <div data-testid="task-snapshots">
      {tasks.map((task) => (
        <span key={task.id}>
          {task.id}:{task.status}:{String(task.background_active === true)}
        </span>
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
    localStorage.clear();
    Object.defineProperty(window, 'innerWidth', {
      configurable: true,
      value: 1024,
    });
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

  it('shows the attention tag in the split-mode task sidebar', async () => {
    Object.defineProperty(window, 'innerWidth', {
      configurable: true,
      value: 1440,
    });
    vi.mocked(api.listTasks).mockResolvedValue([
      { ...task, attention_tag: '等待任务结束' },
    ] as never);

    render(
      <TasksPage
        chatTaskId={task.id}
        onChatTaskChange={vi.fn()}
      />,
    );

    expect(await screen.findByText('等待任务结束')).toBeInTheDocument();
  });
});
