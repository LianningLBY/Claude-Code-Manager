import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor, act } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { ChatView } from './ChatView';
import type { Task, Project, ChatMessage } from '../../api/client';

// Mock dependencies
vi.mock('../../api/client', () => ({
  api: {
    getTaskChatHistory: vi.fn().mockResolvedValue([]),
    sendTaskChat: vi.fn().mockResolvedValue({}),
    updateTask: vi.fn().mockResolvedValue({}),
    stopTaskSession: vi.fn().mockResolvedValue({}),
    listForkAnchors: vi.fn().mockResolvedValue([]),
    forkTask: vi.fn().mockResolvedValue({}),
    uploadImages: vi.fn().mockResolvedValue([]),
    listMonitorSessions: vi.fn().mockResolvedValue([]),
    getAskUserPending: vi.fn().mockResolvedValue({ pending: [] }),
    getRuntimeSettings: vi.fn().mockResolvedValue({
      use_pty_mode: false,
      pty_available: false,
      codex_app_server_enabled: true,
      codex_main_mcp_enabled: true,
    }),
    getWorkerRuntimeSettings: vi.fn().mockResolvedValue({
      use_pty_mode: false,
      pty_available: false,
      codex_app_server_enabled: true,
      codex_main_mcp_enabled: true,
    }),
    config: vi.fn().mockResolvedValue({ model_options: ['claude-opus-4-6'], codex_model_options: [] }),
    getInjectCapabilities: vi.fn().mockResolvedValue({
      attachment_protocol: 1,
      codex_native_inputs: true,
    }),
    injectTaskMessage: vi.fn().mockResolvedValue({ ok: true, injected: true }),
    listQuickPhrases: vi.fn().mockResolvedValue([]),
    createQuickPhrase: vi.fn().mockResolvedValue({}),
    updateQuickPhrase: vi.fn().mockResolvedValue({}),
    deleteQuickPhrase: vi.fn().mockResolvedValue({}),
    getMessageDetail: vi.fn().mockResolvedValue({}),
    getMonitorChecks: vi.fn().mockResolvedValue([]),
    deleteMonitorSession: vi.fn().mockResolvedValue({}),
    resolvePermission: vi.fn().mockResolvedValue({}),
    submitAskUser: vi.fn().mockResolvedValue({}),
    starTask: vi.fn().mockResolvedValue({}),
    distillTask: vi.fn().mockResolvedValue({}),
    saveDistilledSkill: vi.fn().mockResolvedValue({}),
  },
}));

// Store the onMessage/onReconnect callbacks so tests can trigger them
let capturedOnReconnect: (() => void) | undefined;
let capturedOnMessage: ((msg: Record<string, unknown>) => void) | undefined;
let capturedOnSubscribed: ((channels: string[]) => void) | undefined;
vi.mock('../../hooks/useWebSocket', () => ({
  useWebSocket: vi.fn((
    _channels: string[],
    onMessage?: unknown,
    onReconnect?: () => void,
    onSubscribed?: (channels: string[]) => void,
  ) => {
    capturedOnMessage = onMessage as typeof capturedOnMessage;
    capturedOnReconnect = onReconnect;
    capturedOnSubscribed = onSubscribed;
    return { lastMessage: null, isConnected: true };
  }),
}));

vi.mock('../Secrets/SecretPicker', () => ({
  SecretPicker: () => null,
}));

import { api } from '../../api/client';

function makeTask(overrides: Partial<Task> = {}): Task {
  return {
    id: 1,
    title: '',
    description: 'Initial task prompt here',
    status: 'pending',
    priority: 0,
    project_id: null,
    target_repo: null,
    target_branch: 'main',
    result_branch: null,
    merge_status: 'pending',
    instance_id: null,
    retry_count: 0,
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
    session_id: 'session-123',
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

describe('ChatView', () => {
  const projects: Project[] = [];
  const onBack = vi.fn();
  const onTaskUpdated = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
    (api.getTaskChatHistory as ReturnType<typeof vi.fn>).mockResolvedValue([]);
    (api.getAskUserPending as ReturnType<typeof vi.fn>).mockResolvedValue({ pending: [] });
    (api.listForkAnchors as ReturnType<typeof vi.fn>).mockResolvedValue([]);
    (api.getRuntimeSettings as ReturnType<typeof vi.fn>).mockResolvedValue({
      use_pty_mode: false,
      pty_available: false,
      codex_app_server_enabled: true,
      codex_main_mcp_enabled: true,
    });
    (api.getWorkerRuntimeSettings as ReturnType<typeof vi.fn>).mockResolvedValue({
      use_pty_mode: false,
      pty_available: false,
      codex_app_server_enabled: true,
      codex_main_mcp_enabled: true,
    });
    (api.uploadImages as ReturnType<typeof vi.fn>).mockResolvedValue([]);
    (api.getInjectCapabilities as ReturnType<typeof vi.fn>).mockResolvedValue({
      attachment_protocol: 1,
      codex_native_inputs: true,
    });
    (api.injectTaskMessage as ReturnType<typeof vi.fn>).mockResolvedValue({
      ok: true,
      injected: true,
    });
  });

  describe('Codex main MCP capability', () => {
    it('shows the enabled runtime capability on Codex tasks', async () => {
      render(
        <ChatView
          task={makeTask({ provider: 'codex' })}
          projects={projects}
          onBack={onBack}
          onTaskUpdated={onTaskUpdated}
        />,
      );

      expect(await screen.findByTitle('Codex 主任务 MCP 已启用')).toHaveTextContent('MCP 已启用');
    });

    it('shows the Fast badge for a Codex priority task', async () => {
      render(
        <ChatView
          task={makeTask({
            provider: 'codex',
            codex_service_tier: 'priority',
          })}
          projects={projects}
          onBack={onBack}
          onTaskUpdated={onTaskUpdated}
        />,
      );

      expect(screen.getByTestId('codex-fast-badge')).toHaveTextContent('Fast');
    });

    it('blocks an unsupported one-shot model while the task is Fast', async () => {
      vi.mocked(api.config).mockResolvedValue({
        default_provider: 'codex',
        provider_options: ['claude', 'codex'],
        default_model: 'claude-opus-4-6',
        model_options: ['claude-opus-4-6'],
        default_codex_model: 'gpt-5.5',
        codex_model_options: ['gpt-5.5', 'gpt-5.4-mini'],
        default_effort: 'medium',
        effort_options: ['low', 'medium', 'high'],
        claude_model_efforts: {},
        claude_model_context_windows: {},
        codex_effort_options: ['low', 'medium', 'high', 'xhigh'],
        codex_model_efforts: {},
        codex_model_service_tiers: {
          'gpt-5.5': ['default', 'priority'],
          'gpt-5.4-mini': ['default'],
        },
      });
      render(
        <ChatView
          task={makeTask({
            provider: 'codex',
            model: 'gpt-5.5',
            codex_service_tier: 'priority',
          })}
          projects={projects}
          onBack={onBack}
          onTaskUpdated={onTaskUpdated}
        />,
      );

      await userEvent.click(screen.getByTitle('临时切换模型（仅下一条消息）'));
      const unsupported = await screen.findByRole('button', { name: /gpt-5\.4-mini/ });
      expect(unsupported).toBeDisabled();
      expect(unsupported).toHaveAttribute(
        'title',
        'gpt-5.4-mini 不支持 Fast；先在 Task Config 中切换为 Standard',
      );
    });

    it('shows the emergency opt-out state returned by the backend', async () => {
      (api.getRuntimeSettings as ReturnType<typeof vi.fn>).mockResolvedValueOnce({
        use_pty_mode: false,
        pty_available: false,
        codex_app_server_enabled: true,
        codex_main_mcp_enabled: false,
      });

      render(
        <ChatView
          task={makeTask({ provider: 'codex' })}
          projects={projects}
          onBack={onBack}
          onTaskUpdated={onTaskUpdated}
        />,
      );

      expect(await screen.findByTitle('Codex 主任务 MCP 已关闭')).toHaveTextContent('MCP 已关闭');
    });

    it('refreshes the capability badge from runtime settings broadcasts', async () => {
      render(
        <ChatView
          task={makeTask({ provider: 'codex' })}
          projects={projects}
          onBack={onBack}
          onTaskUpdated={onTaskUpdated}
        />,
      );
      await screen.findByTitle('Codex 主任务 MCP 已启用');

      act(() => {
        capturedOnMessage?.({
          channel: 'system',
          data: {
            event: 'runtime_settings_changed',
            use_pty_mode: false,
            codex_app_server_enabled: true,
            codex_main_mcp_enabled: false,
          },
        });
      });

      expect(screen.getByTitle('Codex 主任务 MCP 已关闭')).toHaveTextContent('MCP 已关闭');
    });

    it('shows the proxied Worker capability for Worker tasks', async () => {
      (api.getWorkerRuntimeSettings as ReturnType<typeof vi.fn>).mockResolvedValueOnce({
        use_pty_mode: false,
        pty_available: false,
        codex_app_server_enabled: true,
        codex_main_mcp_enabled: false,
      });

      render(
        <ChatView
          task={makeTask({ provider: 'codex', worker_id: 7 })}
          projects={projects}
          onBack={onBack}
          onTaskUpdated={onTaskUpdated}
        />,
      );

      await waitFor(() => expect(api.getWorkerRuntimeSettings).toHaveBeenCalledWith(7));
      expect(api.getRuntimeSettings).not.toHaveBeenCalled();
      expect(screen.getByTitle('Codex 主任务 MCP 已关闭')).toHaveTextContent('MCP 已关闭');

      act(() => {
        capturedOnMessage?.({
          channel: 'system',
          data: {
            event: 'runtime_settings_changed',
            use_pty_mode: false,
            codex_app_server_enabled: true,
            codex_main_mcp_enabled: true,
          },
        });
      });
      expect(screen.getByTitle('Codex 主任务 MCP 已关闭')).toBeInTheDocument();
    });

    it('does not report an old Worker capability as disabled when it is absent', async () => {
      (api.getWorkerRuntimeSettings as ReturnType<typeof vi.fn>).mockResolvedValueOnce({
        use_pty_mode: false,
        pty_available: false,
        codex_app_server_enabled: true,
      });

      render(
        <ChatView
          task={makeTask({ provider: 'codex', worker_id: 7 })}
          projects={projects}
          onBack={onBack}
          onTaskUpdated={onTaskUpdated}
        />,
      );

      await waitFor(() => expect(api.getWorkerRuntimeSettings).toHaveBeenCalledWith(7));
      expect(screen.queryByTestId('codex-main-mcp-status')).not.toBeInTheDocument();
    });
  });

  describe('Live turn injection', () => {
    it('steers an executing Codex turn even when Claude PTY is off', async () => {
      const task = makeTask({
        provider: 'codex',
        status: 'executing',
        worker_id: null,
        shared_from_id: null,
      });
      render(<ChatView task={task} projects={projects} onBack={onBack} onTaskUpdated={onTaskUpdated} />);

      const toggle = await screen.findByTitle(/Codex turn\/steer.*插入运行中的 turn/);
      await userEvent.click(toggle);
      await userEvent.type(screen.getByRole('textbox'), 'change direction');
      await userEvent.click(screen.getByTitle('注入到运行中的 turn (Ctrl+Enter)'));

      await waitFor(() => {
        expect(api.injectTaskMessage).toHaveBeenCalledWith(
          task.id,
          'change direction',
          {
            provider: 'codex',
            model: null,
            codex_service_tier: 'default',
          },
          undefined,
        );
      });
      expect(api.sendTaskChat).not.toHaveBeenCalled();
    });

    it('uploads images and files and injects their exact server metadata into the active turn', async () => {
      const task = makeTask({
        provider: 'codex',
        status: 'executing',
        worker_id: null,
        shared_from_id: null,
      });
      vi.mocked(api.uploadImages).mockImplementation(async ([file]) => [{
        id: `upload-${file.name}`,
        filename: file.name,
        path: `/srv/uploads/${file.name}`,
        url: `/api/uploads/${file.name}`,
        is_image: file.type.startsWith('image/'),
      }]);
      vi.mocked(api.injectTaskMessage).mockResolvedValue({
        ok: true,
        injected: true,
        attachment_count: 2,
      });
      const { container } = render(
        <ChatView task={task} projects={projects} onBack={onBack} onTaskUpdated={onTaskUpdated} />,
      );

      await userEvent.click(await screen.findByTitle(/Codex turn\/steer.*插入运行中的 turn/));
      const picker = container.querySelector<HTMLInputElement>('input[type="file"]')!;
      await userEvent.upload(picker, [
        new File(['png'], 'diagram.png', { type: 'image/png' }),
        new File(['notes'], 'notes.txt', { type: 'text/plain' }),
      ]);
      await waitFor(() => expect(api.uploadImages).toHaveBeenCalledTimes(2));
      await screen.findByText('notes.txt');
      await userEvent.click(screen.getByTitle('注入到运行中的 turn (Ctrl+Enter)'));

      await waitFor(() => {
        expect(api.getInjectCapabilities).toHaveBeenCalledWith(task.id);
        expect(api.injectTaskMessage).toHaveBeenCalledWith(
          task.id,
          '(files attached)',
          {
            provider: 'codex',
            model: null,
            codex_service_tier: 'default',
          },
          {
            file_paths: [
              '/srv/uploads/diagram.png',
              '/srv/uploads/notes.txt',
            ],
            image_paths: ['/srv/uploads/diagram.png'],
            attachments: [
              {
                url: '/api/uploads/diagram.png',
                name: 'diagram.png',
                is_image: true,
              },
              {
                url: '/api/uploads/notes.txt',
                name: 'notes.txt',
                is_image: false,
              },
            ],
          },
        );
      });
      expect(api.sendTaskChat).not.toHaveBeenCalled();
      await waitFor(() => {
        expect(screen.queryByText('notes.txt')).not.toBeInTheDocument();
      });
    });

    it('does not contact an old inject endpoint before attachment capability is confirmed', async () => {
      const task = makeTask({
        provider: 'codex',
        status: 'executing',
        worker_id: null,
        shared_from_id: null,
      });
      vi.mocked(api.uploadImages).mockResolvedValue([{
        id: 'upload-evidence',
        filename: 'evidence.txt',
        path: '/srv/uploads/evidence.txt',
        url: '/api/uploads/evidence.txt',
        is_image: false,
      }]);
      vi.mocked(api.getInjectCapabilities).mockRejectedValue(
        new Error('HTTP 404'),
      );
      const { container } = render(
        <ChatView task={task} projects={projects} onBack={onBack} onTaskUpdated={onTaskUpdated} />,
      );

      await userEvent.click(await screen.findByTitle(/Codex turn\/steer.*插入运行中的 turn/));
      await userEvent.upload(
        container.querySelector<HTMLInputElement>('input[type="file"]')!,
        new File(['evidence'], 'evidence.txt', { type: 'text/plain' }),
      );
      await screen.findByText('evidence.txt');
      await userEvent.click(screen.getByTitle('注入到运行中的 turn (Ctrl+Enter)'));

      expect(await screen.findByText(/HTTP 404/)).toHaveTextContent(
        '消息和附件已保留',
      );
      expect(api.injectTaskMessage).not.toHaveBeenCalled();
      expect(screen.getByText('evidence.txt')).toBeInTheDocument();
    });

    it('keeps attachments unless the server confirms the exact attachment count', async () => {
      const task = makeTask({
        provider: 'codex',
        status: 'executing',
        worker_id: null,
        shared_from_id: null,
      });
      vi.mocked(api.uploadImages).mockResolvedValue([{
        id: 'upload-evidence',
        filename: 'evidence.txt',
        path: '/srv/uploads/evidence.txt',
        url: '/api/uploads/evidence.txt',
        is_image: false,
      }]);
      vi.mocked(api.injectTaskMessage).mockResolvedValue({
        ok: true,
        injected: true,
      });
      const { container } = render(
        <ChatView task={task} projects={projects} onBack={onBack} onTaskUpdated={onTaskUpdated} />,
      );

      await userEvent.click(await screen.findByTitle(/Codex turn\/steer.*插入运行中的 turn/));
      await userEvent.upload(
        container.querySelector<HTMLInputElement>('input[type="file"]')!,
        new File(['evidence'], 'evidence.txt', { type: 'text/plain' }),
      );
      await screen.findByText('evidence.txt');
      await userEvent.click(screen.getByTitle('注入到运行中的 turn (Ctrl+Enter)'));

      expect(await screen.findByText(/没有确认全部附件均已注入/)).toHaveTextContent(
        '消息和附件已保留',
      );
      expect(screen.getByText('evidence.txt')).toBeInTheDocument();
    });

    it('freezes composer edits while an injection request is in flight', async () => {
      const task = makeTask({
        provider: 'codex',
        status: 'executing',
        worker_id: null,
        shared_from_id: null,
      });
      let resolveInjection!: (value: {
        ok: boolean;
        injected: boolean;
      }) => void;
      vi.mocked(api.injectTaskMessage).mockImplementation(
        () => new Promise((resolve) => {
          resolveInjection = resolve;
        }),
      );
      render(
        <ChatView task={task} projects={projects} onBack={onBack} onTaskUpdated={onTaskUpdated} />,
      );

      await userEvent.click(await screen.findByTitle(/Codex turn\/steer.*插入运行中的 turn/));
      const textbox = screen.getByRole('textbox');
      await userEvent.type(textbox, 'snapshot');
      await userEvent.click(screen.getByTitle('注入到运行中的 turn (Ctrl+Enter)'));
      await waitFor(() => expect(api.injectTaskMessage).toHaveBeenCalled());

      expect(textbox).toBeDisabled();
      expect(screen.getByTitle('Attach files')).toBeDisabled();
      expect(screen.getByTitle(/注入模式已开启/)).toBeDisabled();

      await act(async () => {
        resolveInjection({ ok: true, injected: true });
      });
      await waitFor(() => expect(textbox).not.toBeDisabled());
      expect(textbox).toHaveValue('');
    });

    it('keeps the text and uploaded attachment when the server does not confirm injection', async () => {
      const task = makeTask({
        provider: 'codex',
        status: 'executing',
        worker_id: null,
        shared_from_id: null,
      });
      vi.mocked(api.uploadImages).mockResolvedValue([{
        id: 'upload-evidence',
        filename: 'evidence.txt',
        path: '/srv/uploads/evidence.txt',
        url: '/api/uploads/evidence.txt',
        is_image: false,
      }]);
      vi.mocked(api.injectTaskMessage).mockResolvedValue({
        ok: true,
        injected: false,
      });
      const { container } = render(
        <ChatView task={task} projects={projects} onBack={onBack} onTaskUpdated={onTaskUpdated} />,
      );

      await userEvent.click(await screen.findByTitle(/Codex turn\/steer.*插入运行中的 turn/));
      await userEvent.upload(
        container.querySelector<HTMLInputElement>('input[type="file"]')!,
        new File(['evidence'], 'evidence.txt', { type: 'text/plain' }),
      );
      await screen.findByText('evidence.txt');
      await userEvent.type(screen.getByRole('textbox'), '请结合附件继续');
      await userEvent.click(screen.getByTitle('注入到运行中的 turn (Ctrl+Enter)'));

      expect(await screen.findByText(/服务器没有确认消息已注入/)).toHaveTextContent(
        '消息和附件已保留',
      );
      expect(screen.getByRole('textbox')).toHaveValue('请结合附件继续');
      expect(screen.getByText('evidence.txt')).toBeInTheDocument();
      expect(api.sendTaskChat).not.toHaveBeenCalled();
    });

    it('does not silently drop a failed upload when injecting text', async () => {
      const task = makeTask({
        provider: 'codex',
        status: 'executing',
        worker_id: null,
        shared_from_id: null,
      });
      vi.mocked(api.uploadImages).mockRejectedValue(new Error('upload rejected'));
      const { container } = render(
        <ChatView task={task} projects={projects} onBack={onBack} onTaskUpdated={onTaskUpdated} />,
      );

      await userEvent.click(await screen.findByTitle(/Codex turn\/steer.*插入运行中的 turn/));
      await userEvent.upload(
        container.querySelector<HTMLInputElement>('input[type="file"]')!,
        new File(['bad'], 'broken.txt', { type: 'text/plain' }),
      );
      await screen.findByTitle('Click to retry');
      await userEvent.type(screen.getByRole('textbox'), '不要漏掉附件');
      fireEvent.keyDown(screen.getByRole('textbox'), { key: 'Enter', ctrlKey: true });

      expect(await screen.findByText('Retry or remove failed attachments before sending.')).toBeInTheDocument();
      expect(api.injectTaskMessage).not.toHaveBeenCalled();
      expect(screen.getByText('broken.txt')).toBeInTheDocument();
    });

    it('does not offer local injection for worker tasks', async () => {
      const task = makeTask({
        provider: 'codex',
        status: 'executing',
        worker_id: 7,
        shared_from_id: null,
      });
      render(<ChatView task={task} projects={projects} onBack={onBack} onTaskUpdated={onTaskUpdated} />);

      await waitFor(() => expect(api.getWorkerRuntimeSettings).toHaveBeenCalledWith(7));
      expect(screen.queryByTitle(/Codex turn\/steer/)).not.toBeInTheDocument();
    });
  });

  describe('Initial prompt bubble', () => {
    it('renders initial prompt as first bubble', async () => {
      const task = makeTask({ title: 'Has Title', description: 'Build a login page' });
      render(<ChatView task={task} projects={projects} onBack={onBack} onTaskUpdated={onTaskUpdated} />);

      expect(screen.getByText('— Initial Prompt —')).toBeInTheDocument();
      // Description appears in the initial prompt bubble (header shows title instead)
      expect(screen.getByText('Build a login page')).toBeInTheDocument();
    });

    it('shows timestamp on initial prompt bubble from task.created_at', async () => {
      const task = makeTask({ description: 'Hello', created_at: '2024-06-15T10:30:00Z' });
      const { container } = render(<ChatView task={task} projects={projects} onBack={onBack} onTaskUpdated={onTaskUpdated} />);

      // The initial prompt bubble should contain a MessageTimestamp span
      const initialPromptDiv = container.querySelector('[data-user-msg]')!;
      expect(initialPromptDiv).toBeInTheDocument();
      // Look for the timestamp span (text-[10px] is the MessageTimestamp class)
      const timestampSpan = initialPromptDiv.querySelector('span.select-none');
      expect(timestampSpan).toBeInTheDocument();
      expect(timestampSpan!.textContent).toBeTruthy();
    });

    it('does not show timestamp on initial prompt when created_at is missing', async () => {
      const task = makeTask({ description: 'Hello', created_at: '' });
      const { container } = render(<ChatView task={task} projects={projects} onBack={onBack} onTaskUpdated={onTaskUpdated} />);

      const initialPromptDiv = container.querySelector('[data-user-msg]')!;
      expect(initialPromptDiv).toBeInTheDocument();
      // No timestamp span should appear
      const timestampSpan = initialPromptDiv.querySelector('span.select-none');
      expect(timestampSpan).not.toBeInTheDocument();
    });

    it('does not render initial prompt bubble when description is null', async () => {
      const task = makeTask({ description: null });
      render(<ChatView task={task} projects={projects} onBack={onBack} onTaskUpdated={onTaskUpdated} />);

      expect(screen.queryByText('— Initial Prompt —')).not.toBeInTheDocument();
    });

    it('opens one toolbar picker and forks before a selected user message', async () => {
      const task = makeTask({ id: 12, provider: 'codex', status: 'completed' });
      const anchor = {
        type: 'user_message' as const,
        id: 456,
        content: 'Try a different implementation',
        timestamp: '2024-01-01T00:01:00Z',
        attachments: [],
      };
      const forked = makeTask({
        id: 99,
        provider: 'codex',
        status: 'completed',
        session_id: 'fork-thread',
        metadata_: {
          fork_seed_message: anchor.content,
          fork_seed_log_id: anchor.id,
        },
      });
      const onTaskForked = vi.fn();
      (api.listForkAnchors as ReturnType<typeof vi.fn>).mockResolvedValue([anchor]);
      (api.forkTask as ReturnType<typeof vi.fn>).mockResolvedValue(forked);
      render(
        <ChatView
          task={task}
          projects={projects}
          onBack={onBack}
          onTaskForked={onTaskForked}
        />,
      );

      const forkButton = screen.getByLabelText('Fork Codex session');
      expect(forkButton).toHaveClass('h-8', 'w-8');
      expect(forkButton).toHaveClass('text-gray-500', 'hover:text-indigo-400');
      expect(forkButton).not.toHaveClass('text-indigo-400');
      await userEvent.click(forkButton);
      expect(await screen.findByText(anchor.content)).toBeInTheDocument();
      expect(api.listForkAnchors).toHaveBeenCalledWith(task.id);
      await userEvent.click(screen.getByText(anchor.content));
      await userEvent.click(screen.getByRole('button', { name: 'Create fork' }));

      await waitFor(() => {
        expect(api.forkTask).toHaveBeenCalledWith(
          12,
          { type: 'user_message', id: anchor.id },
          '',
        );
        expect(onTaskForked).toHaveBeenCalledWith(forked);
      });
    });

    it('can copy the complete latest Codex context without a seed message', async () => {
      const task = makeTask({ id: 14, provider: 'codex', status: 'completed' });
      const latest = {
        type: 'latest' as const,
        id: null,
        content: '完整复制当前上下文',
        timestamp: '2024-01-01T00:02:00Z',
        attachments: [],
      };
      const forked = makeTask({
        id: 100,
        provider: 'codex',
        session_id: 'full-copy-thread',
        metadata_: { fork_mode: 'full_copy' },
      });
      (api.listForkAnchors as ReturnType<typeof vi.fn>).mockResolvedValue([latest]);
      (api.forkTask as ReturnType<typeof vi.fn>).mockResolvedValue(forked);

      render(
        <ChatView
          task={task}
          projects={projects}
          onBack={onBack}
          onTaskForked={vi.fn()}
        />,
      );

      await userEvent.click(screen.getByLabelText('Fork Codex session'));
      await userEvent.click(await screen.findByText('完整复制当前上下文'));
      await userEvent.click(screen.getByRole('button', { name: 'Create fork' }));

      await waitFor(() => {
        expect(api.forkTask).toHaveBeenCalledWith(
          task.id,
          { type: 'latest' },
          '',
        );
      });
    });

    it('does not offer native fork actions for Claude sessions', () => {
      const task = makeTask({ provider: 'claude', status: 'completed' });
      render(<ChatView task={task} projects={projects} onBack={onBack} />);

      expect(screen.queryByLabelText('Fork Codex session')).not.toBeInTheDocument();
    });

    it('does not add fork actions to assistant messages', async () => {
      const task = makeTask({
        id: 13,
        description: null,
        provider: 'codex',
        status: 'completed',
      });
      const message: ChatMessage = {
        id: 456,
        task_id: task.id,
        event_type: 'message',
        role: 'assistant',
        content: 'Forkable answer',
        is_error: false,
        timestamp: '2024-01-01T00:01:00Z',
      };
      (api.getTaskChatHistory as ReturnType<typeof vi.fn>).mockResolvedValue([message]);
      render(<ChatView task={task} projects={projects} onBack={onBack} />);

      expect(await screen.findByText('Forkable answer')).toBeInTheDocument();
      expect(screen.getAllByLabelText('Fork Codex session')).toHaveLength(1);
    });

    it('prefills a fork seed only once when the child task opens', () => {
      const task = makeTask({
        id: 101,
        provider: 'codex',
        metadata_: {
          fork_seed_message: 'editable fork prompt',
          fork_seed_log_id: 456,
        },
      });
      const first = render(<ChatView task={task} projects={projects} onBack={onBack} />);
      expect(screen.getByRole('textbox')).toHaveValue('editable fork prompt');
      first.unmount();
      localStorage.removeItem(`ccm-chat-draft-${task.id}`);

      render(<ChatView task={task} projects={projects} onBack={onBack} />);
      expect(screen.getByRole('textbox')).toHaveValue('');
    });

    it('shows and sends fork seed attachments with the editable message', async () => {
      const task = makeTask({
        id: 102,
        provider: 'codex',
        session_id: 'fork-thread',
        metadata_: {
          fork_seed_message: 'inspect this file',
          fork_seed_log_id: 456,
          fork_seed_uploads: [{
            id: 'fork-seed-0',
            filename: 'evidence.txt',
            path: '/repo/uploads/evidence.txt',
            url: '/api/uploads/evidence.txt',
            is_image: false,
          }],
        },
      });
      render(<ChatView task={task} projects={projects} onBack={onBack} />);

      expect(screen.getByText('evidence.txt')).toBeInTheDocument();
      await userEvent.click(screen.getByTitle('Send (Ctrl+Enter)'));

      await waitFor(() => expect(api.sendTaskChat).toHaveBeenCalledWith(
        task.id,
        'inspect this file',
        ['/repo/uploads/evidence.txt'],
        undefined,
        null,
        {
          provider: 'codex',
          model: null,
          codex_service_tier: 'default',
        },
      ));
    });

    it('allows removing a fork seed attachment before sending', async () => {
      const task = makeTask({
        id: 103,
        provider: 'codex',
        session_id: 'fork-thread',
        metadata_: {
          fork_seed_message: 'text only now',
          fork_seed_uploads: [{
            id: 'fork-seed-0',
            filename: 'remove.txt',
            path: '/repo/uploads/remove.txt',
            url: '/api/uploads/remove.txt',
            is_image: false,
          }],
        },
      });
      render(<ChatView task={task} projects={projects} onBack={onBack} />);

      await userEvent.click(screen.getByRole('button', { name: 'Remove remove.txt' }));
      await userEvent.click(screen.getByTitle('Send (Ctrl+Enter)'));

      await waitFor(() => expect(api.sendTaskChat).toHaveBeenCalledWith(
        task.id,
        'text only now',
        undefined,
        undefined,
        null,
        {
          provider: 'codex',
          model: null,
          codex_service_tier: 'default',
        },
      ));
    });
  });

  describe('Title display', () => {
    it('shows title when set', () => {
      const task = makeTask({ title: 'Custom Title', description: 'Some prompt' });
      render(<ChatView task={task} projects={projects} onBack={onBack} onTaskUpdated={onTaskUpdated} />);

      expect(screen.getByText('Custom Title')).toBeInTheDocument();
    });

    it('falls back to description when title is empty', () => {
      const task = makeTask({ title: '', description: 'The prompt' });
      render(<ChatView task={task} projects={projects} onBack={onBack} onTaskUpdated={onTaskUpdated} />);

      // Description appears both in header (as title fallback) and in initial prompt bubble
      const matches = screen.getAllByText('The prompt');
      expect(matches.length).toBeGreaterThanOrEqual(2);
    });

    it('shows Untitled when both title and description are empty', () => {
      const task = makeTask({ title: '', description: null });
      render(<ChatView task={task} projects={projects} onBack={onBack} onTaskUpdated={onTaskUpdated} />);

      expect(screen.getByText('Untitled')).toBeInTheDocument();
    });
  });

  describe('Title editing', () => {
    it('enters edit mode on pencil click', async () => {
      const task = makeTask({ title: 'My Title' });
      render(<ChatView task={task} projects={projects} onBack={onBack} onTaskUpdated={onTaskUpdated} />);

      const editBtn = screen.getByTitle('Edit title');
      await userEvent.click(editBtn);

      expect(screen.getByPlaceholderText('Enter title...')).toBeInTheDocument();
    });

    it('saves title on Enter and calls onTaskUpdated', async () => {
      const task = makeTask({ id: 5, title: 'Old Title' });
      render(<ChatView task={task} projects={projects} onBack={onBack} onTaskUpdated={onTaskUpdated} />);

      await userEvent.click(screen.getByTitle('Edit title'));
      const input = screen.getByPlaceholderText('Enter title...');
      await userEvent.clear(input);
      await userEvent.type(input, 'New Title{Enter}');

      expect(api.updateTask).toHaveBeenCalledWith(5, { title: 'New Title' });
      expect(onTaskUpdated).toHaveBeenCalled();
    });

    it('cancels editing on Escape without saving', async () => {
      const task = makeTask({ title: 'Keep' });
      render(<ChatView task={task} projects={projects} onBack={onBack} onTaskUpdated={onTaskUpdated} />);

      await userEvent.click(screen.getByTitle('Edit title'));
      const input = screen.getByPlaceholderText('Enter title...');
      await userEvent.type(input, 'Nope');
      await userEvent.keyboard('{Escape}');

      expect(api.updateTask).not.toHaveBeenCalled();
    });
  });

  describe('Scroll container', () => {
    it('message container has min-h-0 for proper flex scrolling', () => {
      const task = makeTask();
      const { container } = render(<ChatView task={task} projects={projects} onBack={onBack} />);

      const messageContainer = container.querySelector('.overflow-y-auto.min-h-0');
      expect(messageContainer).toBeInTheDocument();
    });
  });

  describe('Textarea auto-resize', () => {
    it('textarea has ref and auto-resize classes', () => {
      const task = makeTask();
      const { container } = render(<ChatView task={task} projects={projects} onBack={onBack} />);

      const textarea = container.querySelector('textarea');
      expect(textarea).toBeInTheDocument();
      expect(textarea?.className).toContain('max-h-48');
      expect(textarea?.className).toContain('overflow-y-auto');
      expect(textarea?.className).toContain('resize-none');
    });

    it('adjusts height when input changes', async () => {
      const task = makeTask();
      const { container } = render(<ChatView task={task} projects={projects} onBack={onBack} />);

      const textarea = container.querySelector('textarea')!;
      // Mock scrollHeight
      Object.defineProperty(textarea, 'scrollHeight', { value: 80, configurable: true });

      await userEvent.type(textarea, 'Line 1\nLine 2\nLine 3');

      expect(textarea.style.height).toBe('80px');
    });

    it('resets height when input is cleared', async () => {
      const task = makeTask();
      const { container } = render(<ChatView task={task} projects={projects} onBack={onBack} />);

      const textarea = container.querySelector('textarea')!;
      Object.defineProperty(textarea, 'scrollHeight', { value: 80, configurable: true });

      await userEvent.type(textarea, 'Hello');
      expect(textarea.style.height).toBe('80px');

      // Simulate clearing input and smaller scrollHeight
      Object.defineProperty(textarea, 'scrollHeight', { value: 40, configurable: true });
      await userEvent.clear(textarea);
      expect(textarea.style.height).toBe('40px');
    });
  });

  describe('Back button', () => {
    it('calls onBack when back button clicked', async () => {
      const task = makeTask();
      render(<ChatView task={task} projects={projects} onBack={onBack} />);

      const backButtons = screen.getAllByRole('button');
      // First button is the back arrow
      await userEvent.click(backButtons[0]);
      expect(onBack).toHaveBeenCalled();
    });
  });

  describe('Chat history loading', () => {
    it('loads chat history on mount', async () => {
      const task = makeTask({ id: 42 });
      render(<ChatView task={task} projects={projects} onBack={onBack} />);

      await waitFor(() => {
        // compact=true, limit=HISTORY_PAGE_SIZE (paginated initial load)
        expect(api.getTaskChatHistory).toHaveBeenCalledWith(42, true, 200, 0, true);
      });
    });

    it('re-fetches chat history on WebSocket reconnect', async () => {
      const msgs: ChatMessage[] = [
        { id: 1, role: 'assistant', event_type: 'message', content: 'Hello', tool_name: null, tool_input: null, tool_output: null, is_error: false, loop_iteration: null, timestamp: '2024-01-01T00:00:00Z' },
      ];
      (api.getTaskChatHistory as ReturnType<typeof vi.fn>).mockResolvedValue(msgs);
      const task = makeTask({ id: 10 });
      render(<ChatView task={task} projects={projects} onBack={onBack} />);

      // Wait for initial load
      await waitFor(() => {
        expect(api.getTaskChatHistory).toHaveBeenCalledTimes(1);
      });

      // Simulate WebSocket reconnection
      capturedOnReconnect?.();

      await waitFor(() => {
        expect(api.getTaskChatHistory).toHaveBeenCalledTimes(2);
      });
    });

    it('passes onReconnect callback to useWebSocket', () => {
      const task = makeTask();
      render(<ChatView task={task} projects={projects} onBack={onBack} />);

      expect(capturedOnReconnect).toBeDefined();
      expect(typeof capturedOnReconnect).toBe('function');
      expect(capturedOnSubscribed).toBeDefined();
      expect(typeof capturedOnSubscribed).toBe('function');
    });

    it('keeps a live message when an older history snapshot finishes later', async () => {
      let resolveHistory!: (messages: ChatMessage[]) => void;
      (api.getTaskChatHistory as ReturnType<typeof vi.fn>).mockReturnValueOnce(
        new Promise<ChatMessage[]>((resolve) => { resolveHistory = resolve; }),
      );
      const task = makeTask({ id: 12, description: null });
      render(<ChatView task={task} projects={projects} onBack={onBack} />);

      act(() => {
        capturedOnMessage?.({
          channel: 'task:12',
          data: {
            event_type: 'message',
            role: 'assistant',
            content: 'live while snapshot is in flight',
            is_error: false,
          },
        });
      });
      expect(screen.getByText('live while snapshot is in flight')).toBeInTheDocument();

      await act(async () => { resolveHistory([]); });
      expect(screen.getByText('live while snapshot is in flight')).toBeInTheDocument();
    });

    it('backfills again after the task-channel subscription is acknowledged', async () => {
      const task = makeTask({ id: 13 });
      render(<ChatView task={task} projects={projects} onBack={onBack} />);
      await waitFor(() => expect(api.getTaskChatHistory).toHaveBeenCalledTimes(1));

      act(() => { capturedOnSubscribed?.(['task:13']); });
      await waitFor(() => expect(api.getTaskChatHistory).toHaveBeenCalledTimes(2));
    });

    it('does not revive an answered ask-user card from a stale pending snapshot', async () => {
      let resolvePending!: (value: {
        pending: { request_id: string; questions: {
          question: string;
          options: { label: string }[];
        }[] }[];
      }) => void;
      (api.getAskUserPending as ReturnType<typeof vi.fn>).mockReturnValueOnce(
        new Promise((resolve) => { resolvePending = resolve; }),
      );
      render(
        <ChatView
          task={makeTask({ id: 14, description: null })}
          projects={projects}
          onBack={onBack}
        />,
      );

      act(() => {
        capturedOnMessage?.({
          channel: 'task:14',
          data: {
            event_type: 'ask_user_question',
            request_id: 'ask-1',
            questions: [{
              question: 'Proceed?',
              options: [{ label: 'Yes' }, { label: 'No' }],
            }],
          },
        });
      });
      expect(screen.getByText('Proceed?')).toBeInTheDocument();

      act(() => {
        capturedOnMessage?.({
          channel: 'task:14',
          data: {
            event_type: 'ask_user_resolved',
            request_id: 'ask-1',
            timed_out: false,
          },
        });
      });
      expect(screen.getByText('✓ 已回答')).toBeInTheDocument();

      await act(async () => {
        resolvePending({
          pending: [{
            request_id: 'ask-1',
            questions: [{
              question: 'Proceed?',
              options: [{ label: 'Yes' }, { label: 'No' }],
            }],
          }],
        });
      });

      expect(screen.getAllByText('Proceed?')).toHaveLength(1);
      expect(screen.getByText('✓ 已回答')).toBeInTheDocument();
      expect(screen.queryByPlaceholderText('或自定义回答…')).not.toBeInTheDocument();
    });

    it('tombstones a locally submitted answer before a stale pending snapshot returns', async () => {
      let resolvePending!: (value: {
        pending: { request_id: string; questions: {
          question: string;
          options: { label: string }[];
        }[] }[];
      }) => void;
      (api.getAskUserPending as ReturnType<typeof vi.fn>).mockReturnValueOnce(
        new Promise((resolve) => { resolvePending = resolve; }),
      );
      render(
        <ChatView
          task={makeTask({ id: 15, description: null })}
          projects={projects}
          onBack={onBack}
        />,
      );

      act(() => {
        capturedOnMessage?.({
          channel: 'task:15',
          data: {
            event_type: 'ask_user_question',
            request_id: 'ask-local',
            questions: [{
              question: 'Ship it?',
              options: [{ label: 'Yes' }, { label: 'No' }],
            }],
          },
        });
      });
      await userEvent.click(screen.getByRole('button', { name: /Yes/ }));
      await userEvent.click(screen.getByRole('button', { name: '提交' }));
      await waitFor(() => expect(api.submitAskUser).toHaveBeenCalled());
      expect(screen.getByText('✓ 已回答')).toBeInTheDocument();

      await act(async () => {
        resolvePending({
          pending: [{
            request_id: 'ask-local',
            questions: [{
              question: 'Ship it?',
              options: [{ label: 'Yes' }, { label: 'No' }],
            }],
          }],
        });
      });

      expect(screen.getAllByText('Ship it?')).toHaveLength(1);
      expect(screen.getByText('✓ 已回答')).toBeInTheDocument();
      expect(screen.queryByPlaceholderText('或自定义回答…')).not.toBeInTheDocument();
    });

    it('copies a user message without its sender prefix', async () => {
      const writeText = vi.fn().mockResolvedValue(undefined);
      Object.assign(navigator, { clipboard: { writeText } });
      const msgs: ChatMessage[] = [{
        id: 1,
        role: 'user',
        event_type: 'user_message',
        content: '[Admin] 现在进度怎么样了',
        tool_name: null,
        tool_input: null,
        tool_output: null,
        is_error: false,
        loop_iteration: null,
        timestamp: '2024-01-01T00:00:00Z',
        image_urls: null,
        attachments: null,
      }];
      (api.getTaskChatHistory as ReturnType<typeof vi.fn>).mockResolvedValue(msgs);

      render(<ChatView task={makeTask({ description: null })} projects={projects} onBack={onBack} />);

      await userEvent.click(await screen.findByTitle('Copy message'));

      expect(writeText).toHaveBeenCalledWith('现在进度怎么样了');
    });

    it('preserves a real bracket tag when raw user content is available', async () => {
      const writeText = vi.fn().mockResolvedValue(undefined);
      Object.assign(navigator, { clipboard: { writeText } });
      const msgs: ChatMessage[] = [{
        id: 1,
        role: 'user',
        event_type: 'user_message',
        content: '[Admin] [BUG] preserve this tag',
        raw_content: '[BUG] preserve this tag',
        tool_name: null,
        tool_input: null,
        tool_output: null,
        is_error: false,
        loop_iteration: null,
        timestamp: '2024-01-01T00:00:00Z',
        image_urls: null,
        attachments: null,
      }];
      (api.getTaskChatHistory as ReturnType<typeof vi.fn>).mockResolvedValue(msgs);

      render(<ChatView task={makeTask({ description: null })} projects={projects} onBack={onBack} />);

      await userEvent.click(await screen.findByTitle('Copy message'));

      expect(writeText).toHaveBeenCalledWith('[BUG] preserve this tag');
    });
  });

  describe('User message navigation', () => {
    function makeChatMessages(count: number): ChatMessage[] {
      const msgs: ChatMessage[] = [];
      for (let i = 0; i < count; i++) {
        msgs.push({
          id: i * 2 + 1,
          role: 'user',
          event_type: 'user_message',
          content: `User message ${i + 1}`,
          tool_name: null,
          tool_input: null,
          tool_output: null,
          is_error: false,
          loop_iteration: null,
          timestamp: '2024-01-01T00:00:00Z',
          image_urls: null,
          attachments: null,
        });
        msgs.push({
          id: i * 2 + 2,
          role: 'assistant',
          event_type: 'message',
          content: `Assistant response ${i + 1}`,
          tool_name: null,
          tool_input: null,
          tool_output: null,
          is_error: false,
          loop_iteration: null,
          timestamp: '2024-01-01T00:01:00Z',
          image_urls: null,
          attachments: null,
        });
      }
      return msgs;
    }

    it('shows navigation buttons even with fewer than 2 user messages (always visible since e049d57)', async () => {
      const msgs = makeChatMessages(0);
      (api.getTaskChatHistory as ReturnType<typeof vi.fn>).mockResolvedValue(msgs);
      const task = makeTask({ description: 'Only one user msg' });
      render(<ChatView task={task} projects={projects} onBack={onBack} />);

      await waitFor(() => {
        expect(api.getTaskChatHistory).toHaveBeenCalled();
      });

      // e049d57 起导航按钮常驻工具栏右侧（不再按消息数量显隐）
      expect(screen.getByTitle('Previous user message')).toBeInTheDocument();
      expect(screen.getByTitle('Next user message')).toBeInTheDocument();
    });

    it('shows navigation buttons when 2+ user messages exist (description + 1 chat msg)', async () => {
      const msgs = makeChatMessages(1);
      (api.getTaskChatHistory as ReturnType<typeof vi.fn>).mockResolvedValue(msgs);
      const task = makeTask({ description: 'Initial prompt' });
      render(<ChatView task={task} projects={projects} onBack={onBack} />);

      await waitFor(() => {
        expect(screen.getByTitle('Previous user message')).toBeInTheDocument();
      });
      expect(screen.getByTitle('Next user message')).toBeInTheDocument();
    });

    it('shows navigation buttons when 2+ chat user messages exist (no description)', async () => {
      const msgs = makeChatMessages(2);
      (api.getTaskChatHistory as ReturnType<typeof vi.fn>).mockResolvedValue(msgs);
      const task = makeTask({ description: null });
      render(<ChatView task={task} projects={projects} onBack={onBack} />);

      await waitFor(() => {
        expect(screen.getByTitle('Previous user message')).toBeInTheDocument();
      });
      expect(screen.getByTitle('Next user message')).toBeInTheDocument();
    });

    it('marks initial prompt with data-user-msg attribute', async () => {
      (api.getTaskChatHistory as ReturnType<typeof vi.fn>).mockResolvedValue([]);
      const task = makeTask({ description: 'Initial prompt text' });
      const { container } = render(<ChatView task={task} projects={projects} onBack={onBack} />);

      await waitFor(() => {
        expect(api.getTaskChatHistory).toHaveBeenCalled();
      });

      const userMsgNodes = container.querySelectorAll('[data-user-msg]');
      expect(userMsgNodes.length).toBe(1);
    });

    it('marks user chat messages with data-user-msg attribute', async () => {
      const msgs = makeChatMessages(3);
      (api.getTaskChatHistory as ReturnType<typeof vi.fn>).mockResolvedValue(msgs);
      const task = makeTask({ description: 'Prompt' });
      const { container } = render(<ChatView task={task} projects={projects} onBack={onBack} />);

      await waitFor(() => {
        expect(container.querySelectorAll('[data-user-msg]').length).toBe(4);
      });
    });

    it('does not mark assistant messages with data-user-msg attribute', async () => {
      const msgs = makeChatMessages(2);
      (api.getTaskChatHistory as ReturnType<typeof vi.fn>).mockResolvedValue(msgs);
      const task = makeTask({ description: null });
      const { container } = render(<ChatView task={task} projects={projects} onBack={onBack} />);

      await waitFor(() => {
        expect(container.querySelectorAll('[data-user-msg]').length).toBe(2);
      });

      const allMsgDivs = container.querySelectorAll('.items-start');
      allMsgDivs.forEach((div) => {
        expect(div).not.toHaveAttribute('data-user-msg');
      });
    });

    it('clicking "Previous user message" scrolls the container to the user message', async () => {
      const msgs = makeChatMessages(3);
      (api.getTaskChatHistory as ReturnType<typeof vi.fn>).mockResolvedValue(msgs);
      const task = makeTask({ description: 'Prompt' });
      const { container } = render(<ChatView task={task} projects={projects} onBack={onBack} />);

      await waitFor(() => {
        expect(screen.getByTitle('Previous user message')).toBeInTheDocument();
      });

      const scrollContainer = container.querySelector('.overflow-y-auto')!;
      const userMsgNodes = scrollContainer.querySelectorAll('[data-user-msg]');

      // Navigation is getBoundingClientRect-based: container top = 100;
      // nodes below except nodes[2], which sits above the viewport (top = 0)
      (scrollContainer as HTMLElement).getBoundingClientRect = () => ({ top: 100 } as DOMRect);
      const scrollToMock = vi.fn();
      (scrollContainer as HTMLElement).scrollTo = scrollToMock;
      userMsgNodes.forEach((node, i) => {
        Object.defineProperty(node, 'offsetTop', { value: i * 140, configurable: true });
        (node as HTMLElement).getBoundingClientRect = () => ({ top: i === 2 ? 0 : 200 } as DOMRect);
      });

      await userEvent.click(screen.getByTitle('Previous user message'));

      expect(scrollToMock).toHaveBeenCalledWith({ top: 280, behavior: 'smooth' });
    });

    it('clicking "Next user message" scrolls the container to the next user message', async () => {
      const msgs = makeChatMessages(3);
      (api.getTaskChatHistory as ReturnType<typeof vi.fn>).mockResolvedValue(msgs);
      const task = makeTask({ description: 'Prompt' });
      const { container } = render(<ChatView task={task} projects={projects} onBack={onBack} />);

      await waitFor(() => {
        expect(screen.getByTitle('Next user message')).toBeInTheDocument();
      });

      const scrollContainer = container.querySelector('.overflow-y-auto')!;
      const userMsgNodes = scrollContainer.querySelectorAll('[data-user-msg]');

      // Container top = 100; all nodes below the viewport top (top = 200)
      // → "down" navigates to the first node past container top + threshold
      (scrollContainer as HTMLElement).getBoundingClientRect = () => ({ top: 100 } as DOMRect);
      const scrollToMock = vi.fn();
      (scrollContainer as HTMLElement).scrollTo = scrollToMock;
      userMsgNodes.forEach((node, i) => {
        Object.defineProperty(node, 'offsetTop', { value: i * 140 + 40, configurable: true });
        (node as HTMLElement).getBoundingClientRect = () => ({ top: 200 } as DOMRect);
      });

      await userEvent.click(screen.getByTitle('Next user message'));

      expect(scrollToMock).toHaveBeenCalledWith({ top: 40, behavior: 'smooth' });
    });

    it('does nothing when already at the top and clicking up', async () => {
      const msgs = makeChatMessages(2);
      (api.getTaskChatHistory as ReturnType<typeof vi.fn>).mockResolvedValue(msgs);
      const task = makeTask({ description: 'Prompt' });
      const { container } = render(<ChatView task={task} projects={projects} onBack={onBack} />);

      await waitFor(() => {
        expect(screen.getByTitle('Previous user message')).toBeInTheDocument();
      });

      const scrollContainer = container.querySelector('.overflow-y-auto')!;
      const userMsgNodes = scrollContainer.querySelectorAll('[data-user-msg]');

      const scrollToMock = vi.fn();
      (scrollContainer as HTMLElement).scrollTo = scrollToMock;
      userMsgNodes.forEach((node, i) => {
        Object.defineProperty(node, 'offsetTop', { value: i * 300 + 100, configurable: true });
      });

      Object.defineProperty(scrollContainer, 'scrollTop', { value: 0, configurable: true, writable: true });

      await userEvent.click(screen.getByTitle('Previous user message'));

      expect(scrollToMock).not.toHaveBeenCalled();
    });

    it('does nothing when already at the last user message and clicking down', async () => {
      const msgs = makeChatMessages(2);
      (api.getTaskChatHistory as ReturnType<typeof vi.fn>).mockResolvedValue(msgs);
      const task = makeTask({ description: 'Prompt' });
      const { container } = render(<ChatView task={task} projects={projects} onBack={onBack} />);

      await waitFor(() => {
        expect(screen.getByTitle('Next user message')).toBeInTheDocument();
      });

      const scrollContainer = container.querySelector('.overflow-y-auto')!;
      const userMsgNodes = scrollContainer.querySelectorAll('[data-user-msg]');

      const scrollToMock = vi.fn();
      (scrollContainer as HTMLElement).scrollTo = scrollToMock;
      userMsgNodes.forEach((node, i) => {
        Object.defineProperty(node, 'offsetTop', { value: i * 100, configurable: true });
      });

      Object.defineProperty(scrollContainer, 'scrollTop', { value: 9999, configurable: true, writable: true });

      await userEvent.click(screen.getByTitle('Next user message'));

      expect(scrollToMock).not.toHaveBeenCalled();
    });
  });

  describe('Draft buffering (localStorage)', () => {
    const draftKey = (id: number) => `ccm-chat-draft-${id}`;

    beforeEach(() => {
      localStorage.clear();
    });

    it('persists typed input to localStorage', async () => {
      const task = makeTask({ id: 7 });
      render(<ChatView task={task} projects={projects} onBack={onBack} onTaskUpdated={onTaskUpdated} />);

      const textarea = screen.getByPlaceholderText(/follow-up message/i);
      fireEvent.change(textarea, { target: { value: 'unsent draft' } });

      await waitFor(() => {
        expect(localStorage.getItem(draftKey(7))).toBe('unsent draft');
      });
    });

    it('restores the draft when re-entering the chat', async () => {
      localStorage.setItem(draftKey(7), 'restored draft');
      const task = makeTask({ id: 7 });
      render(<ChatView task={task} projects={projects} onBack={onBack} onTaskUpdated={onTaskUpdated} />);

      const textarea = screen.getByPlaceholderText(/follow-up message/i) as HTMLTextAreaElement;
      expect(textarea.value).toBe('restored draft');
    });

    it('does not leak drafts between tasks', async () => {
      localStorage.setItem(draftKey(7), 'task seven draft');
      const task = makeTask({ id: 8 });
      render(<ChatView task={task} projects={projects} onBack={onBack} onTaskUpdated={onTaskUpdated} />);

      const textarea = screen.getByPlaceholderText(/follow-up message/i) as HTMLTextAreaElement;
      expect(textarea.value).toBe('');
    });

    it('clears the draft after sending', async () => {
      const task = makeTask({ id: 7 });
      render(<ChatView task={task} projects={projects} onBack={onBack} onTaskUpdated={onTaskUpdated} />);

      const textarea = screen.getByPlaceholderText(/follow-up message/i);
      fireEvent.change(textarea, { target: { value: 'about to send' } });
      await waitFor(() => expect(localStorage.getItem(draftKey(7))).toBe('about to send'));

      fireEvent.keyDown(textarea, { key: 'Enter', code: 'Enter', ctrlKey: true });

      await waitFor(() => {
        expect(localStorage.getItem(draftKey(7))).toBeNull();
      });
    });
  });
});

describe('聊天图片附件展示（2026-07-16 用户反馈：发图后图片不显示）', () => {
  const projects: Project[] = [];
  const onBack = vi.fn();
  const onTaskUpdated = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
    localStorage.clear();
    delete (window as Record<string, unknown>).Capacitor;
  });

  function wsUserMessage(taskId: number, data: Record<string, unknown>) {
    capturedOnMessage!({
      channel: `task:${taskId}`,
      data: { event_type: 'user_message', ...data },
    });
  }

  it('WS user_message 与已展示消息内容重复时，附件必须合并进已展示消息（去重不能吃掉图片）', async () => {
    const task = makeTask({ id: 11 });
    render(<ChatView task={task} projects={projects} onBack={onBack} onTaskUpdated={onTaskUpdated} />);
    await waitFor(() => expect(api.getTaskChatHistory).toHaveBeenCalled());

    // 第一条：乐观回显场景 —— 只有文字、无附件
    await act(async () => {
      wsUserMessage(11, { content: '看下这张截图', image_urls: null, attachments: null });
    });
    expect(screen.getByText('看下这张截图')).toBeInTheDocument();

    // 第二条：服务端广播 —— 同样内容但带图片附件（真实发送时后端会广播这条）
    await act(async () => {
      wsUserMessage(11, {
        content: '看下这张截图',
        image_urls: ['/api/uploads/shot.png'],
        attachments: [{ url: '/api/uploads/shot.png', name: 'shot.png', is_image: true }],
      });
    });

    // 不应产生重复文本消息
    expect(screen.getAllByText('看下这张截图')).toHaveLength(1);
    // 但图片必须显示出来（去重时合并附件，而不是整条丢弃）
    const imgs = document.querySelectorAll('img[src*="/api/uploads/shot.png"]');
    expect(imgs.length).toBeGreaterThan(0);
  });

  it('Capacitor（手机 App）下附件相对 URL 必须拼上远程服务器地址', async () => {
    (window as Record<string, unknown>).Capacitor = {};
    localStorage.setItem('cc_server_url', 'https://ccm.example.com');
    const task = makeTask({ id: 12 });
    render(<ChatView task={task} projects={projects} onBack={onBack} onTaskUpdated={onTaskUpdated} />);
    await waitFor(() => expect(api.getTaskChatHistory).toHaveBeenCalled());

    await act(async () => {
      wsUserMessage(12, {
        content: '手机发图',
        image_urls: ['/api/uploads/phone.png'],
        attachments: [
          { url: '/api/uploads/phone.png', name: 'phone.png', is_image: true },
          { url: '/api/uploads/doc.pdf', name: 'doc.pdf', is_image: false },
        ],
      });
    });

    const img = document.querySelector('img[src="https://ccm.example.com/api/uploads/phone.png"]');
    expect(img).not.toBeNull();
    const link = document.querySelector('a[href="https://ccm.example.com/api/uploads/doc.pdf"]');
    expect(link).not.toBeNull();
  });

  it('初始 Prompt 气泡渲染 task.metadata_.attachments 里的图片', () => {
    const task = makeTask({
      id: 13,
      description: '看图建任务',
      metadata_: {
        attachments: [{ url: '/api/uploads/init.png', name: 'init.png', is_image: true }],
      },
    } as Partial<Task>);
    render(<ChatView task={task} projects={projects} onBack={onBack} onTaskUpdated={onTaskUpdated} />);
    const img = document.querySelector('img[src*="/api/uploads/init.png"]');
    expect(img).not.toBeNull();
  });
});

describe('Codex app-server 增量消息', () => {
  const projects: Project[] = [];
  const onBack = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
    (api.getTaskChatHistory as ReturnType<typeof vi.fn>).mockResolvedValue([]);
  });

  it('按 item_id 合并 delta，并用最终消息原位替换而不重复', async () => {
    const task = makeTask({ id: 21, provider: 'codex' });
    render(<ChatView task={task} projects={projects} onBack={onBack} />);
    await waitFor(() => expect(api.getTaskChatHistory).toHaveBeenCalled());

    await act(async () => {
      capturedOnMessage!({
        channel: 'task:21',
        data: { event_type: 'message_delta', item_id: 'msg-1', content: 'Hel' },
      });
      capturedOnMessage!({
        channel: 'task:21',
        data: { event_type: 'message_delta', item_id: 'msg-1', content: 'lo' },
      });
    });
    expect(screen.getAllByText('Hello')).toHaveLength(1);

    await act(async () => {
      capturedOnMessage!({
        channel: 'task:21',
        data: {
          event_type: 'message', item_id: 'msg-1', role: 'assistant',
          content: 'Hello', is_error: false,
        },
      });
    });
    expect(screen.getAllByText('Hello')).toHaveLength(1);
  });
});

describe('failed attachment sending', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    (api.getTaskChatHistory as ReturnType<typeof vi.fn>).mockResolvedValue([]);
    (api.getAskUserPending as ReturnType<typeof vi.fn>).mockResolvedValue({ pending: [] });
  });

  it('keeps Send disabled instead of posting an empty attachment placeholder', async () => {
    (api.uploadImages as ReturnType<typeof vi.fn>)
      .mockRejectedValueOnce(new Error('upload failed'));
    render(<ChatView task={makeTask()} projects={[]} onBack={vi.fn()} />);

    const input = document.querySelector('input[type="file"]') as HTMLInputElement;
    fireEvent.change(input, {
      target: { files: [new File(['evidence'], 'evidence.txt', { type: 'text/plain' })] },
    });
    await screen.findByTitle('Click to retry');

    const send = screen.getByTitle('Retry or remove failed attachments before sending');
    expect(send).toBeDisabled();
    expect(api.sendTaskChat).not.toHaveBeenCalled();
  });
});
