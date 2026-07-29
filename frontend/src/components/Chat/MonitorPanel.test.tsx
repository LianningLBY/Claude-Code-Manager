import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { api } from '../../api/client';
import type { MonitorSession } from '../../api/client';
import { MonitorPanel } from './MonitorPanel';

vi.mock('../../api/client', () => ({
  api: {
    listMonitorSessions: vi.fn(() => Promise.resolve([])),
    getMonitorChecks: vi.fn(() => Promise.resolve([])),
    deleteMonitorSession: vi.fn(() => Promise.resolve({ ok: true })),
  },
}));

const baseProps = {
  taskId: 1,
  sessions: [],
  onSessionsChange: vi.fn(),
  onClose: vi.fn(),
};

describe('MonitorPanel codex annotation', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('shows the split capability notice for codex tasks', () => {
    render(<MonitorPanel {...baseProps} provider="codex" />);
    expect(
      screen.getByText('Sub-Agent 已支持 Codex；后台 Monitor 仍仅支持 Claude'),
    ).toBeInTheDocument();
  });

  it('shows no notice for claude tasks', () => {
    render(<MonitorPanel {...baseProps} provider="claude" />);
    expect(
      screen.queryByText('Sub-Agent 已支持 Codex；后台 Monitor 仍仅支持 Claude'),
    ).not.toBeInTheDocument();
  });

  it('shows no notice when provider is omitted', () => {
    render(<MonitorPanel {...baseProps} />);
    expect(
      screen.queryByText(/暂不支持 Codex/),
    ).not.toBeInTheDocument();
  });

  it('allows an existing internal Codex Monitor to be stopped', async () => {
    const session: MonitorSession = {
      id: 7,
      task_id: 1,
      agent_type: 'monitor',
      source: 'ccm',
      description: 'PR7B1 active-turn stop fixture',
      monitor_context: null,
      interval: 300,
      max_checks: 10,
      model: 'gpt-5.6-sol',
      provider: 'codex',
      status: 'running',
      checks_done: 0,
      last_summary: null,
      next_check_at: null,
      turn_generation: 1,
      active_turn_generation: 1,
      consecutive_failures: 0,
      last_error: null,
      codex_cleanup_pending: false,
      codex_cleanup_error: null,
      created_at: '2026-07-29T00:00:00Z',
      completed_at: null,
    };

    render(
      <MonitorPanel
        {...baseProps}
        provider="codex"
        sessions={[session]}
      />,
    );
    await userEvent.click(screen.getByTitle('Stop monitor'));

    expect(api.deleteMonitorSession).toHaveBeenCalledWith(1, 7);
  });

  it('shows and retries durable Codex cleanup failures', async () => {
    const session: MonitorSession = {
      id: 8,
      task_id: 1,
      agent_type: 'monitor',
      source: 'ccm',
      description: 'terminal cleanup fixture',
      monitor_context: null,
      interval: 300,
      max_checks: 10,
      model: 'gpt-5.6-sol',
      provider: 'codex',
      status: 'cancelled',
      checks_done: 0,
      last_summary: null,
      next_check_at: null,
      turn_generation: 1,
      active_turn_generation: null,
      consecutive_failures: 0,
      last_error: null,
      codex_cleanup_pending: true,
      codex_cleanup_error: 'thread/delete transport unavailable',
      created_at: '2026-07-29T00:00:00Z',
      completed_at: '2026-07-29T00:01:00Z',
    };

    render(
      <MonitorPanel
        {...baseProps}
        provider="codex"
        sessions={[session]}
      />,
    );

    expect(
      screen.getByText('Codex runtime cleanup pending'),
    ).toBeInTheDocument();
    expect(
      screen.getByText(/thread\/delete transport unavailable/),
    ).toBeInTheDocument();
    await userEvent.click(screen.getByTitle('Retry Codex cleanup'));

    expect(api.deleteMonitorSession).toHaveBeenCalledWith(1, 8);
  });

  it('refreshes the durable row when Stop reports cleanup failure', async () => {
    const session: MonitorSession = {
      id: 9,
      task_id: 1,
      agent_type: 'monitor',
      source: 'ccm',
      description: 'cleanup transition fixture',
      monitor_context: null,
      interval: 300,
      max_checks: 10,
      model: 'gpt-5.6-sol',
      provider: 'codex',
      status: 'running',
      checks_done: 0,
      last_summary: null,
      next_check_at: null,
      turn_generation: 1,
      active_turn_generation: 1,
      consecutive_failures: 0,
      last_error: null,
      codex_cleanup_pending: false,
      codex_cleanup_error: null,
      created_at: '2026-07-29T00:00:00Z',
      completed_at: null,
    };
    vi.mocked(api.deleteMonitorSession).mockRejectedValueOnce(
      new Error('409 cleanup pending'),
    );

    render(
      <MonitorPanel
        {...baseProps}
        provider="codex"
        sessions={[session]}
      />,
    );
    await waitFor(() => {
      expect(api.listMonitorSessions).toHaveBeenCalled();
    });
    vi.mocked(api.listMonitorSessions).mockClear();

    await userEvent.click(screen.getByTitle('Stop monitor'));

    await waitFor(() => {
      expect(api.listMonitorSessions).toHaveBeenCalledTimes(1);
    });
  });
});
