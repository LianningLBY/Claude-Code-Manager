import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import { MonitorPanel } from './MonitorPanel';

vi.mock('../../api/client', () => ({
  api: {
    listMonitorSessions: vi.fn(() => Promise.resolve([])),
    getMonitorChecks: vi.fn(() => Promise.resolve([])),
    stopMonitorSession: vi.fn(),
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
});
