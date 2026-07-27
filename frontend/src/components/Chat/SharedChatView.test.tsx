import { act, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { SharedChatView } from './SharedChatView';
import type { SharedTaskReceived } from '../../api/client';

vi.mock('../../api/client', () => ({
  api: {
    getSharedHistory: vi.fn().mockResolvedValue([]),
    getSharedConfig: vi.fn().mockResolvedValue({}),
    sendSharedChat: vi.fn().mockResolvedValue({ ok: true }),
  },
}));

import { api } from '../../api/client';

class MockWebSocket {
  static latest: MockWebSocket | null = null;
  onopen: (() => void) | null = null;
  onclose: (() => void) | null = null;
  onerror: (() => void) | null = null;
  onmessage: ((event: { data: string }) => void) | null = null;

  constructor(public url: string) {
    MockWebSocket.latest = this;
  }

  close() {}

  emit(payload: object) {
    this.onmessage?.({ data: JSON.stringify(payload) });
  }
}

const shared: SharedTaskReceived = {
  id: 3,
  owner_ccm_url: 'https://owner.example.com',
  owner_name: 'Owner',
  remote_task_id: 17,
  share_token: 'share-token',
  task_title: 'Shared task',
};

describe('SharedChatView', () => {
  beforeEach(() => {
    vi.stubGlobal('WebSocket', MockWebSocket);
    vi.mocked(api.getSharedHistory).mockReset().mockResolvedValue([]);
    vi.mocked(api.getSharedConfig).mockReset().mockResolvedValue({});
    vi.mocked(api.sendSharedChat).mockReset().mockResolvedValue({ ok: true });
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    vi.clearAllMocks();
    MockWebSocket.latest = null;
  });

  it('reconciles the server echo with its optimistic user bubble', async () => {
    render(<SharedChatView shared={shared} onBack={vi.fn()} />);
    await waitFor(() => expect(api.getSharedHistory).toHaveBeenCalled());

    const input = screen.getByPlaceholderText('Send a message...');
    await userEvent.type(input, 'hello');
    await userEvent.click(input.parentElement!.querySelector('button')!);
    await waitFor(() => expect(api.sendSharedChat).toHaveBeenCalledWith(3, 'hello'));

    act(() => {
      MockWebSocket.latest?.emit({
        data: {
          event_type: 'user_message',
          role: 'user',
          content: '[Alice] hello',
          raw_content: 'hello',
        },
      });
    });

    expect(screen.getAllByText('[Alice] hello')).toHaveLength(1);
    expect(screen.queryByText('hello')).not.toBeInTheDocument();
  });

  it('rolls back an unconfirmed optimistic bubble and restores input on send failure', async () => {
    vi.mocked(api.sendSharedChat).mockRejectedValueOnce(new Error('owner offline'));
    render(<SharedChatView shared={shared} onBack={vi.fn()} />);
    await waitFor(() => expect(api.getSharedHistory).toHaveBeenCalled());

    const input = screen.getByPlaceholderText('Send a message...');
    await userEvent.type(input, 'not delivered');
    await userEvent.click(input.parentElement!.querySelector('button')!);

    await waitFor(() => {
      expect(screen.getByText('Error: owner offline')).toBeInTheDocument();
      expect(input).toHaveValue('not delivered');
    });
    expect(screen.queryByText('not delivered')).not.toBeInTheDocument();
  });

  it('does not let a late initial history snapshot erase optimistic or WS messages', async () => {
    let resolveHistory!: (messages: []) => void;
    vi.mocked(api.getSharedHistory).mockReturnValueOnce(
      new Promise<[]>((resolve) => { resolveHistory = resolve; }),
    );
    render(<SharedChatView shared={shared} onBack={vi.fn()} />);

    const input = screen.getByPlaceholderText('Send a message...');
    await userEvent.type(input, 'optimistic message');
    await userEvent.click(input.parentElement!.querySelector('button')!);
    act(() => {
      MockWebSocket.latest?.emit({
        data: {
          event_type: 'message',
          role: 'assistant',
          content: 'live answer',
          id: 101,
          timestamp: '2026-01-01T00:00:01Z',
        },
      });
    });

    await act(async () => { resolveHistory([]); });

    expect(screen.getByText('optimistic message')).toBeInTheDocument();
    expect(screen.getByText('live answer')).toBeInTheDocument();
  });

  it('does not mistake an older identical history row for a new optimistic send', async () => {
    let resolveInitialHistory!: (messages: Array<{
      id: number;
      role: string;
      event_type: string;
      content: string;
      raw_content: string;
    }>) => void;
    vi.mocked(api.getSharedHistory).mockReturnValueOnce(
      new Promise((resolve) => { resolveInitialHistory = resolve; }),
    );
    render(<SharedChatView shared={shared} onBack={vi.fn()} />);

    const input = screen.getByPlaceholderText('Send a message...');
    await userEvent.type(input, 'repeat me');
    await userEvent.click(input.parentElement!.querySelector('button')!);
    await waitFor(() => expect(api.sendSharedChat).toHaveBeenCalledWith(3, 'repeat me'));

    await act(async () => {
      resolveInitialHistory([{
        id: 5,
        role: 'user',
        event_type: 'user_message',
        content: 'repeat me',
        raw_content: 'repeat me',
      }]);
    });

    expect(screen.getAllByText('repeat me')).toHaveLength(2);
  });

  it('lets a post-send history request confirm its optimistic bubble', async () => {
    vi.mocked(api.getSharedHistory)
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([{
        id: 9,
        role: 'user',
        event_type: 'user_message',
        content: '[Owner] delivered',
        raw_content: 'delivered',
      }]);
    render(<SharedChatView shared={shared} onBack={vi.fn()} />);
    await waitFor(() => expect(api.getSharedHistory).toHaveBeenCalledTimes(1));

    const input = screen.getByPlaceholderText('Send a message...');
    await userEvent.type(input, 'delivered');
    await userEvent.click(input.parentElement!.querySelector('button')!);

    await waitFor(() => {
      expect(screen.getByText('[Owner] delivered')).toBeInTheDocument();
    });
    expect(screen.queryByText(/^delivered$/)).not.toBeInTheDocument();
  });
});
