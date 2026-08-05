import { beforeEach, describe, expect, it, vi } from 'vitest';
import { useState } from 'react';
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';

import { AttentionTag } from './AttentionTag';

vi.mock('../../api/client', () => ({
  api: {
    updateTask: vi.fn(),
  },
}));

import { api } from '../../api/client';

describe('AttentionTag', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('renders a prominent tag and asks the parent to edit it', async () => {
    const onEdit = vi.fn();
    render(
      <AttentionTag
        taskId={12}
        value="等它结束后再看"
        editing={false}
        onEdit={onEdit}
        onCancel={vi.fn()}
      />,
    );

    await userEvent.click(screen.getByTitle('Edit attention tag'));
    expect(screen.getByText('等它结束后再看')).toBeInTheDocument();
    expect(onEdit).toHaveBeenCalledTimes(1);
  });

  it('trims and saves an edited tag', async () => {
    const onSaved = vi.fn();
    vi.mocked(api.updateTask).mockResolvedValue({
      id: 12,
      attention_tag: '今晚继续',
    } as never);
    render(
      <AttentionTag
        taskId={12}
        value="旧标签"
        editing
        onEdit={vi.fn()}
        onCancel={vi.fn()}
        onSaved={onSaved}
      />,
    );

    const input = screen.getByLabelText('Attention tag');
    await userEvent.clear(input);
    await userEvent.type(input, '  今晚继续  ');
    await userEvent.click(screen.getByTitle('Save attention tag'));

    await waitFor(() => {
      expect(api.updateTask).toHaveBeenCalledWith(12, {
        attention_tag: '今晚继续',
      });
    });
    expect(onSaved).toHaveBeenCalledTimes(1);
  });

  it('saves a blank tag as null', async () => {
    vi.mocked(api.updateTask).mockResolvedValue({
      id: 12,
      attention_tag: null,
    } as never);
    render(
      <AttentionTag
        taskId={12}
        value="删掉我"
        editing
        onEdit={vi.fn()}
        onCancel={vi.fn()}
      />,
    );

    await userEvent.clear(screen.getByLabelText('Attention tag'));
    await userEvent.click(screen.getByTitle('Save attention tag'));

    await waitFor(() => {
      expect(api.updateTask).toHaveBeenCalledWith(12, {
        attention_tag: null,
      });
    });
  });

  it('keeps the saved value visible while the parent refresh is pending', async () => {
    vi.mocked(api.updateTask).mockResolvedValue({
      id: 12,
      attention_tag: '刚刚保存',
    } as never);

    function ControlledAttentionTag() {
      const [editing, setEditing] = useState(true);
      return (
        <AttentionTag
          taskId={12}
          value={null}
          editing={editing}
          onEdit={() => setEditing(true)}
          onCancel={() => setEditing(false)}
        />
      );
    }

    render(<ControlledAttentionTag />);
    await userEvent.type(screen.getByLabelText('Attention tag'), '刚刚保存');
    await userEvent.click(screen.getByTitle('Save attention tag'));

    expect(await screen.findByText('刚刚保存')).toBeInTheDocument();
  });

  it('keeps the draft visible when saving fails', async () => {
    vi.mocked(api.updateTask).mockRejectedValue(new Error('Network unavailable'));
    render(
      <AttentionTag
        taskId={12}
        value={null}
        editing
        onEdit={vi.fn()}
        onCancel={vi.fn()}
      />,
    );

    await userEvent.type(screen.getByLabelText('Attention tag'), '需要确认');
    await userEvent.click(screen.getByTitle('Save attention tag'));

    expect(await screen.findByRole('alert')).toHaveTextContent('Network unavailable');
    expect(screen.getByLabelText('Attention tag')).toHaveValue('需要确认');
  });

  it('shows a compact add action only when requested', async () => {
    const onEdit = vi.fn();
    const { rerender } = render(
      <AttentionTag
        taskId={12}
        value={null}
        editing={false}
        onEdit={onEdit}
        onCancel={vi.fn()}
      />,
    );
    expect(screen.queryByTitle('Add attention tag')).not.toBeInTheDocument();

    rerender(
      <AttentionTag
        taskId={12}
        value={null}
        editing={false}
        onEdit={onEdit}
        onCancel={vi.fn()}
        showAddButton
      />,
    );
    await userEvent.click(screen.getByTitle('Add attention tag'));
    expect(onEdit).toHaveBeenCalledTimes(1);
  });
});
