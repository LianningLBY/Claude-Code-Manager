import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';

import type { Task } from '../../api/client';
import { RelatedPlansDialog } from './RelatedPlansDialog';

function readyPlan(): Task {
  return {
    id: 71,
    title: 'Review workspace Plan',
    mode: 'plan',
    status: 'plan_review',
    plan_content: 'Inspect and implement the change',
    plan_target_task_id: 7,
    provider: 'codex',
    model: 'gpt-5.6-sol',
    codex_service_tier: 'default',
  } as Task;
}

function renderDialog(overrides: Partial<Parameters<typeof RelatedPlansDialog>[0]> = {}) {
  const onDelete = vi.fn().mockResolvedValue(true);
  render(<RelatedPlansDialog
    open
    taskId={7}
    plans={[readyPlan()]}
    loading={false}
    error={null}
    creating={false}
    busyId={null}
    selectedPlanIds={[]}
    staleIds={new Set()}
    createInput=""
    onCreateInputChange={vi.fn()}
    onCreate={vi.fn().mockResolvedValue(true)}
    onApprove={vi.fn().mockResolvedValue(undefined)}
    onReject={vi.fn().mockResolvedValue(undefined)}
    onRevise={vi.fn().mockResolvedValue(null)}
    onCancel={vi.fn().mockResolvedValue(undefined)}
    onDelete={onDelete}
    onToggleAttachment={vi.fn()}
    onClose={vi.fn()}
    {...overrides}
  />);
  return { onDelete };
}

describe('RelatedPlansDialog', () => {
  it('owns one close control at the dialog shell', () => {
    renderDialog();

    const closeButtons = screen.getAllByRole('button', { name: 'Close Plans' });
    expect(closeButtons).toHaveLength(1);
    expect(closeButtons[0].parentElement).toHaveAttribute('role', 'dialog');
    const statusBadges = screen.getAllByText('Ready · decision needed')
      .filter((badge) => badge.closest('header'));
    expect(statusBadges).toHaveLength(2);
    expect(statusBadges[0]).toHaveClass('sm:hidden');
    expect(statusBadges[1].parentElement).toHaveClass('hidden', 'sm:block');
  });

  it('keeps the shell close control when there are no Plans', async () => {
    const onClose = vi.fn();
    renderDialog({ plans: [], onClose });

    expect(screen.getByText('No Plans yet. Creating one will not interrupt the current session.')).toBeInTheDocument();
    await userEvent.click(screen.getByRole('button', { name: 'Close Plans' }));

    expect(onClose).toHaveBeenCalledOnce();
  });

  it('uses a New Task-style multiline Plan composer', () => {
    renderDialog({ plans: [] });

    const composer = screen.getByPlaceholderText('Create an independent Plan…');
    expect(composer.tagName).toBe('TEXTAREA');
    expect(composer).toHaveAttribute('rows', '3');
    expect(screen.getByRole('button', { name: 'Attach files' })).toBeInTheDocument();
  });

  it('renders selected Plan content as Markdown', () => {
    renderDialog({
      plans: [{
        ...readyPlan(),
        plan_content: [
          '# Implementation heading',
          '',
          'Use the **shared renderer**.',
          '',
          '```ts',
          'const ready = true;',
          '```',
        ].join('\n'),
      }],
    });

    expect(screen.getByRole('heading', {
      level: 1,
      name: 'Implementation heading',
    })).toBeInTheDocument();
    expect(screen.getByText('shared renderer').tagName).toBe('STRONG');
    const code = screen.getByText(/const ready = true/);
    expect(code.tagName).toBe('CODE');
    expect(code.closest('pre')).not.toBeNull();
    expect(screen.getByTitle('Copy')).toBeInTheDocument();
    expect(screen.queryByText('# Implementation heading')).not.toBeInTheDocument();
  });

  it('offers deletion from the selected Plan detail', async () => {
    const { onDelete } = renderDialog();

    await userEvent.click(screen.getByRole('button', {
      name: 'Delete Plan #71',
    }));

    expect(onDelete).toHaveBeenCalledWith(71);
  });
});
