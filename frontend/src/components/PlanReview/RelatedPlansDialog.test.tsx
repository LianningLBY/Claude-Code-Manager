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
    onCreate={vi.fn().mockResolvedValue(undefined)}
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
  it('keeps the mobile and desktop close controls mutually exclusive', () => {
    renderDialog();

    const closeButtons = screen.getAllByRole('button', { name: 'Close Plans' });
    expect(closeButtons).toHaveLength(2);
    expect(closeButtons[0]).toHaveClass('sm:hidden');
    expect(closeButtons[1]).toHaveClass('hidden', 'sm:block');
    const statusBadges = screen.getAllByText('Ready · decision needed')
      .filter((badge) => badge.closest('header'));
    expect(statusBadges).toHaveLength(2);
    expect(statusBadges[0]).toHaveClass('sm:hidden');
    expect(statusBadges[1].parentElement).toHaveClass('hidden', 'sm:block');
  });

  it('offers deletion from the selected Plan detail', async () => {
    const { onDelete } = renderDialog();

    await userEvent.click(screen.getByRole('button', {
      name: 'Delete Plan #71',
    }));

    expect(onDelete).toHaveBeenCalledWith(71);
  });
});
