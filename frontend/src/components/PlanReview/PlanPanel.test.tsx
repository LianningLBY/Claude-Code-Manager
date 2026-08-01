import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import type { Task } from '../../api/client';
import { PlanPanel } from './PlanPanel';

vi.mock('../../api/client', () => ({
  api: {
    approvePlan: vi.fn().mockResolvedValue({}),
    rejectPlan: vi.fn().mockResolvedValue({}),
    revisePlan: vi.fn().mockResolvedValue({}),
  },
}));

import { api } from '../../api/client';

describe('PlanPanel', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('fences approval with the routing tuple displayed by the UI', async () => {
    const task = {
      id: 41,
      title: 'Fast plan',
      mode: 'plan',
      status: 'plan_review',
      plan_content: 'Do the work',
      provider: 'codex',
      model: 'gpt-5.6-sol',
      codex_service_tier: 'priority',
    } as Task;
    const onRefresh = vi.fn();

    render(<PlanPanel tasks={[task]} onRefresh={onRefresh} />);
    await userEvent.click(screen.getByRole('button', { name: /Approve/ }));

    expect(api.approvePlan).toHaveBeenCalledWith(41, {
      provider: 'codex',
      model: 'gpt-5.6-sol',
      codex_service_tier: 'priority',
    });
    await waitFor(() => expect(onRefresh).toHaveBeenCalled());
  });

  it('creates a standalone revision and shows its predecessor', async () => {
    const task = {
      id: 42,
      title: 'Standalone revision',
      mode: 'plan',
      status: 'plan_review',
      plan_content: 'Version two',
      plan_target_task_id: null,
      supersedes_plan_task_id: 41,
      provider: 'claude',
      model: 'claude-fable-5',
      codex_service_tier: 'default',
      metadata_: {},
    } as Task;
    const onRefresh = vi.fn();
    const prompt = vi.spyOn(window, 'prompt').mockReturnValue(
      'Add a rollback phase',
    );

    render(<PlanPanel tasks={[task]} onRefresh={onRefresh} />);
    expect(screen.getByText('Revision of #41')).toBeInTheDocument();
    await userEvent.click(screen.getByRole('button', { name: 'Revise' }));

    expect(api.revisePlan).toHaveBeenCalledWith(42, 'Add a rollback phase');
    await waitFor(() => expect(onRefresh).toHaveBeenCalled());
    prompt.mockRestore();
  });
});
