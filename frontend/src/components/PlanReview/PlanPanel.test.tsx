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
    createPlanExecutionTask: vi.fn().mockResolvedValue({
      execution_task: { id: 99 },
    }),
  },
}));

import { api } from '../../api/client';

describe('PlanPanel', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    localStorage.removeItem('ccm-plan-dismissed-7');
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
    await userEvent.click(screen.getByRole('button', { name: 'Approve only' }));

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
    render(<PlanPanel tasks={[task]} onRefresh={onRefresh} />);
    expect(screen.getByText('Revision of #41')).toBeInTheDocument();
    await userEvent.type(
      screen.getByPlaceholderText('Describe changes for a new revision…'),
      'Add a rollback phase',
    );
    await userEvent.click(screen.getByRole('button', { name: 'Revise' }));

    expect(api.revisePlan).toHaveBeenCalledWith(42, 'Add a rollback phase');
    await waitFor(() => expect(onRefresh).toHaveBeenCalled());
  });

  it('approves and creates an execution Task from the primary standalone action', async () => {
    const task = {
      id: 43,
      title: 'Ready to execute',
      mode: 'plan',
      status: 'plan_review',
      plan_content: 'Ship the implementation',
      plan_target_task_id: null,
      provider: 'codex',
      model: 'gpt-5.6-sol',
      codex_service_tier: 'default',
    } as Task;

    render(<PlanPanel tasks={[task]} onRefresh={vi.fn()} />);
    await userEvent.click(screen.getByRole('button', {
      name: 'Approve & create execution Task',
    }));

    await waitFor(() => expect(api.approvePlan).toHaveBeenCalledWith(43, {
      provider: 'codex',
      model: 'gpt-5.6-sol',
      codex_service_tier: 'default',
    }));
    expect(api.createPlanExecutionTask).toHaveBeenCalledWith(43);
    expect(window.location.hash).toBe('#/tasks/chat/99');
    window.location.hash = '';
  });

  it('keeps related Plan approval and composer attachment explicit', async () => {
    const task = {
      id: 44,
      title: 'Related Plan',
      mode: 'plan',
      status: 'plan_review',
      plan_content: 'Apply this later',
      plan_target_task_id: 7,
      provider: 'claude',
      model: 'claude-fable-5',
      codex_service_tier: 'default',
    } as Task;

    render(<PlanPanel tasks={[task]} onRefresh={vi.fn()} />);
    await userEvent.click(screen.getByRole('button', { name: 'Approve only' }));

    expect(JSON.parse(localStorage.getItem('ccm-plan-dismissed-7') || '[]')).toContain(44);
    expect(window.location.hash).toBe('');
  });
});
