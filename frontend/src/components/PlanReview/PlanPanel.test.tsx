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
    deleteTask: vi.fn().mockResolvedValue({ ok: true }),
    createPlanExecutionTask: vi.fn().mockResolvedValue({
      execution_task: { id: 99 },
    }),
  },
}));

import { api } from '../../api/client';

describe('PlanPanel', () => {
  beforeEach(() => {
    vi.restoreAllMocks();
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
    await userEvent.click(screen.getByRole('button', { name: 'Review Plan #41' }));
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
    expect(screen.queryByPlaceholderText('Describe changes for a new revision…')).not.toBeInTheDocument();
    await userEvent.click(screen.getByRole('button', { name: 'Review Plan #42' }));
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
    await userEvent.click(screen.getByRole('button', { name: 'Review Plan #43' }));
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
    await userEvent.click(screen.getByRole('button', { name: 'Review Plan #44' }));
    await userEvent.click(screen.getByRole('button', { name: 'Approve only' }));

    expect(JSON.parse(localStorage.getItem('ccm-plan-dismissed-7') || '[]')).toContain(44);
    expect(window.location.hash).toBe('');
  });

  it('renders a compact row and opens the full Plan in a review dialog', async () => {
    const task = {
      id: 45,
      title: 'Compact review',
      mode: 'plan',
      status: 'plan_review',
      plan_content: 'Hidden until requested',
      plan_target_task_id: null,
      provider: 'codex',
      model: 'gpt-5.6-sol',
      codex_service_tier: 'default',
    } as Task;

    render(<PlanPanel tasks={[task]} onRefresh={vi.fn()} />);

    expect(screen.queryByText('Hidden until requested')).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Approve only' })).not.toBeInTheDocument();
    await userEvent.click(screen.getByRole('button', { name: 'Review Plan #45' }));
    expect(screen.getByRole('dialog', { name: 'Review Plan #45' })).toBeInTheDocument();
    expect(screen.getByText('Hidden until requested')).toBeInTheDocument();
    await userEvent.click(screen.getByRole('button', { name: 'Close Plan review' }));
    expect(screen.queryByText('Hidden until requested')).not.toBeInTheDocument();
  });

  it('renders standalone Plan detail content as Markdown', async () => {
    const task = {
      id: 48,
      title: 'Markdown review',
      mode: 'plan',
      status: 'plan_review',
      plan_content: [
        '# Standalone heading',
        '',
        'Keep this **readable**.',
        '',
        '```bash',
        'npm run build',
        '```',
      ].join('\n'),
      plan_target_task_id: null,
      provider: 'codex',
      model: 'gpt-5.6-sol',
      codex_service_tier: 'default',
    } as Task;

    render(<PlanPanel tasks={[task]} onRefresh={vi.fn()} />);
    await userEvent.click(screen.getByRole('button', { name: 'Review Plan #48' }));

    expect(screen.getByRole('heading', {
      level: 1,
      name: 'Standalone heading',
    })).toBeInTheDocument();
    expect(screen.getByText('readable').tagName).toBe('STRONG');
    const code = screen.getByText(/npm run build/);
    expect(code.tagName).toBe('CODE');
    expect(code.closest('pre')).not.toBeNull();
    expect(screen.queryByText('# Standalone heading')).not.toBeInTheDocument();
  });

  it('keeps the approved standalone action directly on the compact row', async () => {
    const task = {
      id: 47,
      title: 'Approved Plan',
      mode: 'plan',
      status: 'completed',
      plan_content: 'Ready for execution',
      plan_approved: true,
      plan_target_task_id: null,
      plan_execution_task_id: null,
      provider: 'codex',
      model: 'gpt-5.6-sol',
      codex_service_tier: 'default',
    } as Task;

    render(<PlanPanel tasks={[task]} onRefresh={vi.fn()} />);

    expect(screen.queryByRole('button', { name: 'Review Plan #47' })).not.toBeInTheDocument();
    await userEvent.click(screen.getByRole('button', { name: 'Create execution Task' }));
    expect(api.approvePlan).not.toHaveBeenCalled();
    expect(api.createPlanExecutionTask).toHaveBeenCalledWith(47);
    window.location.hash = '';
  });

  it('deletes a reviewed Plan after explicit confirmation', async () => {
    vi.spyOn(window, 'confirm').mockReturnValue(true);
    const task = {
      id: 46,
      title: 'Disposable review',
      mode: 'plan',
      status: 'plan_review',
      plan_content: 'Remove this',
      plan_target_task_id: null,
      provider: 'claude',
      model: 'claude-fable-5',
      codex_service_tier: 'default',
    } as Task;
    const onRefresh = vi.fn();

    render(<PlanPanel tasks={[task]} onRefresh={onRefresh} />);
    await userEvent.click(screen.getByRole('button', {
      name: 'Delete Plan #46',
    }));

    expect(api.deleteTask).toHaveBeenCalledWith(46);
    await waitFor(() => expect(onRefresh).toHaveBeenCalled());
  });
});
