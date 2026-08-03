import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { api, type PlanResource, type PlanVersion } from '../../api/client';
import { PlanDetail } from './PlanDetail';

vi.mock('../../api/client', () => ({
  isApiRequestError: () => false,
  api: {
    listPlanVersions: vi.fn(),
    listPlanResourceRuns: vi.fn().mockResolvedValue([]),
    getPlanVersionStaleness: vi.fn().mockResolvedValue({
      stale: false,
      hard_conflict: false,
      reasons: [],
      hard_conflicts: [],
      can_confirm: false,
    }),
  },
}));

function version(overrides: Partial<PlanVersion>): PlanVersion {
  return {
    id: 12,
    plan_id: 4,
    version_number: 2,
    parent_version_id: 11,
    produced_by_run_id: 22,
    produced_by_step_id: 32,
    content: '# Current proposal',
    context_session_id: 'session-1',
    context_log_id: 41,
    repo_revision: { head: 'planner-head' },
    reviewer_repo_revision: { head: 'reviewer-head' },
    review_verdict: 'approve',
    review_feedback: null,
    reviewed_by_step_id: 33,
    review_exhausted: false,
    reviewed_at: '2026-08-02T08:00:00Z',
    human_decision: 'pending',
    decided_at: null,
    decided_by: null,
    superseded_by_version_id: null,
    applied: false,
    created_at: '2026-08-02T08:00:00Z',
    ...overrides,
  };
}

function plan(current: PlanVersion, prior: PlanVersion): PlanResource {
  return {
    id: 4,
    title: 'Version history',
    initial_request: 'Design the migration',
    initial_attachments: null,
    target_task_id: null,
    project_id: 7,
    target_repo: '/repo',
    target_branch: 'main',
    worker_id: null,
    priority: 0,
    timeout_hours: null,
    created_by: 1,
    current_version_id: current.id,
    active_run_id: null,
    forked_from_version_id: null,
    archived_at: null,
    closed_at: null,
    lock_version: 3,
    created_at: '2026-08-02T08:00:00Z',
    updated_at: '2026-08-02T09:00:00Z',
    display_state: 'awaiting_review',
    legacy: false,
    latest_run_status: 'completed',
    latest_run_error: null,
    pipeline_config: {
      version: 1,
      planner: {
        primary: { provider: 'codex', model: 'gpt-5.6-sol', effort: 'ultra' },
        fallback: { provider: 'claude', model: 'claude-opus-4-6', effort: 'high' },
      },
      reviewer: {
        enabled: true,
        primary: { provider: 'claude', model: 'claude-opus-4-6', effort: 'high' },
        fallback: { provider: 'codex', model: 'gpt-5.6-terra', effort: 'high' },
      },
      max_revision_cycles: 2,
      max_interactions: 5,
    },
    application: {
      id: 51,
      plan_id: 4,
      plan_version_id: prior.id,
      application_type: 'execution_task',
      target_task_id: null,
      target_session_id: null,
      user_log_id: null,
      execution_task_id: 91,
      created_at: '2026-08-02T08:30:00Z',
    },
    applications: [{
      id: 51,
      plan_id: 4,
      plan_version_id: prior.id,
      application_type: 'execution_task',
      target_task_id: null,
      target_session_id: null,
      user_log_id: null,
      execution_task_id: 91,
      created_at: '2026-08-02T08:30:00Z',
    }],
    current_version: current,
    active_run: null,
    open_input_request: null,
  };
}

describe('PlanDetail', () => {
  beforeEach(() => vi.clearAllMocks());

  it('keeps an applied older Version, current review actions, routes, and execution link visible', async () => {
    const prior = version({
      id: 11,
      version_number: 1,
      parent_version_id: null,
      content: '# Applied proposal',
      human_decision: 'approved',
      applied: true,
    });
    const current = version({});
    vi.mocked(api.listPlanVersions).mockResolvedValue([current, prior]);
    const navigate = vi.fn();

    render(<PlanDetail plan={plan(current, prior)} onRefresh={vi.fn()} onNavigateTask={navigate} />);

    expect(await screen.findByText('v1 applied')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Approve v2 & create execution Task' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Approve v2 only' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Reject v2' })).toBeInTheDocument();
    expect(screen.getByText(/Input pauses: 5/)).toBeInTheDocument();

    await userEvent.click(screen.getByRole('button', { name: 'Open v1 execution Task #91' }));
    expect(navigate).toHaveBeenCalledWith(91);
    await userEvent.selectOptions(screen.getByRole('combobox', { name: 'Plan Version' }), '11');
    expect(await screen.findByRole('heading', { level: 1, name: 'Applied proposal' })).toBeInTheDocument();
    expect(screen.getByText(/Historical Version/)).toBeInTheDocument();
  });

  it('shows a confirmable warning for a migrated Version without blocking decisions', async () => {
    const current = version({ repo_revision: null, reviewer_repo_revision: null });
    const prior = version({ id: 11, version_number: 1 });
    vi.mocked(api.listPlanVersions).mockResolvedValue([current, prior]);
    vi.mocked(api.getPlanVersionStaleness).mockResolvedValueOnce({
      stale: true,
      reasons: ['captured_repository_state_missing'],
      hard_conflict: false,
      hard_conflicts: [],
      can_confirm: true,
      current_log_id: null,
      current_repo_revision: { available: true, head: 'current' },
    });

    render(<PlanDetail plan={plan(current, prior)} onRefresh={vi.fn()} />);

    expect(await screen.findByText(/no historical repository snapshot/)).toBeInTheDocument();
    expect(screen.getByText(/refreshing or re-planning is optional/i)).toBeInTheDocument();
    expect(screen.queryByText(/This action is blocked/)).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Approve v2 & create execution Task' })).toBeEnabled();
    expect(screen.getByRole('button', { name: 'Reject v2' })).toBeEnabled();
  });
});
