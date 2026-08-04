import { render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { api, type PlanResource, type PlanRun, type PlanVersion } from '../../api/client';
import { PlanDetail } from './PlanDetail';

vi.mock('../../api/client', () => ({
  isApiRequestError: () => false,
  api: {
    listPlanVersions: vi.fn(),
    listPlanResourceRuns: vi.fn().mockResolvedValue([]),
    createPlanRun: vi.fn(),
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
    display_state: 'awaiting_review',
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
      execution_task_available: true,
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
      execution_task_available: true,
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
      display_state: 'applied',
    });
    const current = version({});
    vi.mocked(api.listPlanVersions).mockResolvedValue([current, prior]);
    const navigate = vi.fn();

    render(<PlanDetail plan={plan(current, prior)} onRefresh={vi.fn()} onNavigateTask={navigate} />);

    expect(await screen.findByText(/v1 applied/)).toBeInTheDocument();
    expect(screen.getByRole('option', { name: 'v2 · Awaiting approval · Current' })).toBeInTheDocument();
    expect(screen.getByRole('option', { name: 'v1 · Applied' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Approve v2 & create execution Task' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Approve v2 only' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Reject v2' })).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Refresh contexts and regenerate Plan' }))
      .not.toBeInTheDocument();
    expect(screen.getByText(/Input pauses: 5/)).toBeInTheDocument();

    await userEvent.click(screen.getByRole('button', { name: 'Open v1 execution Task #91' }));
    expect(navigate).toHaveBeenCalledWith(91);
    await userEvent.selectOptions(screen.getByRole('combobox', { name: 'Plan Version' }), '11');
    expect(await screen.findByRole('heading', { level: 1, name: 'Applied proposal' })).toBeInTheDocument();
    expect(screen.getByText(/Historical Version/)).toBeInTheDocument();
  });

  it('labels an undecided historical Version as superseded and disables a missing execution Task link', async () => {
    const prior = version({
      id: 11,
      version_number: 1,
      parent_version_id: null,
      superseded_by_version_id: 12,
      display_state: 'superseded',
    });
    const current = version({
      human_decision: 'approved',
      applied: true,
      display_state: 'applied',
    });
    const resource = plan(current, prior);
    resource.application = {
      ...resource.applications[0],
      plan_version_id: current.id,
      execution_task_available: false,
    };
    resource.applications = [resource.application];
    vi.mocked(api.listPlanVersions).mockResolvedValue([current, prior]);

    render(<PlanDetail plan={resource} onRefresh={vi.fn()} onNavigateTask={vi.fn()} />);

    expect(await screen.findByRole('option', { name: 'v1 · Superseded (not decided)' })).toBeInTheDocument();
    expect(screen.getByRole('option', { name: 'v2 · Applied · Current' })).toBeInTheDocument();
    expect(screen.getByText('v2 applied · execution Task #91 unavailable')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /Open v2 execution Task/ })).not.toBeInTheDocument();
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
    expect(screen.getByRole('alert')).toHaveClass('text-gray-200', 'bg-amber-500/15');
    expect(screen.getByText('Confirmation required')).toHaveClass('text-amber-300');
    expect(screen.queryByText(/This action is blocked/)).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Approve v2 & create execution Task' })).toBeEnabled();
    expect(screen.getByRole('button', { name: 'Reject v2' })).toBeEnabled();
    expect(screen.getByRole('button', { name: 'Refresh contexts and regenerate Plan' }))
      .toBeInTheDocument();
  });

  it('shows live planning feedback and keeps internal Run identifiers inside Debug information', async () => {
    const prior = version({ id: 11, version_number: 1 });
    const current = version({});
    const activeRun = {
      id: 15,
      plan_id: 4,
      run_type: 'initial',
      status: 'running',
      current_stage: 'planner',
      base_version_id: null,
      source_run_id: null,
      result_version_id: null,
      request_text: 'Design the migration',
      round: 1,
      generation: 1,
      instance_id: 2,
      worker_id: null,
      open_input_request_id: null,
      interaction_count: 0,
      max_interactions: 5,
      execution_seconds: 8,
      last_execution_started_at: '2026-08-03T08:00:00Z',
      review_verdict: null,
      review_feedback: null,
      review_exhausted: false,
      error: null,
      created_at: '2026-08-03T08:00:00Z',
      updated_at: '2026-08-03T08:00:08Z',
      finished_at: null,
      steps: [],
      input_requests: [],
    } satisfies PlanRun;
    const resource = plan(current, prior);
    resource.current_version_id = null;
    resource.current_version = null;
    resource.active_run_id = activeRun.id;
    resource.active_run = activeRun;
    resource.display_state = 'planner';
    resource.latest_run_status = 'running';
    vi.mocked(api.listPlanVersions).mockResolvedValue([]);
    vi.mocked(api.listPlanResourceRuns).mockResolvedValue([activeRun]);

    render(<PlanDetail plan={resource} onRefresh={vi.fn()} />);

    expect(await screen.findByRole('status')).toHaveTextContent('Creating v1 draft');
    expect(screen.queryByRole('region', { name: 'Plan activity' })).not.toBeInTheDocument();

    const debug = screen.getByText('Debug information').closest('details');
    expect(debug).not.toHaveAttribute('open');
    expect(within(debug!).getByText(/Run #15 · initial · running · round 1/))
      .toBeInTheDocument();
  });

  it('keeps failed Run details in Debug and offers an in-place retry', async () => {
    const current = version({});
    const prior = version({ id: 11, version_number: 1 });
    const rawError = 'Claude Plan Agent exited with 1: API Error: 400 tools.3.custom.input_schema';
    const failedRun = {
      id: 16,
      plan_id: 4,
      run_type: 'initial',
      status: 'failed',
      current_stage: 'failed',
      base_version_id: null,
      source_run_id: null,
      result_version_id: null,
      request_text: 'Design the migration',
      round: 1,
      generation: 1,
      instance_id: null,
      worker_id: null,
      open_input_request_id: null,
      interaction_count: 0,
      max_interactions: 5,
      execution_seconds: 2,
      last_execution_started_at: null,
      review_verdict: null,
      review_feedback: null,
      review_exhausted: false,
      error: rawError,
      created_at: '2026-08-03T10:27:22Z',
      updated_at: '2026-08-03T10:27:26Z',
      finished_at: '2026-08-03T10:27:26Z',
      steps: [],
      input_requests: [],
    } satisfies PlanRun;
    const resource = plan(current, prior);
    resource.current_version_id = null;
    resource.current_version = null;
    resource.display_state = 'failed';
    resource.latest_run_status = 'failed';
    resource.latest_run_error = rawError;
    vi.mocked(api.listPlanVersions).mockResolvedValue([]);
    vi.mocked(api.listPlanResourceRuns).mockResolvedValue([failedRun]);
    vi.mocked(api.createPlanRun).mockResolvedValue(failedRun);

    render(<PlanDetail plan={resource} onRefresh={vi.fn()} />);

    const alert = await screen.findByRole('alert');
    expect(alert).toHaveTextContent('Latest planning attempt failed');
    expect(alert).not.toHaveTextContent(rawError);
    expect(screen.queryByRole('region', { name: 'Plan activity' })).not.toBeInTheDocument();
    const debug = screen.getByText('Debug information').closest('details');
    expect(within(debug!).getByText(rawError)).toBeInTheDocument();

    await userEvent.click(screen.getByRole('button', { name: 'Retry planning' }));
    expect(api.createPlanRun).toHaveBeenCalledWith(4, {
      run_type: 'retry',
      request: 'Design the migration',
      base_version_id: undefined,
      expected_current_version_id: undefined,
      source_run_id: 16,
    });
  });

  it('shows the planner draft read-only while the reviewer is running', async () => {
    const prior = version({ id: 11, version_number: 1 });
    const current = version({ review_verdict: null, reviewed_at: null });
    const reviewerRun = {
      id: 20,
      plan_id: 4,
      run_type: 'user_revision',
      status: 'running',
      current_stage: 'reviewer',
      base_version_id: prior.id,
      source_run_id: null,
      result_version_id: null,
      draft_content: '# Candidate proposal',
      draft_step_id: 31,
      draft_repo_revision: { head: 'candidate-head' },
      request_text: 'Tighten the rollout plan',
      round: 1,
      generation: 2,
      instance_id: 2,
      worker_id: null,
      open_input_request_id: null,
      interaction_count: 1,
      max_interactions: 5,
      execution_seconds: 9,
      last_execution_started_at: '2026-08-03T11:07:57Z',
      review_verdict: null,
      review_feedback: null,
      review_exhausted: false,
      error: null,
      created_at: '2026-08-03T11:06:48Z',
      updated_at: '2026-08-03T11:07:57Z',
      finished_at: null,
      steps: [],
      input_requests: [{
        id: 41,
        plan_id: 4,
        run_id: 20,
        source_step_id: 30,
        requested_by: 'planner',
        reason: 'Choose the rollout window',
        questions: [{
          id: 'window',
          header: 'Window',
          question: 'Which rollout window?',
          response_type: 'text',
          options: [],
          required: true,
        }],
        status: 'answered',
        answers: [{ question_id: 'window', value: 'Sunday 02:00 UTC' }],
        response_text: null,
        attachments: null,
        answered_by: 1,
        opened_at: '2026-08-03T11:06:50Z',
        answered_at: '2026-08-03T11:07:00Z',
        created_at: '2026-08-03T11:06:50Z',
      }],
    } satisfies PlanRun;
    const resource = plan(current, prior);
    resource.active_run_id = reviewerRun.id;
    resource.active_run = reviewerRun;
    resource.display_state = 'reviewer';
    resource.latest_run_status = 'running';
    vi.mocked(api.listPlanVersions).mockResolvedValue([current, prior]);
    vi.mocked(api.listPlanResourceRuns).mockResolvedValue([reviewerRun]);

    render(<PlanDetail plan={resource} onRefresh={vi.fn()} />);

    expect(await screen.findByRole('status')).toHaveTextContent(
      'Reviewing v3 candidate · actions unlock when review finishes',
    );
    expect(await screen.findByRole('heading', { level: 1, name: 'Candidate proposal' }))
      .toBeInTheDocument();
    expect(screen.getByText('v3 candidate · not a Version yet')).toBeInTheDocument();
    expect(screen.getByText('v3 input history (1)')).toBeInTheDocument();
    expect(screen.getByText('Sunday 02:00 UTC')).toBeVisible();
    expect(screen.queryByPlaceholderText('Revise from v2…')).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Refresh context' })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Fork' })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /Approve v2/ })).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Cancel planning' })).toBeInTheDocument();
  });
});
