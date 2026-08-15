import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { act, cleanup, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { PRMonitorPage } from './PRMonitorPage';
import type { MonitoredRepo, PRMonitorRun, PRReview } from '../api/client';

vi.mock('../hooks/useWebSocket', () => ({
  useWebSocket: vi.fn(),
}));

vi.mock('../api/client', () => ({
  api: {
    config: vi.fn(),
    listWorkers: vi.fn(),
    getMonitoredRepos: vi.fn(),
    getMonitoredRepo: vi.fn(),
    createMonitoredRepo: vi.fn(),
    updateMonitoredRepo: vi.fn(),
    deleteMonitoredRepo: vi.fn(),
    toggleMonitoredRepo: vi.fn(),
    regenerateSecret: vi.fn(),
    getRepoReviews: vi.fn(),
    getReviewDetail: vi.fn(),
    getPRMonitorRun: vi.fn(),
    bindPRMonitorDeveloper: vi.fn(),
    pausePRMonitorRun: vi.fn(),
    resumePRMonitorRun: vi.fn(),
    unbindPRMonitorDeveloper: vi.fn(),
    submitPRFindingRebuttal: vi.fn(),
    enqueuePRMonitorMerge: vi.fn(),
    getWebhookInfo: vi.fn(),
  },
}));

import { api } from '../api/client';
import { useWebSocket } from '../hooks/useWebSocket';

const baseRepo: MonitoredRepo = {
  id: 1,
  repo_full_name: 'acme/widgets',
  project_id: 10,
  enabled: true,
  auto_merge: false,
  webhook_secret: 'secr***',
  provider: 'codex',
  review_model: null,
  review_effort: null,
  review_mode: 'panel',
  wait_for_ci: true,
  required_checks: [{ kind: 'check_run', name: 'tests', app_slug: 'github-actions' }],
  auto_repair: true,
  max_repair_attempts: 3,
  merge_queue_mode: 'shadow',
  default_branch: 'main',
  allowed_authors: [],
  status: 'active',
  error_message: null,
  created_at: '2026-08-01T00:00:00Z',
  updated_at: '2026-08-01T00:00:00Z',
};

function reviewFixture(overrides: Partial<PRReview> = {}): PRReview {
  return {
    id: 11,
    monitor_run_id: 21,
    repo_id: baseRepo.id,
    pr_number: 42,
    base_sha: 'base-sha',
    head_sha: 'head-sha',
    delivery_id: 'delivery-1',
    pr_title: 'Harden the widget loop',
    pr_author: 'developer',
    pr_url: 'https://github.com/acme/widgets/pull/42',
    task_id: null,
    status: 'changes_required',
    review_summary: 'One exact-head review is being tracked.',
    action_taken: null,
    ci_status: 'failure',
    ci_summary: 'Failed: tests',
    ci_details: {
      head_sha: 'head-sha',
      required: [{ kind: 'check_run', name: 'tests', app_slug: 'github-actions' }],
      observed: [{ kind: 'check_run', name: 'tests', app_slug: 'github-actions', state: 'failure' }],
    },
    reviewer_runs: [],
    created_at: '2026-08-02T00:00:00Z',
    completed_at: null,
    ...overrides,
  };
}

function runFixture(overrides: Partial<PRMonitorRun> = {}): PRMonitorRun {
  return {
    id: 21,
    repo_id: baseRepo.id,
    pr_number: 42,
    status: 'waiting_for_fix',
    current_head_sha: 'head-sha',
    developer_task_id: null,
    repair_attempts: 0,
    max_repair_attempts: 3,
    pause_reason: null,
    wakes: [],
    merge_actions: [],
    ...overrides,
  };
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((promiseResolve, promiseReject) => {
    resolve = promiseResolve;
    reject = promiseReject;
  });
  return { promise, resolve, reject };
}

function selectFollowingLabel(label: string): HTMLSelectElement {
  const select = screen.getByText(label).parentElement?.querySelector('select');
  if (!(select instanceof HTMLSelectElement)) throw new Error(`No select found for ${label}`);
  return select;
}

async function openRepo(user: ReturnType<typeof userEvent.setup>, repo = baseRepo) {
  vi.mocked(api.getMonitoredRepos).mockResolvedValue([repo]);
  vi.mocked(api.getMonitoredRepo).mockResolvedValue(repo);
  render(<PRMonitorPage />);
  await user.click(await screen.findByText(repo.repo_full_name));
  await screen.findByRole('button', { name: 'Save Changes' });
}

async function openReview(
  user: ReturnType<typeof userEvent.setup>,
  review: PRReview,
  run: PRMonitorRun,
  repo: MonitoredRepo = baseRepo,
) {
  vi.mocked(api.getRepoReviews).mockResolvedValue([review]);
  vi.mocked(api.getReviewDetail).mockResolvedValue(review);
  vi.mocked(api.getPRMonitorRun).mockResolvedValue(run);
  await openRepo(user, repo);
  await user.click(await screen.findByText(review.pr_title));
  await screen.findByText(`Review Detail · PR #${review.pr_number}`);
}

describe('PRMonitorPage safety controls', () => {
  beforeEach(() => {
    localStorage.setItem('cc_user', JSON.stringify({ id: 1, role: 'admin' }));
    vi.mocked(api.config).mockResolvedValue({
      default_provider: 'codex',
      provider_options: ['claude', 'codex'],
      default_model: 'claude-opus-4-6',
      model_options: ['default', 'claude-opus-4-6'],
      default_codex_model: 'gpt-5.6-sol',
      codex_model_options: ['default', 'gpt-5.6-sol'],
      default_effort: 'medium',
      effort_options: ['low', 'medium', 'high'],
      claude_model_efforts: {},
      claude_model_context_windows: {},
      codex_effort_options: ['low', 'medium', 'high'],
      codex_model_efforts: {},
      codex_model_service_tiers: {},
    });
    vi.mocked(api.listWorkers).mockResolvedValue([]);
    vi.mocked(api.getMonitoredRepos).mockResolvedValue([]);
    vi.mocked(api.getMonitoredRepo).mockResolvedValue(baseRepo);
    vi.mocked(api.createMonitoredRepo).mockResolvedValue({
      ...baseRepo,
      webhook_secret: 'newly-created-raw-secret',
    });
    vi.mocked(api.updateMonitoredRepo).mockResolvedValue(baseRepo);
    vi.mocked(api.deleteMonitoredRepo).mockResolvedValue({ ok: true });
    vi.mocked(api.toggleMonitoredRepo).mockResolvedValue(baseRepo);
    vi.mocked(api.regenerateSecret).mockResolvedValue({
      ...baseRepo,
      webhook_secret: 'newly-rotated-raw-secret',
    });
    vi.mocked(api.getRepoReviews).mockResolvedValue([]);
    vi.mocked(api.getReviewDetail).mockResolvedValue(reviewFixture());
    vi.mocked(api.getPRMonitorRun).mockResolvedValue(runFixture());
    vi.mocked(api.bindPRMonitorDeveloper).mockResolvedValue(runFixture({ developer_task_id: 99 }));
    vi.mocked(api.pausePRMonitorRun).mockResolvedValue(runFixture({ status: 'paused' }));
    vi.mocked(api.resumePRMonitorRun).mockResolvedValue(runFixture());
    vi.mocked(api.unbindPRMonitorDeveloper).mockResolvedValue(runFixture());
    vi.mocked(api.enqueuePRMonitorMerge).mockResolvedValue(runFixture({ status: 'merge_queue_pending' }));
    vi.mocked(api.getWebhookInfo).mockResolvedValue({ webhook_url: '/api/github/webhook' });
    vi.spyOn(window, 'confirm').mockReturnValue(true);
  });

  afterEach(() => {
    cleanup();
    localStorage.clear();
    vi.restoreAllMocks();
    vi.clearAllMocks();
  });

  it('keeps the add-repository dialog scrollable and dismissible within the viewport', async () => {
    const user = userEvent.setup();
    render(<PRMonitorPage />);
    await user.click(await screen.findByRole('button', { name: 'Add Repository' }));

    const dialog = screen.getByRole('dialog', { name: 'Add Repository' });
    const form = dialog.querySelector('form');
    expect(dialog).toHaveClass('max-h-[calc(100dvh-2rem)]', 'overflow-hidden');
    expect(form).toHaveClass('min-h-0', 'overflow-y-auto', 'overscroll-contain');
    expect(document.body.style.overflow).toBe('hidden');

    await user.keyboard('{Escape}');
    expect(screen.queryByRole('dialog', { name: 'Add Repository' })).not.toBeInTheDocument();
    expect(document.body.style.overflow).toBe('');

    await user.click(screen.getByRole('button', { name: 'Add Repository' }));
    const reopenedDialog = screen.getByRole('dialog', { name: 'Add Repository' });
    await user.click(reopenedDialog.parentElement!);
    expect(screen.queryByRole('dialog', { name: 'Add Repository' })).not.toBeInTheDocument();
  });

  it('hides projectless PR Monitor creation from members', async () => {
    localStorage.setItem('cc_user', JSON.stringify({ id: 9, role: 'member' }));

    render(<PRMonitorPage />);

    await waitFor(() => expect(api.getMonitoredRepos).toHaveBeenCalled());
    expect(screen.queryByRole('button', { name: /Add Repository/i })).not.toBeInTheDocument();
  });

  it('fails closed if administrator identity changes while the add dialog is open', async () => {
    const user = userEvent.setup();
    render(<PRMonitorPage />);

    await user.click(await screen.findByRole('button', { name: /Add Repository/i }));
    localStorage.setItem('cc_user', JSON.stringify({ id: 9, role: 'member' }));
    await user.type(screen.getByPlaceholderText('owner/repo'), 'acme/new-repo');
    await user.click(screen.getByRole('button', { name: 'Add' }));

    expect(await screen.findByText('Only administrators can add a PR Monitor repository.')).toBeInTheDocument();
    expect(api.createMonitoredRepo).not.toHaveBeenCalled();
  });

  it('defaults new repositories to one bounded reviewer Task', async () => {
    const user = userEvent.setup();
    render(<PRMonitorPage />);
    await user.click(await screen.findByRole('button', { name: 'Add Repository' }));

    expect(selectFollowingLabel('Review Harness')).toHaveValue('single');
    expect(screen.getByText('One review Task with a bounded PR context.')).toBeInTheDocument();
    expect(screen.queryByText(/roughly 3× the model work/)).not.toBeInTheDocument();
    expect(screen.getByLabelText('Wait for exact-head CI')).toBeDisabled();

    await user.type(screen.getByPlaceholderText('owner/repo'), 'acme/default-single');
    await user.click(screen.getByRole('button', { name: 'Add' }));

    await waitFor(() => expect(api.createMonitoredRepo).toHaveBeenCalledWith(
      expect.objectContaining({
        repo_full_name: 'acme/default-single',
        review_mode: 'single',
        wait_for_ci: false,
        required_checks: [],
      }),
    ));
  });

  it('allows panel auto-merge and keeps it mutually exclusive with Merge Queue', async () => {
    const user = userEvent.setup();
    render(<PRMonitorPage />);
    await user.click(await screen.findByRole('button', { name: 'Add Repository' }));

    await user.selectOptions(selectFollowingLabel('Review Harness'), 'panel');
    expect(screen.getByText(/three independent review Tasks/)).toHaveTextContent('roughly 3×');
    const autoMerge = screen.getByLabelText('Direct auto-merge after review and exact-head gates pass');
    const mergeQueue = selectFollowingLabel('Merge Queue');
    expect(autoMerge).toBeEnabled();
    expect(autoMerge).not.toBeChecked();
    await user.selectOptions(mergeQueue, 'auto');
    expect(autoMerge).not.toBeChecked();
    expect(screen.getByText(/Merge Queue AUTO is a separate automatic policy/)).toBeInTheDocument();
    await user.click(autoMerge);
    expect(autoMerge).toBeChecked();
    expect(mergeQueue).toHaveValue('manual');

    await user.type(screen.getByPlaceholderText('owner/repo'), 'acme/new-repo');
    await user.type(
      screen.getByPlaceholderText(/check_run,tests,github-actions/),
      'check_run,tests,github-actions',
    );
    await user.click(screen.getByRole('button', { name: 'Add' }));

    await waitFor(() => {
      expect(api.createMonitoredRepo).toHaveBeenCalledWith(expect.objectContaining({
        repo_full_name: 'acme/new-repo',
        review_mode: 'panel',
        auto_merge: true,
        auto_repair: false,
        wait_for_ci: true,
        merge_queue_mode: 'manual',
        required_checks: [{ kind: 'check_run', name: 'tests', app_slug: 'github-actions' }],
      }));
    });
    expect(await screen.findByText('newly-created-raw-secret')).toBeInTheDocument();
    expect(screen.getByRole('alert')).toHaveTextContent('will not be shown again');
  });

  it('creates a Claude PR Monitor when Claude is the only available provider', async () => {
    const configRequest = deferred<Awaited<ReturnType<typeof api.config>>>();
    vi.mocked(api.config).mockReturnValue(configRequest.promise);
    const claudeOnlyConfig = {
      default_provider: 'codex',
      provider_options: ['claude'],
      default_model: 'claude-opus-4-6',
      model_options: ['default', 'claude-opus-4-6'],
      default_codex_model: 'gpt-5.6-sol',
      codex_model_options: ['default', 'gpt-5.6-sol'],
      default_effort: 'medium',
      effort_options: ['low', 'medium', 'high'],
      claude_model_efforts: {},
      claude_model_context_windows: {},
      codex_effort_options: ['low', 'medium', 'high'],
      codex_model_efforts: {},
      codex_model_service_tiers: {},
    } as Awaited<ReturnType<typeof api.config>>;
    const user = userEvent.setup();
    render(<PRMonitorPage />);
    await user.click(await screen.findByRole('button', { name: 'Add Repository' }));

    await user.selectOptions(selectFollowingLabel('Review Harness'), 'single');
    await user.type(screen.getByPlaceholderText('owner/repo'), 'acme/claude-only');
    expect(screen.getByRole('button', { name: 'Add' })).toBeDisabled();
    await act(async () => configRequest.resolve(claudeOnlyConfig));

    const provider = selectFollowingLabel('Provider');
    await waitFor(() => expect(provider).toHaveValue('claude'));
    expect(provider.options).toHaveLength(1);
    await user.click(screen.getByRole('button', { name: 'Add' }));

    await waitFor(() => expect(api.createMonitoredRepo).toHaveBeenCalledWith(
      expect.objectContaining({ provider: 'claude' }),
    ));
  });

  it('allows direct auto-merge with the single-reviewer harness', async () => {
    const user = userEvent.setup();
    render(<PRMonitorPage />);
    await user.click(await screen.findByRole('button', { name: 'Add Repository' }));

    await user.selectOptions(selectFollowingLabel('Review Harness'), 'single');
    const autoMerge = screen.getByLabelText('Direct auto-merge after review and exact-head gates pass');
    expect(autoMerge).toBeEnabled();
    await user.click(autoMerge);
    await user.type(screen.getByPlaceholderText('owner/repo'), 'acme/single-review');
    await user.click(screen.getByRole('button', { name: 'Add' }));

    await waitFor(() => {
      expect(api.createMonitoredRepo).toHaveBeenCalledWith(expect.objectContaining({
        repo_full_name: 'acme/single-review',
        review_mode: 'single',
        auto_merge: true,
        auto_repair: false,
        wait_for_ci: false,
        merge_queue_mode: 'manual',
        required_checks: [],
      }));
    });
  });

  it('keeps stored secrets masked and reveals only the rotated value', async () => {
    const user = userEvent.setup();
    await openRepo(user);

    expect(await screen.findByText('secr***')).toBeInTheDocument();
    expect(screen.getByTitle('Rotate to reveal a new secret')).toBeDisabled();

    await user.click(screen.getByTitle('Regenerate secret'));

    expect(await screen.findByText('newly-rotated-raw-secret')).toBeInTheDocument();
    expect(screen.getByTitle('Copy newly generated secret')).toBeEnabled();
  });

  it('preserves auto-merge when editing a panel repository', async () => {
    const user = userEvent.setup();
    const autoRepo = { ...baseRepo, auto_merge: true, merge_queue_mode: 'manual' as const };
    await openRepo(user, autoRepo);

    const autoMerge = screen.getByLabelText('Direct auto-merge after review and exact-head gates pass');
    expect(autoMerge).toBeEnabled();
    expect(autoMerge).toBeChecked();
    expect(screen.getByText(/ON: CCM confirms the exact-head merge/)).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Save Changes' }));

    await waitFor(() => {
      expect(api.updateMonitoredRepo).toHaveBeenCalledWith(autoRepo.id, expect.objectContaining({
        review_mode: 'panel',
        auto_merge: true,
        auto_repair: true,
        wait_for_ci: true,
        merge_queue_mode: 'manual',
      }));
    });
  });

  it('shows direct and queued automatic merge policies distinctly', async () => {
    vi.mocked(api.getMonitoredRepos).mockResolvedValue([
      { ...baseRepo, id: 1, repo_full_name: 'acme/shadow' },
      { ...baseRepo, id: 2, repo_full_name: 'acme/direct', auto_merge: true, merge_queue_mode: 'manual' },
      { ...baseRepo, id: 3, repo_full_name: 'acme/queued', merge_queue_mode: 'auto' },
    ]);

    render(<PRMonitorPage />);

    expect(await screen.findByText('Merge Policy')).toBeInTheDocument();
    expect(screen.getByText('SHADOW')).toBeInTheDocument();
    expect(screen.getByText('AUTO')).toBeInTheDocument();
    expect(screen.getByText('QUEUE AUTO')).toBeInTheDocument();
  });

  it('renders the backend human projection and reviewer summaries without canned advice', async () => {
    const user = userEvent.setup();
    const review = reviewFixture({
      task_ids: [301, 302, 303],
      task_id: null,
      display_status: 'Review system failed',
      display_summary: 'The Senior reviewer could not start, so this panel has no complete code verdict.',
      outcome_kind: 'infrastructure_error',
      aggregate_verdict: null,
      reviewer_count: 3,
      reviewer_status_counts: { completed: 2, error: 1 },
      reviewer_verdict_counts: { pass: 1, changes_required: 1 },
      reviewer_runs: [{
        id: 31,
        role: 'principal',
        task_id: 301,
        provider: 'codex',
        model: 'gpt-5.6-sol',
        effort: 'high',
        status: 'completed',
        verdict: 'changes_required',
        result_body: 'Principal found one correctness issue in the changed request path.',
        outcome_kind: 'review_result',
        error_message: null,
        created_at: '2026-08-02T00:00:00Z',
        completed_at: '2026-08-02T00:05:00Z',
        findings: [],
      }],
    });
    await openReview(user, review, runFixture());

    expect(screen.getAllByText('Review system failed').length).toBeGreaterThan(0);
    expect(screen.getByRole('alert')).toHaveTextContent('Senior reviewer could not start');
    expect(screen.getByRole('alert')).toHaveTextContent('No code verdict was produced');
    expect(screen.getByText('Principal found one correctness issue in the changed request path.')).toBeInTheDocument();
    expect(screen.getByText('Task #301')).toBeInTheDocument();
    expect(screen.getByText(/3 reviewers/)).toBeInTheDocument();
    expect(screen.getByText(/Progress: 2 completed · 1 review failed/)).toBeInTheDocument();
    expect(screen.queryByText(/优先处理高风险和中风险问题/)).not.toBeInTheDocument();
    expect(screen.queryByText(/应用修复前下载并检查 Diff/)).not.toBeInTheDocument();
  });

  it('does not infer a historical review mode from the current repository setting', async () => {
    const user = userEvent.setup();
    const currentPanelRepo: MonitoredRepo = {
      ...baseRepo,
      review_mode: 'panel',
      wait_for_ci: true,
      required_checks: [],
      auto_repair: false,
      merge_queue_mode: 'manual',
    };
    const review = reviewFixture({
      task_id: 701,
      task_ids: undefined,
      reviewer_runs: [],
      reviewer_count: 0,
      display_status: 'Changes required',
      display_summary: 'The single reviewer found one blocking issue.',
      outcome_kind: 'review_result',
      aggregate_verdict: 'changes_required',
    });

    await openReview(user, review, runFixture(), currentPanelRepo);

    expect(screen.getByText('The single reviewer found one blocking issue.')).toBeInTheDocument();
    expect(screen.getAllByText('#701').length).toBeGreaterThan(0);
    expect(screen.queryByText('Reviewer panel has not started yet.')).not.toBeInTheDocument();
  });

  it('loads the complete review body when a bounded history row is opened', async () => {
    const user = userEvent.setup();
    const listReview = reviewFixture({
      display_summary: 'Authorization finding preview…',
      review_summary: 'Authorization finding preview…',
    });
    const detailReview = {
      ...listReview,
      display_summary: 'Authorization can be bypassed.\n\nCheck the project ACL before dispatch.',
      review_summary: 'Authorization can be bypassed.\n\nCheck the project ACL before dispatch.',
    };
    vi.mocked(api.getRepoReviews).mockResolvedValue([listReview]);
    vi.mocked(api.getReviewDetail).mockResolvedValue(detailReview);

    await openRepo(user);
    await user.click(await screen.findByText(listReview.pr_title));

    expect(await screen.findByText(/Authorization can be bypassed/)).toHaveTextContent(
      'Authorization can be bypassed. Check the project ACL before dispatch.',
    );
    expect(screen.queryByText('Authorization finding preview…')).not.toBeInTheDocument();
    expect(api.getReviewDetail).toHaveBeenCalledWith(listReview.id);
  });

  it('labels current and historical review heads in history', async () => {
    const user = userEvent.setup();
    vi.mocked(api.getRepoReviews).mockResolvedValue([
      reviewFixture({ id: 12, head_sha: '2222222222222222222222222222222222222222', is_current_snapshot: true }),
      reviewFixture({ id: 11, head_sha: '1111111111111111111111111111111111111111', is_current_snapshot: false }),
    ]);

    await openRepo(user);

    expect(await screen.findByText('22222222')).toBeInTheDocument();
    expect(screen.getByText('11111111')).toBeInTheDocument();
    expect(screen.getByText('Current head')).toBeInTheDocument();
    expect(screen.getByText('Historical head')).toBeInTheDocument();
  });

  it('subscribes to PR Monitor updates and refreshes an active review list', async () => {
    const user = userEvent.setup();
    const activeReview = reviewFixture({ status: 'reviewing', outcome_kind: 'in_progress' });
    vi.mocked(api.getRepoReviews).mockResolvedValue([activeReview]);
    await openRepo(user);

    const subscription = vi.mocked(useWebSocket).mock.calls.find(([channels]) => (
      channels.length === 1 && channels[0] === 'pr-monitor'
    ));
    expect(subscription).toBeDefined();

    vi.mocked(api.getRepoReviews).mockResolvedValue([{ ...activeReview, pr_title: 'Refreshed review title' }]);
    await act(async () => {
      subscription?.[1]?.({ type: 'review_updated', review_id: activeReview.id });
      await Promise.resolve();
    });

    expect(await screen.findByText('Refreshed review title')).toBeInTheDocument();
  });

  it('shows CI and monitor details before reviewer runs exist', async () => {
    const user = userEvent.setup();
    const review = reviewFixture({ status: 'waiting_ci', reviewer_runs: [] });
    const run = runFixture({
      wakes: [{
        id: 7,
        developer_task_id: null,
        trigger_head_sha: 'head-sha',
        reason_kind: 'review_findings',
        status: 'shadow',
        attempt: 1,
        last_error: null,
      }],
    });
    await openReview(user, review, run);

    expect(screen.getByText('CI: Failed: tests')).toBeInTheDocument();
    expect(screen.getByText('failure · tests · github-actions')).toBeInTheDocument();
    const diagnostics = screen.getByText('Advanced diagnostics').closest('details');
    expect(diagnostics).not.toHaveAttribute('open');
    await user.click(screen.getByText('Advanced diagnostics'));
    expect(screen.getByText(/Loop: Waiting For Fix/)).toBeInTheDocument();
    expect(screen.getByText(/Wake #7: Shadow/)).toBeInTheDocument();
    expect(screen.getByText('Reviewer panel has not started yet.')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Bind' })).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Pause loop' })).not.toBeInTheDocument();
  });

  it('offers only enqueue for a ready run and renders an enqueue failure', async () => {
    const user = userEvent.setup();
    const review = reviewFixture({ status: 'approved', ci_status: 'success', ci_summary: 'All required checks passed' });
    const run = runFixture({ status: 'ready_to_merge', developer_task_id: 55 });
    const enqueueRequest = deferred<PRMonitorRun>();
    vi.mocked(api.enqueuePRMonitorMerge).mockReturnValueOnce(enqueueRequest.promise);
    await openReview(user, review, run);

    expect(screen.queryByRole('button', { name: 'Bind' })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Unbind Developer' })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Pause loop' })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Resume loop' })).not.toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: 'Enqueue merge' }));
    expect(screen.getByRole('button', { name: 'Enqueuing…' })).toBeDisabled();
    await act(async () => enqueueRequest.reject(new Error('queue rejected')));
    expect(await screen.findByRole('alert')).toHaveTextContent('Error: queue rejected');
  });

  it('shows unbind pending and failure states only when the run is safe to mutate', async () => {
    const user = userEvent.setup();
    const unbindRequest = deferred<PRMonitorRun>();
    vi.mocked(api.unbindPRMonitorDeveloper).mockReturnValueOnce(unbindRequest.promise);
    await openReview(user, reviewFixture(), runFixture({ developer_task_id: 55 }));

    await user.click(screen.getByRole('button', { name: 'Unbind Developer' }));
    expect(screen.getByRole('button', { name: 'Unbinding…' })).toBeDisabled();
    expect(screen.getByRole('button', { name: 'Pause loop' })).toBeDisabled();
    await act(async () => unbindRequest.reject(new Error('unbind rejected')));
    expect(await screen.findByRole('alert')).toHaveTextContent('Error: unbind rejected');
    expect(screen.getByRole('button', { name: 'Unbind Developer' })).toBeEnabled();
  });

  it.each(['merged', 'closed'])('does not expose run controls for a %s run', async (status) => {
    const user = userEvent.setup();
    await openReview(
      user,
      reviewFixture({ status }),
      runFixture({ status, developer_task_id: 55 }),
    );

    expect(screen.queryByRole('button', { name: 'Bind' })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Unbind Developer' })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Pause loop' })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Resume loop' })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Enqueue merge' })).not.toBeInTheDocument();
  });

  it('hides pause and binding controls while repair delivery is active', async () => {
    const user = userEvent.setup();
    const run = runFixture({
      developer_task_id: 55,
      wakes: [{
        id: 8,
        developer_task_id: 55,
        trigger_head_sha: 'head-sha',
        reason_kind: 'review_findings',
        status: 'delivering',
        attempt: 1,
        last_error: null,
      }],
    });
    await openReview(user, reviewFixture(), run);

    await user.click(screen.getByText('Advanced diagnostics'));
    expect(screen.getByText(/Wake #8: Delivering/)).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Unbind Developer' })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Pause loop' })).not.toBeInTheDocument();
  });

  it('keeps an accepted rebuttal locked until durable resolution finishes', async () => {
    const user = userEvent.setup();
    const review = reviewFixture({
      reviewer_runs: [{
        id: 31,
        role: 'principal',
        task_id: 301,
        provider: 'codex',
        model: 'gpt-5.6-sol',
        effort: 'high',
        status: 'changes_required',
        verdict: 'changes_required',
        error_message: null,
        created_at: '2026-08-02T00:00:00Z',
        completed_at: '2026-08-02T00:05:00Z',
        findings: [{
          id: 41,
          reviewer_run_id: 31,
          role: 'principal',
          severity: 'medium',
          category: 'correctness',
          path: 'backend/service.py',
          line: 10,
          hunk: null,
          title: 'Durable resolution pending',
          evidence: 'The accepted rebuttal has not resolved its GitHub thread yet.',
          impact: 'A duplicate adjudication could race durable publication.',
          required_fix: 'Wait for the accepted effect to finish.',
          test: 'Verify a second rebuttal cannot be submitted.',
          status: 'open',
          thread_status: 'published_inline',
          github_comment_id: 100,
          github_comment_url: 'https://github.com/acme/widgets/pull/42#discussion_r100',
          thread_error: null,
          rebuttals: [{
            id: 51,
            finding_id: 41,
            task_id: 401,
            attempt: 1,
            evidence: 'Concrete accepted evidence',
            status: 'accepted',
            verdict: 'accepted',
            result_body: 'Accepted; resolving the durable thread.',
            error_message: null,
          }],
        }],
      }],
    });
    await openReview(user, review, runFixture({ status: 'adjudicating' }));

    expect(screen.getByPlaceholderText('Concrete code/test/policy evidence for this exact head')).toBeDisabled();
    expect(screen.getByRole('button', { name: 'Adjudicating…' })).toBeDisabled();
    expect(api.submitPRFindingRebuttal).not.toHaveBeenCalled();
  });
});
