import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { act, cleanup, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { PRMonitorPage } from './PRMonitorPage';
import type { MonitoredRepo, PRMonitorRun, PRReview } from '../api/client';

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
) {
  vi.mocked(api.getRepoReviews).mockResolvedValue([review]);
  vi.mocked(api.getReviewDetail).mockResolvedValue(review);
  vi.mocked(api.getPRMonitorRun).mockResolvedValue(run);
  await openRepo(user);
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

  it('keeps panel-only settings out of a new single-reviewer payload', async () => {
    const user = userEvent.setup();
    render(<PRMonitorPage />);
    await user.click(await screen.findByRole('button', { name: 'Add Repository' }));

    const legacyAutoMerge = screen.getByLabelText('Legacy auto-merge (single reviewer only)');
    const autoRepair = screen.getByLabelText(/Auto-resume bound local Developer Task/);
    const waitForCi = screen.getByLabelText('Wait for exact-head CI');
    const mergeQueue = selectFollowingLabel('Merge Queue');
    const harness = selectFollowingLabel('Review Harness');

    expect(legacyAutoMerge).toBeDisabled();
    await user.click(autoRepair);
    await user.selectOptions(mergeQueue, 'auto');
    await user.selectOptions(harness, 'single');

    expect(autoRepair).toBeDisabled();
    expect(autoRepair).not.toBeChecked();
    expect(waitForCi).toBeDisabled();
    expect(waitForCi).not.toBeChecked();
    expect(mergeQueue).toBeDisabled();
    expect(mergeQueue).toHaveValue('manual');

    await user.click(legacyAutoMerge);
    expect(legacyAutoMerge).toBeChecked();
    await user.selectOptions(harness, 'panel');
    expect(legacyAutoMerge).toBeDisabled();
    expect(legacyAutoMerge).not.toBeChecked();

    await user.selectOptions(harness, 'single');
    await user.click(legacyAutoMerge);
    await user.type(screen.getByPlaceholderText('owner/repo'), 'acme/new-repo');
    await user.click(screen.getByRole('button', { name: 'Add' }));

    await waitFor(() => {
      expect(api.createMonitoredRepo).toHaveBeenCalledWith(expect.objectContaining({
        repo_full_name: 'acme/new-repo',
        review_mode: 'single',
        auto_merge: true,
        auto_repair: false,
        wait_for_ci: false,
        merge_queue_mode: 'manual',
        required_checks: [],
      }));
    });
    expect(await screen.findByText('newly-created-raw-secret')).toBeInTheDocument();
    expect(screen.getByRole('alert')).toHaveTextContent('will not be shown again');
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

  it('normalizes an invalid panel auto-merge setting before saving', async () => {
    const user = userEvent.setup();
    const invalidRepo = { ...baseRepo, auto_merge: true };
    await openRepo(user, invalidRepo);

    const legacyAutoMerge = screen.getByLabelText('Legacy auto-merge (single reviewer only)');
    expect(legacyAutoMerge).toBeDisabled();
    expect(legacyAutoMerge).not.toBeChecked();
    await user.click(screen.getByRole('button', { name: 'Save Changes' }));

    await waitFor(() => {
      expect(api.updateMonitoredRepo).toHaveBeenCalledWith(invalidRepo.id, expect.objectContaining({
        review_mode: 'panel',
        auto_merge: false,
        auto_repair: true,
        wait_for_ci: true,
        merge_queue_mode: 'shadow',
      }));
    });
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
    expect(screen.getByText(/Loop: waiting_for_fix/)).toBeInTheDocument();
    expect(screen.getByText(/Wake #7: shadow/)).toBeInTheDocument();
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

    expect(screen.getByText(/Wake #8: delivering/)).toBeInTheDocument();
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
