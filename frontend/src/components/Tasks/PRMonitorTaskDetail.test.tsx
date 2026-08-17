import { render, screen, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { PRReview, PRReviewResult } from '../../api/client';
import { api } from '../../api/client';
import { PRMonitorTaskDetail } from './PRMonitorTaskDetail';

vi.mock('../../api/client', () => ({
  api: {
    getReviewDetail: vi.fn(),
  },
}));

function resultFixture(): PRReviewResult {
  return {
    result_key: 'run:14',
    run_id: 14,
    display_task_id: 42,
    repo_id: 3,
    repo_full_name: 'acme/widget',
    pr_number: 133,
    pr_title: 'Read-only review',
    pr_url: 'https://github.com/acme/widget/pull/133',
    review_id: 113,
    base_ref: 'main',
    base_sha: 'b'.repeat(40),
    head_sha: 'a'.repeat(40),
    verdict_state: 'complete',
    aggregate_verdict: 'changes_required',
    publication_state: 'not_applicable',
    lifecycle_state: 'reviewing',
    failure_stage: null,
    error_category: null,
    error_measured: null,
    error_limit: null,
    error_unit: null,
    display_status: 'Changes required',
    display_summary: 'Review summary',
    published_actor: null,
    published_at: null,
    github_review_id: null,
    github_review_url: null,
    github_state: null,
    github_event: null,
    created_at: '2026-08-16T00:00:00Z',
    updated_at: '2026-08-16T00:02:00Z',
    completed_at: '2026-08-16T00:02:00Z',
    can_rerun: false,
  };
}

describe('PRMonitorTaskDetail', () => {
  beforeEach(() => vi.clearAllMocks());

  it('loads the current PRReviewDetail and keeps it read-only', async () => {
    vi.mocked(api.getReviewDetail).mockResolvedValue({
      ...resultFixture(),
      reviewer_runs: [{
        id: 1,
        role: 'principal_engineer',
        task_id: 900,
        provider: 'codex',
        model: 'gpt-5.6-sol',
        effort: 'high',
        status: 'completed',
        verdict: 'changes_required',
        result_body: 'Found a regression.',
        outcome_kind: 'review_result',
        error_message: null,
        created_at: '2026-08-16T00:00:00Z',
        completed_at: '2026-08-16T00:01:00Z',
        findings: [],
      }],
    } as PRReview);

    render(
      <PRMonitorTaskDetail
        task={{
          title: 'PR Review: acme/widget#133',
          description: null,
          metadata_: { pr_monitor_display: true, pr_monitor_review_id: 113 },
        }}
        result={resultFixture()}
        onBack={vi.fn()}
      />,
    );

    await waitFor(() => expect(api.getReviewDetail).toHaveBeenCalledWith(113));
    expect(await screen.findByRole('region', { name: 'Reviewer details' })).toBeInTheDocument();
    expect(screen.getByText('Principal Engineer')).toBeInTheDocument();
    expect(screen.getByText('Found a regression.')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /Re-run|Create follow-up|Open review details/i })).not.toBeInTheDocument();
  });
});
