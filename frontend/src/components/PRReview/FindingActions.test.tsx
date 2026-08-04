import { beforeEach, describe, expect, it, vi } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { FindingActions } from './FindingActions';

vi.mock('../../api/client', () => ({
  api: {
    ignoreReviewFinding: vi.fn(),
    saveReviewFindingAdvice: vi.fn(),
    createReviewFindingFix: vi.fn(),
    getReviewFindingAction: vi.fn(),
    downloadReviewFindingDiff: vi.fn(),
    confirmReviewFindingFix: vi.fn(),
  },
}));

import { api } from '../../api/client';

const finding = {
  id: 21,
  reviewer_run_id: 4,
  role: 'senior',
  severity: 'high' as const,
  category: 'correctness',
  path: 'backend/example.py',
  line: 12,
  hunk: null,
  title: 'Empty value fails',
  evidence: 'The empty branch raises.',
  impact: 'Valid requests fail.',
  required_fix: 'Return the documented default.',
  test: 'Cover the empty branch.',
  status: 'open',
  thread_status: 'published_inline' as const,
  github_comment_id: null,
  github_comment_url: null,
  thread_error: null,
  rebuttals: [],
  latest_action: null,
};

describe('FindingActions', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.spyOn(window, 'confirm').mockReturnValue(true);
  });

  it('creates an audited ignore action for the current snapshot', async () => {
    vi.mocked(api.ignoreReviewFinding).mockResolvedValue({
      id: 31,
      finding_id: 21,
      action_type: 'ignore',
      status: 'completed',
      idempotency_key: 'ignore-key',
      actor_user_id: null,
      human_advice: null,
      task_id: null,
      expected_head_sha: 'a'.repeat(40),
      patch_sha256: null,
      result: null,
      error_message: null,
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
      completed_at: new Date().toISOString(),
      diff_download_url: null,
      confirmation_token: null,
    });
    const onChanged = vi.fn().mockResolvedValue(undefined);
    render(
      <FindingActions finding={finding} currentSnapshot onChanged={onChanged} />,
    );

    await userEvent.click(screen.getByRole('button', { name: 'Ignore' }));

    await waitFor(() => expect(api.ignoreReviewFinding).toHaveBeenCalledWith(
      21,
      expect.stringMatching(/^ignore-21-/),
    ));
    expect(onChanged).toHaveBeenCalledOnce();
  });

  it('locks new actions on a historical snapshot', () => {
    render(
      <FindingActions finding={finding} currentSnapshot={false} onChanged={vi.fn()} />,
    );

    expect(screen.getByText(/Historical snapshot/)).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Generate AI fix' })).not.toBeInTheDocument();
  });
});
