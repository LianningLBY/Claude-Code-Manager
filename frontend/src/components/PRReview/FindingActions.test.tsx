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
    URL.createObjectURL = vi.fn(() => 'blob:review-diff');
    URL.revokeObjectURL = vi.fn();
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

  it('locks competing actions and confirms the exact downloaded target', async () => {
    const patchSha = 'b'.repeat(64);
    const activeFinding = {
      ...finding,
      latest_action: {
        id: 32,
        finding_id: 21,
        action_type: 'ai_fix' as const,
        status: 'awaiting_confirmation' as const,
        idempotency_key: 'fix-key',
        actor_user_id: null,
        human_advice: null,
        task_id: 71,
        expected_head_sha: 'a'.repeat(40),
        patch_sha256: patchSha,
        result: {
          head_repo_full_name: 'fork-owner/repo',
          head_ref: 'feature/fix',
          pr_number: 7,
          allowed_files: ['backend/example.py'],
        },
        error_message: null,
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
        completed_at: null,
        diff_download_url: '/api/pr-monitor/actions/32/diff',
        confirmation_token: 'confirmation-token',
      },
    };
    vi.mocked(api.downloadReviewFindingDiff).mockResolvedValue({
      blob: new Blob(['diff']),
      filename: 'finding-32.diff',
    });
    vi.mocked(api.confirmReviewFindingFix).mockResolvedValue(activeFinding.latest_action);

    render(<FindingActions finding={activeFinding} currentSnapshot onChanged={vi.fn()} />);

    expect(screen.queryByRole('button', { name: 'Ignore' })).not.toBeInTheDocument();
    expect(screen.getByText(/fork-owner\/repo#7/)).toBeInTheDocument();
    await userEvent.click(screen.getByRole('button', { name: 'Download diff' }));
    await userEvent.click(screen.getByRole('button', { name: 'Confirm and push' }));

    expect(window.confirm).toHaveBeenLastCalledWith(expect.stringContaining(
      `PR: fork-owner/repo#7\nSource ref: feature/fix\nExpected head: ${'a'.repeat(40)}`,
    ));
    expect(window.confirm).toHaveBeenLastCalledWith(expect.stringContaining(
      `Files: backend/example.py\nPatch SHA-256: ${patchSha}`,
    ));
    expect(api.confirmReviewFindingFix).toHaveBeenCalledWith(
      32,
      'confirmation-token',
      patchSha,
    );
  });
});
