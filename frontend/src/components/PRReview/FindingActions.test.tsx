import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { act, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { FindingActions } from './FindingActions';

vi.mock('../../api/client', () => ({
  api: {
    ignoreReviewFinding: vi.fn(),
    saveReviewFindingAdvice: vi.fn(),
    createReviewFindingFix: vi.fn(),
    getReviewFindingAction: vi.fn(),
    cancelPRFindingAction: vi.fn(),
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
    vi.spyOn(HTMLAnchorElement.prototype, 'click').mockImplementation(() => undefined);
    URL.createObjectURL = vi.fn(() => 'blob:review-diff');
    URL.revokeObjectURL = vi.fn();
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
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
      downloaded_by_user_id: null,
      downloaded_at: null,
      confirmed_by_user_id: null,
      confirmed_at: null,
      result: null,
      error_message: null,
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
      completed_at: new Date().toISOString(),
      diff_download_url: null,
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
        downloaded_by_user_id: null,
        downloaded_at: null,
        confirmed_by_user_id: null,
        confirmed_at: null,
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
      },
    };
    vi.mocked(api.downloadReviewFindingDiff).mockResolvedValue({
      blob: new Blob(['diff']),
      filename: 'finding-32.diff',
      receipt: 'download-receipt-32',
      confirmationToken: 'download-confirmation-token',
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
      'download-confirmation-token',
      patchSha,
      'download-receipt-32',
    );
  });

  it.each([
    ['pending', null, true],
    ['running', null, true],
    ['awaiting_confirmation', null, true],
    ['running', new Date().toISOString(), false],
  ] as const)(
    'offers cancellation only for an unconfirmed active fix (%s, confirmed_at=%s)',
    async (status, confirmedAt, cancellable) => {
      const activeAction = {
        id: 41,
        finding_id: 21,
        action_type: 'ai_fix' as const,
        status,
        idempotency_key: 'fix-cancel-key',
        actor_user_id: null,
        human_advice: null,
        task_id: 81,
        expected_head_sha: 'c'.repeat(40),
        patch_sha256: status === 'awaiting_confirmation' ? 'd'.repeat(64) : null,
        downloaded_by_user_id: null,
        downloaded_at: null,
        confirmed_by_user_id: confirmedAt ? 1 : null,
        confirmed_at: confirmedAt,
        result: status === 'awaiting_confirmation' ? {
          head_repo_full_name: 'fork-owner/repo',
          head_ref: 'feature/fix',
          pr_number: 7,
          allowed_files: ['backend/example.py'],
        } : null,
        error_message: null,
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
        completed_at: null,
        diff_download_url: status === 'awaiting_confirmation'
          ? '/api/pr-monitor/actions/41/diff'
          : null,
      };
      const cancelledAction = {
        ...activeAction,
        status: 'cancelled' as const,
        completed_at: new Date().toISOString(),
      };
      vi.mocked(api.cancelPRFindingAction).mockResolvedValue(cancelledAction);
      const onChanged = vi.fn().mockResolvedValue(undefined);

      render(
        <FindingActions
          finding={{ ...finding, latest_action: activeAction }}
          currentSnapshot
          onChanged={onChanged}
        />,
      );

      const cancelButton = screen.queryByRole('button', { name: 'Cancel AI fix' });
      if (!cancellable) {
        expect(cancelButton).not.toBeInTheDocument();
        return;
      }

      await userEvent.click(cancelButton!);
      await waitFor(() => expect(api.cancelPRFindingAction).toHaveBeenCalledWith(41));
      expect(onChanged).toHaveBeenCalledOnce();
    },
  );

  it('keeps a durable cancelling action locked and polls until it becomes terminal', async () => {
    vi.useFakeTimers();
    const cancellingAction = {
      id: 45,
      finding_id: 21,
      action_type: 'ai_fix' as const,
      status: 'cancelling' as const,
      idempotency_key: 'fix-cancelling-key',
      actor_user_id: null,
      human_advice: null,
      task_id: 85,
      expected_head_sha: 'c'.repeat(40),
      patch_sha256: null,
      downloaded_by_user_id: null,
      downloaded_at: null,
      confirmed_by_user_id: null,
      confirmed_at: null,
      result: null,
      error_message: null,
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
      completed_at: null,
      diff_download_url: null,
    };
    vi.mocked(api.getReviewFindingAction).mockResolvedValue(cancellingAction);
    const onChanged = vi.fn().mockResolvedValue(undefined);

    render(
      <FindingActions
        finding={{ ...finding, latest_action: cancellingAction }}
        currentSnapshot
        onChanged={onChanged}
      />,
    );

    expect(screen.queryByRole('button', { name: 'Ignore' })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Human advice' })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Generate AI fix' })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Cancel AI fix' })).not.toBeInTheDocument();

    await act(async () => {
      await vi.advanceTimersByTimeAsync(2000);
    });

    expect(api.getReviewFindingAction).toHaveBeenCalledWith(45);
    expect(onChanged).not.toHaveBeenCalled();
  });

  it('invalidates downloaded confirmation credentials when action identity changes', async () => {
    const baseAction = {
      id: 52,
      finding_id: 21,
      action_type: 'ai_fix' as const,
      status: 'awaiting_confirmation' as const,
      idempotency_key: 'fix-identity-key',
      actor_user_id: null,
      human_advice: null,
      task_id: 91,
      expected_head_sha: 'e'.repeat(40),
      patch_sha256: 'f'.repeat(64),
      downloaded_by_user_id: null,
      downloaded_at: null,
      confirmed_by_user_id: null,
      confirmed_at: null,
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
      diff_download_url: '/api/pr-monitor/actions/52/diff',
    };
    vi.mocked(api.downloadReviewFindingDiff).mockResolvedValue({
      blob: new Blob(['diff']),
      filename: 'finding-52.diff',
      receipt: 'download-receipt-52',
      confirmationToken: 'download-confirmation-token-52',
    });
    const onChanged = vi.fn().mockResolvedValue(undefined);
    const { rerender } = render(
      <FindingActions
        finding={{ ...finding, latest_action: baseAction }}
        currentSnapshot
        onChanged={onChanged}
      />,
    );

    const changes = [
      { ...baseAction, id: 53 },
      { ...baseAction, status: 'running' as const },
      { ...baseAction, expected_head_sha: '0'.repeat(40) },
      { ...baseAction, patch_sha256: '1'.repeat(64) },
    ];
    for (const changedAction of changes) {
      await userEvent.click(screen.getByRole('button', { name: 'Download diff' }));
      await waitFor(() => expect(screen.getByRole('button', { name: 'Confirm and push' })).toBeEnabled());

      rerender(
        <FindingActions
          finding={{ ...finding, latest_action: changedAction }}
          currentSnapshot
          onChanged={onChanged}
        />,
      );
      if (changedAction.status === 'running') {
        await waitFor(() => expect(
          screen.queryByRole('button', { name: 'Confirm and push' }),
        ).not.toBeInTheDocument());
      } else {
        await waitFor(() => expect(
          screen.getByRole('button', { name: 'Confirm and push' }),
        ).toBeDisabled());
      }

      rerender(
        <FindingActions
          finding={{ ...finding, latest_action: baseAction }}
          currentSnapshot
          onChanged={onChanged}
        />,
      );
      await waitFor(() => expect(screen.getByRole('button', { name: 'Confirm and push' })).toBeDisabled());
    }
  });
});
