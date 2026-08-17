import { useEffect, useState } from 'react';
import { api } from '../../api/client';
import type { PRReview, PRReviewResult, Task } from '../../api/client';
import { ArrowLeft, GitPullRequest } from '../icons';
import { MarkdownContent } from '../MarkdownContent';
import { PRReviewResultCard } from '../PRReview/PRReviewResultCard';

interface PRMonitorTaskDetailProps {
  task: Pick<Task, 'title' | 'description' | 'metadata_'>;
  result?: PRReviewResult | null;
  onBack: () => void;
}

/** Read-only Task view for the stable PR Monitor display projection. */
export function PRMonitorTaskDetail({ task, result, onBack }: PRMonitorTaskDetailProps) {
  const [reviewDetail, setReviewDetail] = useState<PRReview | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [detailError, setDetailError] = useState<string | null>(null);

  useEffect(() => {
    let active = true;
    setReviewDetail(null);
    setDetailError(null);
    const reviewId = result?.review_id ?? task.metadata_?.pr_monitor_review_id;
    if (reviewId == null) {
      setDetailLoading(false);
      return () => { active = false; };
    }
    setDetailLoading(true);
    void api.getReviewDetail(reviewId)
      .then((detail) => {
        if (active) setReviewDetail(detail);
      })
      .catch((error: unknown) => {
        if (active) setDetailError(String(error));
      })
      .finally(() => {
        if (active) setDetailLoading(false);
      });
    return () => { active = false; };
  }, [result?.review_id, result?.updated_at, task.metadata_?.pr_monitor_review_id]);

  const reviewerRuns = reviewDetail?.reviewer_runs ?? [];

  return (
    <div className="flex h-full min-h-0 flex-col bg-gray-950/20">
      <div className="flex shrink-0 items-center gap-3 border-b border-gray-800 px-4 py-3">
        <button
          type="button"
          onClick={onBack}
          className="rounded p-1.5 text-gray-400 transition-colors hover:bg-gray-800 hover:text-gray-200"
          title="Back to tasks"
          aria-label="Back to tasks"
        >
          <ArrowLeft size={16} />
        </button>
        <div className="min-w-0">
          <div className="flex items-center gap-1.5 text-xs font-medium uppercase tracking-wide text-indigo-300">
            <GitPullRequest size={14} aria-hidden="true" /> PR Monitor result
          </div>
          <h2 className="truncate text-sm font-semibold text-gray-100">
            {task.title || 'Pull request review'}
          </h2>
        </div>
      </div>
      <div className="min-h-0 flex-1 overflow-y-auto p-4">
        <div className="space-y-4">
          {result && <PRReviewResultCard result={result} readOnly />}
          {detailLoading && <p className="text-xs text-gray-500">Loading reviewer details…</p>}
          {detailError && (
            <p className="text-xs text-amber-300" role="alert">
              Reviewer details could not be loaded. The aggregate result will continue to refresh.
            </p>
          )}
          {!result && !reviewDetail && !detailLoading && !detailError && (
            <div className="rounded border border-gray-700 bg-gray-900/40 p-4 text-sm text-gray-400">
              The PR review result is not available yet. Refresh this Task to check again.
            </div>
          )}
          {reviewDetail && (
            <section aria-label="Reviewer details" className="space-y-3">
              {(reviewDetail.review_summary || reviewDetail.display_summary) && (
                <div className="rounded border border-gray-700 bg-gray-900/40 p-3">
                  <h3 className="mb-2 text-sm font-medium text-gray-200">Review summary</h3>
                  <MarkdownContent
                    content={reviewDetail.display_summary || reviewDetail.review_summary || ''}
                    className="text-xs text-gray-300"
                  />
                </div>
              )}
              {reviewerRuns.length > 0 && (
                <div className="space-y-2">
                  <h3 className="text-sm font-medium text-gray-200">Reviewer results</h3>
                  {reviewerRuns.map((run) => (
                    <ReviewerResult key={run.id} run={run} />
                  ))}
                </div>
              )}
            </section>
          )}
        </div>
      </div>
    </div>
  );
}

function roleLabel(role: string): string {
  return role.split('_').map((part) => part.charAt(0).toUpperCase() + part.slice(1)).join(' ');
}

function ReviewerResult({ run }: { run: NonNullable<PRReview['reviewer_runs']>[number] }) {
  const severityOrder: Record<string, number> = { critical: 0, high: 1, medium: 2, low: 3 };
  const findings = [...(run.findings || [])].sort(
    (a, b) => (severityOrder[a.severity] ?? 9) - (severityOrder[b.severity] ?? 9),
  );
  return (
    <article className="rounded border border-gray-700 bg-gray-900/40 p-3">
      <div className="flex flex-wrap items-center justify-between gap-2 text-sm">
        <h4 className="font-medium text-gray-200">{roleLabel(run.role)}</h4>
        <span className="text-xs text-gray-400">
          {run.verdict ? `${run.status} · ${run.verdict}` : run.status}
        </span>
      </div>
      {run.result_body && <MarkdownContent content={run.result_body} className="mt-2 text-xs text-gray-300" />}
      {run.outcome_kind === 'infrastructure_error' && !run.result_body && (
        <p className="mt-2 text-xs text-red-300">This reviewer did not produce a code verdict.</p>
      )}
      {run.error_message && <p className="mt-2 text-xs text-red-300">{run.error_message}</p>}
      {findings.map((finding) => (
        <div key={finding.id} className="mt-3 space-y-1 border-l-2 border-orange-500 pl-3 text-xs">
          <p className="text-orange-300">
            [{finding.severity}] {finding.path}{finding.line ? `:${finding.line}` : ''} · {finding.title}
          </p>
          <p className="text-gray-300">Evidence: {finding.evidence}</p>
          <p className="text-gray-400">Impact: {finding.impact}</p>
          <p className="text-gray-400">Required fix: {finding.required_fix}</p>
          <p className="text-gray-400">Test: {finding.test}</p>
          <p className="text-gray-500">Thread: {finding.thread_status}</p>
        </div>
      ))}
    </article>
  );
}
