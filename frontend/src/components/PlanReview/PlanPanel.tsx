import { useEffect, useState } from 'react';

import { api, isApiRequestError } from '../../api/client';
import type { Task } from '../../api/client';
import { Check, ChevronRight, Loader2, Play, Trash2, X } from '../icons';
import { MarkdownContent } from '../MarkdownContent';
import { CollapsiblePlanningRequest } from './CollapsiblePlanningRequest';
import { PlanRevisionBadge } from '../Tasks/TaskBadges';
import { PlanProgress } from './PlanProgress';
import { getPlanStatusMeta } from './planStatus';

interface PlanPanelProps {
  tasks: Task[];
  onRefresh: () => void;
}

export function PlanPanel({ tasks, onRefresh }: PlanPanelProps) {
  const [busyId, setBusyId] = useState<number | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [revisionDrafts, setRevisionDrafts] = useState<Record<number, string>>({});
  const [selectedPlanId, setSelectedPlanId] = useState<number | null>(null);
  const planTasks = tasks.filter((task) =>
    task.mode === 'plan'
    && (
      (task.status === 'plan_review' && task.plan_content)
      || (
        task.status === 'completed'
        && task.plan_approved === true
        && task.plan_target_task_id == null
        && task.plan_execution_task_id == null
      )
    )
  );

  const selectedPlan = planTasks.find((task) => task.id === selectedPlanId) || null;

  useEffect(() => {
    if (selectedPlanId != null && selectedPlan == null) {
      setSelectedPlanId(null);
    }
  }, [selectedPlan, selectedPlanId]);

  useEffect(() => {
    if (selectedPlanId == null) return;
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') setSelectedPlanId(null);
    };
    const previousOverflow = document.body.style.overflow;
    document.body.style.overflow = 'hidden';
    document.addEventListener('keydown', onKeyDown);
    return () => {
      document.body.style.overflow = previousOverflow;
      document.removeEventListener('keydown', onKeyDown);
    };
  }, [selectedPlanId]);

  if (planTasks.length === 0) return null;

  const approve = async (
    task: Task,
    action: 'approve-only' | 'create-execution' | 'attach-to-target',
  ) => {
    const routing = {
      provider: task.provider,
      model: task.model,
      codex_service_tier: task.codex_service_tier,
    };
    setBusyId(task.id);
    setError(null);
    let approved = task.status === 'completed' && task.plan_approved === true;
    try {
      if (!approved) {
        try {
          await api.approvePlan(task.id, routing);
          approved = true;
        } catch (approvalError) {
          const detail = isApiRequestError(approvalError) ? approvalError.detail : null;
          const stale = detail && typeof detail === 'object' && 'staleness' in detail;
          if (!stale || !window.confirm(
            'This Plan was created from older conversation or repository state. Approve it anyway?',
          )) {
            throw approvalError;
          }
          await api.approvePlan(task.id, routing, true);
          approved = true;
        }
      }
      if (approved && action === 'create-execution') {
        const result = await api.createPlanExecutionTask(task.id);
        window.location.hash = `#/tasks/chat/${result.execution_task.id}`;
      } else if (approved && task.plan_target_task_id != null) {
        const storageKey = `ccm-plan-dismissed-${task.plan_target_task_id}`;
        let dismissed = new Set<number>();
        try {
          const parsed = JSON.parse(localStorage.getItem(storageKey) || '[]');
          if (Array.isArray(parsed)) {
            dismissed = new Set(parsed.filter((value): value is number => Number.isInteger(value)));
          }
        } catch { /* unavailable or malformed storage starts from an empty set */ }
        if (action === 'attach-to-target') dismissed.delete(task.id);
        else dismissed.add(task.id);
        try {
          localStorage.setItem(storageKey, JSON.stringify([...dismissed]));
        } catch { /* the server-side approval remains valid without local preferences */ }
        if (action === 'attach-to-target') {
          window.location.hash = `#/tasks/chat/${task.plan_target_task_id}`;
        }
      }
    } catch (approvalError) {
      setError(approvalError instanceof Error ? approvalError.message : String(approvalError));
    } finally {
      setBusyId(null);
      onRefresh();
    }
  };

  const handleReject = async (id: number) => {
    setBusyId(id);
    setError(null);
    try {
      await api.rejectPlan(id);
    } catch (rejectError) {
      setError(rejectError instanceof Error ? rejectError.message : String(rejectError));
    } finally {
      setBusyId(null);
      onRefresh();
    }
  };

  const handleRevise = async (task: Task) => {
    const feedback = (revisionDrafts[task.id] || '').trim();
    if (!feedback) return;
    setBusyId(task.id);
    setError(null);
    try {
      await api.revisePlan(task.id, feedback);
      setRevisionDrafts((current) => ({ ...current, [task.id]: '' }));
    } catch (revisionError) {
      setError(revisionError instanceof Error ? revisionError.message : String(revisionError));
    } finally {
      setBusyId(null);
      onRefresh();
    }
  };

  const handleDelete = async (task: Task) => {
    if (!window.confirm(
      `Delete Plan #${task.id} permanently? Its Plan content and review history will be removed.`,
    )) return;
    setBusyId(task.id);
    setError(null);
    try {
      await api.deleteTask(task.id);
      if (task.plan_target_task_id != null) {
        const storageKey = `ccm-plan-dismissed-${task.plan_target_task_id}`;
        try {
          const parsed = JSON.parse(localStorage.getItem(storageKey) || '[]');
          if (Array.isArray(parsed)) {
            localStorage.setItem(
              storageKey,
              JSON.stringify(parsed.filter((value) => value !== task.id)),
            );
          }
        } catch { /* stale local preferences are harmless */ }
      }
      if (selectedPlanId === task.id) setSelectedPlanId(null);
    } catch (deleteError) {
      setError(deleteError instanceof Error ? deleteError.message : String(deleteError));
    } finally {
      setBusyId(null);
      onRefresh();
    }
  };

  return (
    <section className="space-y-3" aria-label="Plans awaiting review">
      <div className="flex items-center gap-2">
        <h2 className="font-semibold text-foreground">Plans Awaiting Review</h2>
        <span className="rounded-full bg-indigo-600 px-2 py-0.5 text-xs font-bold text-white">{planTasks.length}</span>
      </div>
      {error && (
        <div className="rounded-lg border border-red-500/30 bg-red-500/10 px-3 py-2 text-xs text-red-400">
          {error}
        </div>
      )}
      {planTasks.map((task) => {
        const ready = task.status === 'plan_review';
        const standalone = task.plan_target_task_id == null;
        const status = getPlanStatusMeta(task);
        return (
          <article
            key={task.id}
            className="flex flex-col gap-3 rounded-xl border border-gray-700/70 bg-gray-800 px-4 py-3.5 shadow-sm sm:flex-row sm:items-center sm:px-5"
          >
            <div className="flex min-w-0 flex-1 flex-wrap items-center gap-2">
              <h3 className="min-w-0 truncate text-sm font-semibold text-foreground">
                #{task.id} {task.title || 'Untitled Plan'}
              </h3>
              <span className={`rounded-full px-2.5 py-1 text-[10px] font-medium ${status.className}`}>
                {status.label}
              </span>
              <span className="rounded-full border border-gray-600 px-2.5 py-1 text-[10px] font-medium text-gray-400">
                {standalone ? 'Standalone' : `Task #${task.plan_target_task_id}`}
              </span>
              <PlanRevisionBadge task={task} />
            </div>
            <div className="flex shrink-0 items-center justify-end gap-2">
              {ready ? (
                <button
                  type="button"
                  onClick={() => {
                    setError(null);
                    setSelectedPlanId(task.id);
                  }}
                  className="flex items-center gap-1 rounded-lg bg-indigo-600 px-3 py-2 text-xs font-medium text-white hover:bg-indigo-500"
                  aria-label={`Review Plan #${task.id}`}
                >
                  Review <ChevronRight size={13} />
                </button>
              ) : (
                <button
                  type="button"
                  onClick={() => void approve(task, 'create-execution')}
                  disabled={busyId === task.id}
                  className="flex items-center gap-1.5 rounded-lg border border-indigo-500 px-3 py-2 text-xs font-medium text-indigo-300 hover:bg-indigo-500/10 disabled:opacity-40"
                >
                  {busyId === task.id
                    ? <Loader2 size={12} className="animate-spin" />
                    : <Play size={12} />}
                  Create execution Task
                </button>
              )}
              <button
                type="button"
                onClick={() => void handleDelete(task)}
                disabled={busyId === task.id}
                className="rounded-lg p-2 text-gray-500 hover:bg-red-500/10 hover:text-red-300 disabled:opacity-40"
                aria-label={`Delete Plan #${task.id}`}
                title="Delete Plan"
              >
                {busyId === task.id
                  ? <Loader2 size={14} className="animate-spin" />
                  : <Trash2 size={14} />}
              </button>
            </div>
          </article>
        );
      })}

      {selectedPlan && (() => {
        const ready = selectedPlan.status === 'plan_review';
        const standalone = selectedPlan.plan_target_task_id == null;
        const status = getPlanStatusMeta(selectedPlan);
        return (
          <div
            className="fixed inset-0 z-[70] flex items-end justify-center bg-black/60 sm:items-center sm:p-5"
            onMouseDown={(event) => {
              if (event.target === event.currentTarget) setSelectedPlanId(null);
            }}
          >
            <div
              role="dialog"
              aria-modal="true"
              aria-label={`Review Plan #${selectedPlan.id}`}
              className="flex h-[100dvh] w-full flex-col overflow-hidden bg-gray-900 shadow-2xl sm:h-[min(86vh,780px)] sm:max-w-5xl sm:rounded-2xl sm:border sm:border-gray-700"
            >
              <header className="shrink-0 border-b border-gray-800 px-4 pb-3 pt-[max(0.75rem,env(safe-area-inset-top))] sm:px-5 sm:pt-4">
                <div className="flex flex-wrap items-center gap-2">
                  <h2 className="min-w-0 flex-1 text-base font-semibold text-gray-100 sm:text-lg">
                    #{selectedPlan.id} {selectedPlan.title || 'Untitled Plan'}
                  </h2>
                  <span className={`rounded-full px-2.5 py-1 text-[10px] font-medium ${status.className}`}>
                    {status.label}
                  </span>
                  <span className="rounded-full border border-gray-600 px-2.5 py-1 text-[10px] font-medium text-gray-400">
                    {standalone ? 'Standalone' : `Task #${selectedPlan.plan_target_task_id}`}
                  </span>
                  <PlanRevisionBadge task={selectedPlan} />
                  <button
                    type="button"
                    onClick={() => void handleDelete(selectedPlan)}
                    disabled={busyId === selectedPlan.id}
                    className="rounded-lg p-2 text-gray-500 hover:bg-red-500/10 hover:text-red-300 disabled:opacity-40"
                    aria-label={`Delete Plan #${selectedPlan.id}`}
                    title="Delete Plan"
                  >
                    <Trash2 size={15} />
                  </button>
                  <button
                    type="button"
                    onClick={() => setSelectedPlanId(null)}
                    className="rounded-lg p-2 text-gray-500 hover:bg-gray-800 hover:text-gray-300"
                    aria-label="Close Plan review"
                  >
                    <X size={16} />
                  </button>
                </div>
                <div className="mt-3">
                  <PlanProgress task={selectedPlan} />
                </div>
              </header>

              <div className="min-h-0 flex-1 overflow-y-auto px-4 py-3 sm:px-5 sm:py-4">
                {error && (
                  <div className="mb-3 rounded-lg border border-red-500/30 bg-red-500/10 px-3 py-2 text-xs text-red-400">
                    {error}
                  </div>
                )}
                {selectedPlan.metadata_?.plan_review_exhausted && (
                  <div className="mb-3 rounded-lg border border-amber-500/30 bg-amber-500/10 px-3 py-2 text-xs leading-5 text-amber-300">
                    <span className="font-medium">Reviewer revision limit reached:</span>{' '}
                    {selectedPlan.metadata_.plan_review_feedback || 'Review the Plan carefully before approval.'}
                  </div>
                )}
                {selectedPlan.description && (
                  <CollapsiblePlanningRequest
                    key={selectedPlan.id}
                    content={selectedPlan.description}
                  />
                )}
                <div className="min-h-48 rounded-xl border border-gray-800 bg-gray-950/55 p-4">
                  <MarkdownContent
                    content={selectedPlan.plan_content || ''}
                    className="text-xs text-gray-300 sm:text-[13px]"
                  />
                </div>
              </div>

              <footer className="shrink-0 border-t border-gray-800 bg-gray-900 px-4 pb-[max(1rem,env(safe-area-inset-bottom))] pt-3 sm:px-5 sm:pb-4">
                {ready ? (
                  <div className="space-y-2.5">
                    <div className="flex gap-2">
                      <input
                        value={revisionDrafts[selectedPlan.id] || ''}
                        onChange={(event) => setRevisionDrafts((current) => ({
                          ...current,
                          [selectedPlan.id]: event.target.value,
                        }))}
                        onKeyDown={(event) => {
                          if (event.key === 'Enter' && !event.nativeEvent.isComposing) {
                            event.preventDefault();
                            void handleRevise(selectedPlan);
                          }
                        }}
                        placeholder="Describe changes for a new revision…"
                        className="min-w-0 flex-1 rounded-lg border border-gray-700 bg-gray-800 px-3 py-2 text-xs text-gray-100 outline-none focus:border-indigo-500"
                      />
                      <button
                        type="button"
                        onClick={() => void handleRevise(selectedPlan)}
                        disabled={!revisionDrafts[selectedPlan.id]?.trim() || busyId === selectedPlan.id}
                        className="rounded-lg border border-gray-600 px-3 py-2 text-xs font-medium text-gray-300 hover:bg-gray-800 disabled:opacity-40"
                      >
                        Revise
                      </button>
                    </div>
                    <div className="flex flex-wrap items-center gap-2">
                      <button
                        type="button"
                        onClick={() => void handleReject(selectedPlan.id)}
                        disabled={busyId === selectedPlan.id}
                        className="rounded-lg border border-red-500/40 px-3 py-2 text-xs font-medium text-red-300 hover:bg-red-500/10 disabled:opacity-40"
                      >
                        Reject
                      </button>
                      <span className="flex-1" />
                      <button
                        type="button"
                        onClick={() => void approve(selectedPlan, 'approve-only')}
                        disabled={busyId === selectedPlan.id}
                        className="rounded-lg border border-green-500/40 px-3 py-2 text-xs font-medium text-green-300 hover:bg-green-500/10 disabled:opacity-40"
                      >
                        Approve only
                      </button>
                      <button
                        type="button"
                        onClick={() => void approve(
                          selectedPlan,
                          standalone ? 'create-execution' : 'attach-to-target',
                        )}
                        disabled={busyId === selectedPlan.id}
                        className="flex items-center gap-1.5 rounded-lg bg-green-600 px-3 py-2 text-xs font-medium text-white hover:bg-green-500 disabled:opacity-40"
                      >
                        {busyId === selectedPlan.id
                          ? <Loader2 size={12} className="animate-spin" />
                          : standalone ? <Play size={12} /> : <Check size={12} />}
                        {standalone
                          ? 'Approve & create execution Task'
                          : `Approve & open Task #${selectedPlan.plan_target_task_id}`}
                      </button>
                    </div>
                  </div>
                ) : (
                  <div className="flex justify-end">
                    <button
                      type="button"
                      onClick={() => void approve(selectedPlan, 'create-execution')}
                      disabled={busyId === selectedPlan.id}
                      className="flex items-center gap-1.5 rounded-lg bg-indigo-600 px-3 py-2 text-xs font-medium text-white hover:bg-indigo-500 disabled:opacity-40"
                    >
                      {busyId === selectedPlan.id
                        ? <Loader2 size={12} className="animate-spin" />
                        : <Play size={12} />}
                      Create execution Task
                    </button>
                  </div>
                )}
              </footer>
            </div>
          </div>
        );
      })()}
    </section>
  );
}
