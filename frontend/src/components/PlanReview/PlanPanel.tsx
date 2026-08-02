import { useState } from 'react';

import { api, isApiRequestError } from '../../api/client';
import type { Task } from '../../api/client';
import { Check, ChevronDown, Loader2, Play, Trash2, X } from '../icons';
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
  const [expandedIds, setExpandedIds] = useState<Set<number>>(new Set());
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
      setExpandedIds((current) => {
        const next = new Set(current);
        next.delete(task.id);
        return next;
      });
    } catch (deleteError) {
      setError(deleteError instanceof Error ? deleteError.message : String(deleteError));
    } finally {
      setBusyId(null);
      onRefresh();
    }
  };

  const toggleExpanded = (id: number) => {
    setExpandedIds((current) => {
      const next = new Set(current);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
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
        const expanded = expandedIds.has(task.id);
        const status = getPlanStatusMeta(task);
        return (
          <article key={task.id} className="overflow-hidden rounded-xl border border-gray-700/70 bg-gray-800 shadow-sm">
            <div className="space-y-3 px-4 pt-4 sm:px-5">
              <div className="flex flex-wrap items-center gap-2">
                <h3 className="text-sm font-semibold text-foreground">
                  #{task.id} {task.title || 'Untitled Plan'}
                </h3>
                <span className={`rounded-full px-2.5 py-1 text-[10px] font-medium ${status.className}`}>
                  {status.label}
                </span>
                {task.plan_target_task_id != null && (
                  <span className="text-xs text-indigo-300">for Task #{task.plan_target_task_id}</span>
                )}
                <PlanRevisionBadge task={task} />
                <span className="flex-1" />
                <button
                  type="button"
                  onClick={() => void handleDelete(task)}
                  disabled={busyId === task.id}
                  className="flex items-center gap-1 rounded-lg border border-red-500/30 px-2.5 py-1.5 text-xs font-medium text-red-300 hover:bg-red-500/10 disabled:opacity-40"
                  aria-label={`Delete Plan #${task.id}`}
                  title="Delete Plan"
                >
                  {busyId === task.id
                    ? <Loader2 size={12} className="animate-spin" />
                    : <Trash2 size={12} />}
                  <span className="hidden sm:inline">Delete</span>
                </button>
              </div>
              {expanded ? (
                <>
                  <PlanProgress task={task} />
                  {task.metadata_?.plan_review_exhausted && (
                    <div className="rounded-lg border border-amber-500/30 bg-amber-500/10 px-3 py-2 text-xs leading-5 text-amber-300">
                      <span className="font-medium">Reviewer revision limit reached:</span>{' '}
                      {task.metadata_.plan_review_feedback || 'Review the Plan carefully before approval.'}
                    </div>
                  )}
                  <div className="relative rounded-xl border border-gray-700/70 bg-gray-950/55">
                    <pre className="max-h-[55vh] overflow-y-auto whitespace-pre-wrap p-4 font-mono text-xs leading-6 text-gray-300">
                      {task.plan_content}
                    </pre>
                    <button
                      type="button"
                      onClick={() => toggleExpanded(task.id)}
                      className="flex w-full items-center justify-center gap-1 border-t border-gray-800 py-2 text-xs text-indigo-300 hover:bg-gray-900/70"
                      aria-expanded="true"
                    >
                      Collapse Plan
                      <ChevronDown size={13} className="rotate-180" />
                    </button>
                  </div>
                </>
              ) : (
                <button
                  type="button"
                  onClick={() => toggleExpanded(task.id)}
                  className="flex w-full items-center justify-center gap-1 rounded-lg border border-gray-700/70 py-2 text-xs text-indigo-300 hover:bg-gray-900/50"
                  aria-expanded="false"
                >
                  View Plan
                  <ChevronDown size={13} />
                </button>
              )}
            </div>

            <div className="mt-3 border-t border-gray-700/70 bg-gray-900/35 px-4 py-3 sm:px-5">
              {ready ? (
                <div className="space-y-2.5">
                  <div className="flex gap-2">
                    <input
                      value={revisionDrafts[task.id] || ''}
                      onChange={(event) => setRevisionDrafts((current) => ({
                        ...current,
                        [task.id]: event.target.value,
                      }))}
                      onKeyDown={(event) => {
                        if (event.key === 'Enter' && !event.nativeEvent.isComposing) {
                          event.preventDefault();
                          void handleRevise(task);
                        }
                      }}
                      placeholder="Describe changes for a new revision…"
                      className="min-w-0 flex-1 rounded-lg border border-gray-700 bg-gray-800 px-3 py-2 text-xs text-gray-100 outline-none focus:border-indigo-500"
                    />
                    <button
                      type="button"
                      onClick={() => void handleRevise(task)}
                      disabled={!revisionDrafts[task.id]?.trim() || busyId === task.id}
                      className="rounded-lg border border-gray-600 px-3 py-2 text-xs font-medium text-gray-300 hover:bg-gray-700 disabled:opacity-40"
                    >
                      Revise
                    </button>
                  </div>
                  <div className="flex flex-wrap items-center gap-2">
                    <button
                      type="button"
                      onClick={() => void handleReject(task.id)}
                      disabled={busyId === task.id}
                      className="flex items-center gap-1 rounded-lg border border-red-500/40 px-3 py-2 text-xs font-medium text-red-300 hover:bg-red-500/10 disabled:opacity-40"
                    >
                      <X size={12} /> Reject
                    </button>
                    <span className="flex-1" />
                    <button
                      type="button"
                      onClick={() => void approve(task, 'approve-only')}
                      disabled={busyId === task.id}
                      className="flex items-center gap-1 rounded-lg border border-green-500/40 px-3 py-2 text-xs font-medium text-green-300 hover:bg-green-500/10 disabled:opacity-40"
                    >
                      <Check size={12} /> Approve only
                    </button>
                    <button
                      type="button"
                      onClick={() => void approve(
                        task,
                        standalone ? 'create-execution' : 'attach-to-target',
                      )}
                      disabled={busyId === task.id}
                      className="flex items-center gap-1.5 rounded-lg bg-green-600 px-3 py-2 text-xs font-medium text-white hover:bg-green-500 disabled:opacity-40"
                    >
                      {busyId === task.id
                        ? <Loader2 size={12} className="animate-spin" />
                        : standalone ? <Play size={12} /> : <Check size={12} />}
                      {standalone
                        ? 'Approve & create execution Task'
                        : `Approve & open Task #${task.plan_target_task_id}`}
                    </button>
                  </div>
                </div>
              ) : (
                <div className="flex justify-end">
                  <button
                    type="button"
                    onClick={() => void approve(task, 'create-execution')}
                    disabled={busyId === task.id}
                    className="flex items-center gap-1.5 rounded-lg bg-indigo-600 px-3 py-2 text-xs font-medium text-white hover:bg-indigo-500 disabled:opacity-40"
                  >
                    {busyId === task.id ? <Loader2 size={12} className="animate-spin" /> : <Play size={12} />}
                    Create execution Task
                  </button>
                </div>
              )}
            </div>
          </article>
        );
      })}
    </section>
  );
}
