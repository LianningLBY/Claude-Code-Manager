import { useState } from 'react';
import { api, isApiRequestError } from '../../api/client';
import type { Task } from '../../api/client';
import { Check, Loader2, Play, X } from '../icons';
import { PlanRevisionBadge } from '../Tasks/TaskBadges';

interface PlanPanelProps {
  tasks: Task[];
  onRefresh: () => void;
}

export function PlanPanel({ tasks, onRefresh }: PlanPanelProps) {
  const [busyId, setBusyId] = useState<number | null>(null);
  const [error, setError] = useState<string | null>(null);
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

  const handleApprove = async (task: Task) => {
    const routing = {
      provider: task.provider,
      model: task.model,
      codex_service_tier: task.codex_service_tier,
    };
    setBusyId(task.id);
    setError(null);
    try {
      await api.approvePlan(task.id, routing);
    } catch (error) {
      const detail = isApiRequestError(error) ? error.detail : null;
      const stale = detail && typeof detail === 'object'
        && 'staleness' in detail;
      if (!stale || !window.confirm(
        'This Plan was created from older conversation or repository state. Approve it anyway?',
      )) {
        setError(error instanceof Error ? error.message : String(error));
        return;
      }
      try {
        await api.approvePlan(task.id, routing, true);
      } catch (confirmedError) {
        setError(
          confirmedError instanceof Error
            ? confirmedError.message
            : String(confirmedError),
        );
      }
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
      setError(
        rejectError instanceof Error
          ? rejectError.message
          : String(rejectError),
      );
    } finally {
      setBusyId(null);
      onRefresh();
    }
  };

  const handleRevise = async (task: Task) => {
    const feedback = window.prompt(
      'What should the revised Plan change?',
      task.metadata_?.plan_review_feedback || '',
    )?.trim();
    if (!feedback) return;
    setBusyId(task.id);
    setError(null);
    try {
      await api.revisePlan(task.id, feedback);
    } catch (revisionError) {
      setError(
        revisionError instanceof Error
          ? revisionError.message
          : String(revisionError),
      );
    } finally {
      setBusyId(null);
      onRefresh();
    }
  };

  const handleCreateExecution = async (id: number) => {
    setBusyId(id);
    setError(null);
    try {
      const result = await api.createPlanExecutionTask(id);
      window.location.hash = `#/tasks/chat/${result.execution_task.id}`;
    } catch (executionError) {
      setError(
        executionError instanceof Error
          ? executionError.message
          : String(executionError),
      );
    } finally {
      setBusyId(null);
      onRefresh();
    }
  };

  return (
    <div className="space-y-3">
      <h2 className="text-foreground font-semibold flex items-center gap-2">
        Plans Awaiting Review
        <span className="bg-yellow-500 text-black text-xs px-2 py-0.5 rounded-full font-bold">{planTasks.length}</span>
      </h2>
      {error && (
        <div className="rounded border border-red-500/30 bg-red-500/10 px-3 py-2 text-xs text-red-400">
          {error}
        </div>
      )}
      {planTasks.map((task) => (
        <div key={task.id} className="bg-gray-800 rounded-lg p-4 space-y-3">
          <div className="flex items-center justify-between">
            <div>
              <span className="text-foreground font-medium text-sm">{task.title}</span>
              <span className="text-gray-500 text-xs ml-2">#{task.id}</span>
              {task.plan_target_task_id != null && (
                <span className="text-indigo-300 text-xs ml-2">
                  for Task #{task.plan_target_task_id}
                </span>
              )}
              {(task.supersedes_plan_task_id
                || task.metadata_?.plan_superseded_by_task_id) && (
                <span className="ml-2 inline-flex gap-1">
                  <PlanRevisionBadge task={task} />
                </span>
              )}
            </div>
            <div className="flex gap-2">
              {task.status === 'plan_review' ? (
                <>
                  <button
                    onClick={() => handleApprove(task)}
                    disabled={busyId === task.id}
                    className="flex items-center gap-1 bg-green-600 hover:bg-green-700 text-white px-3 py-1.5 rounded text-xs font-medium disabled:opacity-50"
                  >
                    {busyId === task.id
                      ? <Loader2 size={14} className="animate-spin" />
                      : <Check size={14} />}
                    Approve
                  </button>
                  <button
                    onClick={() => handleReject(task.id)}
                    disabled={busyId === task.id}
                    className="flex items-center gap-1 bg-red-600 hover:bg-red-700 text-white px-3 py-1.5 rounded text-xs font-medium disabled:opacity-50"
                  >
                    <X size={14} /> Reject
                  </button>
                  <button
                    onClick={() => void handleRevise(task)}
                    disabled={busyId === task.id}
                    className="rounded bg-gray-700 px-3 py-1.5 text-xs font-medium text-gray-300 hover:bg-gray-600 disabled:opacity-50"
                  >
                    Revise
                  </button>
                </>
              ) : (
                <button
                  onClick={() => handleCreateExecution(task.id)}
                  disabled={busyId === task.id}
                  className="flex items-center gap-1 bg-indigo-600 hover:bg-indigo-500 text-white px-3 py-1.5 rounded text-xs font-medium disabled:opacity-50"
                >
                  {busyId === task.id
                    ? <Loader2 size={14} className="animate-spin" />
                    : <Play size={14} />}
                  Create execution Task
                </button>
              )}
            </div>
          </div>
          {task.plan_pipeline_config && (
            <div className="text-[11px] text-gray-500">
              Planner: {task.plan_pipeline_config.planner.primary.provider}
              {' / '}{task.plan_pipeline_config.planner.primary.model}
              {' (fallback: '}
              {task.plan_pipeline_config.planner.fallback.provider}
              {' / '}{task.plan_pipeline_config.planner.fallback.model})
              {task.plan_pipeline_config.reviewer.enabled && (
                <>
                  {' · Reviewer: '}
                  {task.plan_pipeline_config.reviewer.primary.provider}
                  {' / '}{task.plan_pipeline_config.reviewer.primary.model}
                  {' (fallback: '}
                  {task.plan_pipeline_config.reviewer.fallback.provider}
                  {' / '}{task.plan_pipeline_config.reviewer.fallback.model})
                </>
              )}
            </div>
          )}
          {task.metadata_?.plan_review_exhausted && (
            <div className="rounded border border-amber-500/30 bg-amber-500/10 px-3 py-2 text-xs text-amber-300">
              Reviewer still requested changes after the revision limit:
              {' '}{task.metadata_.plan_review_feedback || 'Review the Plan carefully before approval.'}
            </div>
          )}
          <div className="text-xs text-gray-400 bg-gray-900 rounded p-3 max-h-60 overflow-y-auto whitespace-pre-wrap font-mono">
            {task.plan_content}
          </div>
        </div>
      ))}
    </div>
  );
}
