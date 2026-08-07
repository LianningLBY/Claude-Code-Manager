import type { Task } from '../../api/client';
import { Check, Loader2, X } from '../icons';

type StepState = 'done' | 'active' | 'pending' | 'failed';

interface PlanStep {
  label: string;
  title?: string;
  state: StepState;
}

const ACTIVE_PLAN_STATUSES = new Set(['pending', 'in_progress', 'executing']);

function buildSteps(task: Task): PlanStep[] {
  const round = Math.max(1, task.plan_stage_round || 1);
  const active = ACTIVE_PLAN_STATUSES.has(task.status);
  const reviewing = task.plan_stage === 'reviewing';
  const pipelineFinished = task.status === 'plan_review'
    || task.status === 'completed'
    || task.status === 'superseded';
  const failed = task.status === 'failed';
  const cancelled = task.status === 'cancelled';
  const plannerModel = task.plan_pipeline_config?.planner.primary.model;
  const reviewerModel = task.plan_pipeline_config?.reviewer.enabled
    ? task.plan_pipeline_config.reviewer.primary.model
    : null;

  const steps: PlanStep[] = [
    {
      label: 'Planner',
      title: plannerModel ? `Planner · ${plannerModel}` : 'Planner',
      state: active && !reviewing && round === 1
        ? 'active'
        : (reviewing || round > 1 || pipelineFinished || failed || cancelled)
          ? 'done'
          : 'pending',
    },
  ];

  if (reviewerModel) {
    steps.push({
      label: 'Reviewer',
      title: `Reviewer · ${reviewerModel}`,
      state: active && reviewing && round === 1
        ? 'active'
        : (round > 1 || pipelineFinished || failed || cancelled)
          ? 'done'
          : 'pending',
    });
  }

  if (round > 1) {
    steps.push({
      label: `Round ${round}`,
      title: `${reviewing ? 'Reviewer' : 'Planner'} · Round ${round}`,
      state: active ? 'active' : (failed ? 'failed' : 'done'),
    });
  }

  let decisionState: StepState = 'pending';
  let decisionLabel = 'Your decision';
  if (task.status === 'plan_review') decisionState = 'active';
  else if (task.status === 'completed' && task.plan_approved === true) {
    decisionState = 'done';
    decisionLabel = 'Approved';
  } else if (task.status === 'completed' && task.plan_approved === false) {
    decisionState = 'failed';
    decisionLabel = 'Rejected';
  } else if (task.status === 'superseded') {
    decisionState = 'failed';
    decisionLabel = 'Superseded';
  } else if (failed) {
    decisionState = 'failed';
    decisionLabel = 'Failed';
  } else if (cancelled) {
    decisionLabel = 'Cancelled';
  }
  steps.push({ label: decisionLabel, state: decisionState });
  return steps;
}

function StepIcon({ state }: { state: StepState }) {
  if (state === 'done') return <Check size={11} />;
  if (state === 'failed') return <X size={11} />;
  if (state === 'active') return <Loader2 size={10} className="animate-spin" />;
  return null;
}

export function PlanProgress({ task, compact = false }: { task: Task; compact?: boolean }) {
  const steps = buildSteps(task);
  return (
    <div className="flex min-w-0 items-center overflow-x-auto py-1" aria-label="Plan progress">
      {steps.map((step, index) => (
        <div key={`${step.label}-${index}`} className="flex shrink-0 items-center">
          {index > 0 && <span className={`${compact ? 'w-3' : 'w-5 sm:w-8'} mx-1 h-px bg-gray-700`} />}
          <span
            className={`flex items-center gap-1.5 text-[10px] sm:text-[11px] ${
              step.state === 'done'
                ? 'text-green-300'
                : step.state === 'active'
                  ? 'font-medium text-indigo-300'
                  : step.state === 'failed'
                    ? 'font-medium text-red-300'
                    : 'text-gray-600'
            }`}
            title={step.title || step.label}
          >
            <span className={`flex h-4 w-4 shrink-0 items-center justify-center rounded-full border ${
              step.state === 'done'
                ? 'border-green-500 bg-green-600 text-white'
                : step.state === 'active'
                  ? 'border-indigo-400 bg-indigo-500/15 text-indigo-300'
                  : step.state === 'failed'
                    ? 'border-red-500 bg-red-600 text-white'
                    : 'border-gray-600 text-gray-600'
            }`}>
              <StepIcon state={step.state} />
            </span>
            <span>{step.label}</span>
          </span>
        </div>
      ))}
    </div>
  );
}
