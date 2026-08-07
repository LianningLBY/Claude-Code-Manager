import type { Task } from '../../api/client';

const ACTIVE_PLAN_STAGES = new Set(['planning', 'reviewing']);

function titleCaseStatus(status: string): string {
  return status
    .replace(/_/g, ' ')
    .replace(/\b\w/g, (letter) => letter.toUpperCase());
}

export function getTaskStatusLabel(task: Task): string {
  if (task.background_active) return 'Background';

  if (
    task.mode === 'plan'
    && ['in_progress', 'executing'].includes(task.status)
    && task.plan_stage
    && ACTIVE_PLAN_STAGES.has(task.plan_stage)
  ) {
    const label = titleCaseStatus(task.plan_stage);
    const round = Number.isInteger(task.plan_stage_round)
      ? Math.max(1, task.plan_stage_round as number)
      : 1;
    return round > 1 ? `${label} · Round ${round}` : label;
  }

  return titleCaseStatus(task.status);
}
