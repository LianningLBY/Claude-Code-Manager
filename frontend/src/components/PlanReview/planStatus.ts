import type { Task } from '../../api/client';

const ACTIVE_STATUSES = new Set(['pending', 'in_progress', 'executing']);

export function getPlanStatusMeta(task: Task) {
  if (task.status === 'plan_review') {
    return { label: 'Ready · decision needed', className: 'bg-indigo-600 text-white' };
  }
  if (ACTIVE_STATUSES.has(task.status)) {
    const stage = task.plan_stage === 'reviewing' ? 'Reviewing' : 'Planning';
    const round = Math.max(1, task.plan_stage_round || 1);
    return {
      label: round > 1 ? `${stage} · Round ${round}` : stage,
      className: 'bg-indigo-500/15 text-indigo-300',
    };
  }
  if (task.status === 'completed' && task.plan_approved === true) {
    return { label: 'Approved', className: 'bg-green-500/15 text-green-300' };
  }
  if (task.status === 'completed' && task.plan_approved === false) {
    return { label: 'Rejected', className: 'bg-red-500/15 text-red-300' };
  }
  if (task.status === 'superseded') {
    return { label: 'Superseded', className: 'bg-gray-700 text-gray-400' };
  }
  if (task.status === 'failed') {
    return { label: 'Failed', className: 'bg-red-500/15 text-red-300' };
  }
  if (task.status === 'cancelled') {
    return { label: 'Cancelled', className: 'bg-gray-700 text-gray-400' };
  }
  return { label: task.status, className: 'bg-gray-700 text-gray-400' };
}
