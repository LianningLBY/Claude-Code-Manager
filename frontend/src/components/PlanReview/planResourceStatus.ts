import type { PlanResource } from '../../api/client';

const LABELS: Record<PlanResource['display_state'], string> = {
  queued: 'Queued',
  planner: 'Planning',
  reviewer: 'Reviewing',
  waiting_user: 'Needs input',
  awaiting_review: 'Awaiting review',
  approved: 'Approved',
  rejected: 'Rejected',
  applied: 'Applied',
  failed: 'Failed',
  cancelled: 'Cancelled',
  archived: 'Archived',
};

export function planDisplayStateLabel(state: PlanResource['display_state']) {
  return LABELS[state] ?? state.replaceAll('_', ' ');
}
