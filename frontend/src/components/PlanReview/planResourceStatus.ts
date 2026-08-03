import type { PlanResource, PlanVersion } from '../../api/client';

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

const VERSION_LABELS: Record<PlanVersion['display_state'], string> = {
  applied: 'Applied',
  approved: 'Approved',
  rejected: 'Rejected',
  superseded: 'Superseded (not decided)',
  awaiting_review: 'Awaiting approval',
  draft: 'Draft',
};

export function planVersionDisplayLabel(version: PlanVersion) {
  return VERSION_LABELS[version.display_state];
}
