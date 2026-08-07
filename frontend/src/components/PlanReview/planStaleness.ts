const REASON_MESSAGES: Record<string, string> = {
  captured_repository_state_missing: 'This migrated Version has no historical repository snapshot.',
  repository_changed: 'The repository has changed since this Version was produced.',
  conversation_advanced: 'The related Task conversation has advanced since this Version was produced.',
  session_changed: 'The related Task session has changed since this Version was produced.',
};

const HARD_CONFLICT_MESSAGES: Record<string, string> = {
  version_plan_mismatch: 'This Version no longer belongs to the selected Plan.',
  target_task_missing: 'The related Task no longer exists.',
  project_missing: 'The selected Project no longer exists.',
  worker_missing: 'The selected Worker no longer exists.',
  worker_unavailable: 'The selected Worker is currently unavailable.',
  worker_repo_unavailable: 'The repository on the selected Worker cannot be inspected.',
  repository_fingerprint_invalid: 'The repository returned an invalid state fingerprint.',
  repository_unavailable: 'The repository is currently unavailable.',
};

function stringList(value: unknown, field: string): string[] {
  if (!value || typeof value !== 'object' || !(field in value)) return [];
  const candidate = (value as Record<string, unknown>)[field];
  return Array.isArray(candidate)
    ? candidate.filter((item): item is string => typeof item === 'string')
    : [];
}

export function planStalenessMessages(value: unknown): string[] {
  return stringList(value, 'reasons').map(
    (reason) => REASON_MESSAGES[reason] || 'The stored context is no longer current.',
  );
}

export function planHardConflictMessages(value: unknown): string[] {
  return stringList(value, 'hard_conflicts').map(
    (reason) => HARD_CONFLICT_MESSAGES[reason] || 'This Plan has an unresolved compatibility conflict.',
  );
}

export function planStalenessConfirmationMessage(
  value: unknown,
  action: 'approve' | 'apply' | 'execute',
): string {
  const reasons = stringList(value, 'reasons');
  const verb = action === 'approve'
    ? 'approve it'
    : action === 'apply'
      ? 'apply it'
      : 'create its execution Task';
  if (reasons.includes('captured_repository_state_missing')) {
    return `This migrated Plan Version has no historical repository snapshot. Continue and ${verb} after reviewing it against the current repository?`;
  }
  return `The repository or Task context has changed since this Version was produced. Continue and ${verb}?`;
}
