import { describe, expect, it } from 'vitest';

import {
  planHardConflictMessages,
  planStalenessConfirmationMessage,
  planStalenessMessages,
} from './planStaleness';

describe('Plan staleness copy', () => {
  it('explains a migrated Version without exposing an internal reason code', () => {
    const state = {
      reasons: ['captured_repository_state_missing'],
      hard_conflicts: [],
    };

    expect(planStalenessMessages(state)).toEqual([
      'This migrated Version has no historical repository snapshot.',
    ]);
    expect(planStalenessConfirmationMessage(state, 'execute')).toContain(
      'Continue and create its execution Task',
    );
  });

  it('translates genuine hard conflicts', () => {
    expect(planHardConflictMessages({
      hard_conflicts: ['target_task_missing', 'worker_unavailable'],
    })).toEqual([
      'The related Task no longer exists.',
      'The selected Worker is currently unavailable.',
    ]);
  });
});
