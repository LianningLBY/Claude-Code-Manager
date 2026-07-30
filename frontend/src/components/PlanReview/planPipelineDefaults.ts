import type { PlanPipelineConfig } from '../../api/client';

export const FALLBACK_PLAN_PIPELINE_CONFIG: PlanPipelineConfig = {
  version: 1,
  planner: {
    primary: {
      provider: 'claude',
      model: 'claude-fable-5',
      effort: 'high',
    },
    fallback: {
      provider: 'codex',
      model: 'gpt-5.6-terra',
      effort: 'xhigh',
    },
  },
  reviewer: {
    enabled: true,
    primary: {
      provider: 'codex',
      model: 'gpt-5.6-sol',
      effort: 'xhigh',
    },
    fallback: {
      provider: 'claude',
      model: 'claude-sonnet-5',
      effort: 'high',
    },
  },
  max_revision_cycles: 2,
};
