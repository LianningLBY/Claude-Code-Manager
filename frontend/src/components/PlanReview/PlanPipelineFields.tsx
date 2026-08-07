import { useState } from 'react';

import type {
  PlanModelRoute,
  PlanPipelineConfig,
  SystemConfig,
} from '../../api/client';

interface PlanPipelineFieldsProps {
  value: PlanPipelineConfig;
  onChange: (value: PlanPipelineConfig) => void;
  systemConfig: SystemConfig | null;
  compact?: boolean;
}

type StageName = 'planner' | 'reviewer';
type RouteSlot = 'primary' | 'fallback';

function routeLabel(stage: StageName, slot: RouteSlot): string {
  const stageLabel = stage === 'planner' ? 'Planner' : 'Reviewer';
  return `${stageLabel} ${slot === 'primary' ? 'primary' : 'fallback'}`;
}

export function PlanPipelineFields({
  value,
  onChange,
  systemConfig,
  compact = false,
}: PlanPipelineFieldsProps) {
  const [roundsInput, setRoundsInput] = useState(
    String(Math.max(1, value.max_revision_cycles)),
  );
  const [interactionsInput, setInteractionsInput] = useState(
    String(Math.max(0, value.max_interactions ?? 3)),
  );

  const updateRoute = (
    stage: StageName,
    slot: RouteSlot,
    route: PlanModelRoute,
  ) => {
    onChange({
      ...value,
      [stage]: {
        ...value[stage],
        [slot]: route,
      },
    });
  };

  const routeFields = (stage: StageName, slot: RouteSlot) => {
    const route = value[stage][slot];
    const models = route.provider === 'codex'
      ? systemConfig?.codex_model_options || [route.model]
      : systemConfig?.model_options || [route.model];
    const uniqueModels = [...new Set([route.model, ...models])]
      .filter((model) => model && model !== 'default');
    const efforts = route.provider === 'codex'
      ? (
        systemConfig?.codex_model_efforts?.[route.model]
        || systemConfig?.codex_effort_options
        || []
      )
      : (
        systemConfig?.claude_model_efforts?.[route.model]
        || systemConfig?.effort_options
        || []
      );
    const uniqueEfforts = [...new Set([
      ...(route.effort ? [route.effort] : []),
      ...efforts,
    ])];
    const label = routeLabel(stage, slot);

    return (
      <div className={`grid gap-2 ${
        compact
          ? 'grid-cols-1 sm:grid-cols-3'
          : 'grid-cols-1 sm:grid-cols-[8rem_1fr_7rem]'
      }`}>
        <select
          aria-label={`${label} provider`}
          className="rounded border border-gray-600 bg-gray-700 px-2 py-1 text-xs text-gray-100"
          value={route.provider}
          onChange={(event) => {
            const provider = event.target.value as 'claude' | 'codex';
            updateRoute(stage, slot, {
              provider,
              model: provider === 'codex'
                ? systemConfig?.default_codex_model || 'gpt-5.6-sol'
                : systemConfig?.default_model || 'claude-fable-5',
              effort: systemConfig?.default_effort || 'medium',
            });
          }}
        >
          <option value="claude">Claude</option>
          <option value="codex">Codex</option>
        </select>
        <select
          aria-label={`${label} model`}
          className="min-w-0 rounded border border-gray-600 bg-gray-700 px-2 py-1 text-xs text-gray-100"
          value={route.model}
          onChange={(event) => updateRoute(stage, slot, {
            ...route,
            model: event.target.value,
          })}
        >
          {uniqueModels.map((model) => (
            <option key={model} value={model}>{model}</option>
          ))}
        </select>
        <select
          aria-label={`${label} effort`}
          className="rounded border border-gray-600 bg-gray-700 px-2 py-1 text-xs text-gray-100"
          value={route.effort || ''}
          onChange={(event) => updateRoute(stage, slot, {
            ...route,
            effort: event.target.value || null,
          })}
        >
          <option value="">default</option>
          {uniqueEfforts.map((effort) => (
            <option key={effort} value={effort}>{effort}</option>
          ))}
        </select>
      </div>
    );
  };

  return (
    <div className="space-y-3 rounded-lg border border-gray-700 bg-gray-900/50 p-3">
      <div>
        <div className="mb-1.5 text-[11px] font-medium uppercase tracking-wide text-indigo-300">
          Planner
        </div>
        <div className="space-y-1.5">
          <div>
            <div className="mb-1 text-[10px] text-gray-500">Primary</div>
            {routeFields('planner', 'primary')}
          </div>
          <div>
            <div className="mb-1 text-[10px] text-gray-500">Fallback</div>
            {routeFields('planner', 'fallback')}
          </div>
        </div>
      </div>

      <div className="border-t border-gray-700 pt-3">
        <div className="mb-1.5 flex items-center justify-between">
          <div className="text-[11px] font-medium uppercase tracking-wide text-purple-300">
            Reviewer
          </div>
          <label className="flex items-center gap-1.5 text-[10px] text-gray-400">
            <input
              aria-label="Enable Reviewer"
              type="checkbox"
              checked={value.reviewer.enabled}
              onChange={(event) => onChange({
                ...value,
                reviewer: {
                  ...value.reviewer,
                  enabled: event.target.checked,
                },
              })}
            />
            Enabled
          </label>
        </div>
        {value.reviewer.enabled && (
          <div className="space-y-1.5">
            <div>
              <div className="mb-1 text-[10px] text-gray-500">Primary</div>
              {routeFields('reviewer', 'primary')}
            </div>
            <div>
              <div className="mb-1 text-[10px] text-gray-500">Fallback</div>
              {routeFields('reviewer', 'fallback')}
            </div>
          </div>
        )}
      </div>

      <label className="flex items-center justify-between border-t border-gray-700 pt-2 text-[11px] text-gray-400">
        Maximum rounds
        <input
          aria-label="Plan maximum rounds"
          type="number"
          min={1}
          max={10}
          value={roundsInput}
          onChange={(event) => {
            const next = event.target.value;
            setRoundsInput(next);
            const parsed = Number(next);
            if (Number.isInteger(parsed) && parsed >= 1 && parsed <= 10) {
              onChange({ ...value, max_revision_cycles: parsed });
            }
          }}
          onBlur={() => setRoundsInput(
            String(Math.max(1, Math.min(10, value.max_revision_cycles))),
          )}
          className="w-16 rounded border border-gray-600 bg-gray-700 px-2 py-1 text-xs text-gray-100"
        />
      </label>
      <label className="flex items-center justify-between text-[11px] text-gray-400">
        User-input pauses per Run
        <input
          aria-label="Plan maximum user-input pauses"
          type="number"
          min={0}
          max={5}
          value={interactionsInput}
          onChange={(event) => {
            const next = event.target.value;
            setInteractionsInput(next);
            const parsed = Number(next);
            if (Number.isInteger(parsed) && parsed >= 0 && parsed <= 5) {
              onChange({ ...value, max_interactions: parsed });
            }
          }}
          onBlur={() => setInteractionsInput(
            String(Math.max(0, Math.min(5, value.max_interactions ?? 3))),
          )}
          className="w-16 rounded border border-gray-600 bg-gray-700 px-2 py-1 text-xs text-gray-100"
        />
      </label>
    </div>
  );
}
