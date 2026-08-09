import { useEffect, useState } from 'react';

import { api } from '../api/client';
import type { CapacitySettings, PlanPipelineConfig, SystemConfig } from '../api/client';
import { Check, Loader2, Settings } from '../components/icons';
import { PlanPipelineFields } from '../components/PlanReview/PlanPipelineFields';
import { FALLBACK_PLAN_PIPELINE_CONFIG } from '../components/PlanReview/planPipelineDefaults';


export function SettingsPage() {
  const [systemConfig, setSystemConfig] = useState<SystemConfig | null>(null);
  const [pipeline, setPipeline] = useState<PlanPipelineConfig>(
    FALLBACK_PLAN_PIPELINE_CONFIG,
  );
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [saved, setSaved] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [capacity, setCapacity] = useState<CapacitySettings | null>(null);
  const [capacityInput, setCapacityInput] = useState('');
  const [capacitySaving, setCapacitySaving] = useState(false);
  const [capacitySaved, setCapacitySaved] = useState(false);
  const [capacityError, setCapacityError] = useState<string | null>(null);

  useEffect(() => {
    Promise.all([
      api.config(),
      api.getPlanPipelineSettings(),
      api.getCapacitySettings(),
    ])
      .then(([config, persisted, currentCapacity]) => {
        setSystemConfig(config);
        setPipeline(persisted);
        setCapacity(currentCapacity);
        setCapacityInput(String(currentCapacity.max_concurrent_instances));
      })
      .catch((reason) => {
        setError(reason instanceof Error ? reason.message : String(reason));
      })
      .finally(() => setLoading(false));
  }, []);

  const save = async () => {
    setSaving(true);
    setSaved(false);
    setError(null);
    try {
      const persisted = await api.updatePlanPipelineSettings(pipeline);
      setPipeline(persisted);
      setSaved(true);
      window.setTimeout(() => setSaved(false), 2000);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : String(reason));
    } finally {
      setSaving(false);
    }
  };

  const saveCapacity = async (restoreDefault = false) => {
    const parsed = Number(capacityInput);
    if (!restoreDefault && (!Number.isInteger(parsed) || parsed < 1 || parsed > 64)) {
      setCapacityError('Concurrency must be a whole number between 1 and 64.');
      return;
    }
    if (
      !restoreDefault
      && capacity
      && parsed < capacity.active_instances
      && !window.confirm(
        `There are ${capacity.active_instances} active tasks. They will keep running, and new work will wait until usage falls below ${parsed}. Continue?`,
      )
    ) return;

    setCapacitySaving(true);
    setCapacitySaved(false);
    setCapacityError(null);
    try {
      const updated = await api.updateCapacitySettings(restoreDefault ? null : parsed);
      setCapacity(updated);
      setCapacityInput(String(updated.max_concurrent_instances));
      setCapacitySaved(true);
      window.setTimeout(() => setCapacitySaved(false), 2000);
    } catch (reason) {
      setCapacityError(reason instanceof Error ? reason.message : String(reason));
    } finally {
      setCapacitySaving(false);
    }
  };

  return (
    <div className="mx-auto max-w-4xl space-y-5">
      <div className="flex items-center gap-2">
        <Settings size={20} className="text-indigo-400" />
        <h2 className="text-lg font-semibold text-foreground">Settings</h2>
      </div>

      <section className="rounded-xl border border-gray-800 bg-gray-900/70 p-5 shadow-sm">
        <div className="mb-4">
          <h3 className="text-sm font-semibold text-gray-100">Local task capacity</h3>
          <p className="mt-1 text-xs leading-5 text-gray-500">
            Limits concurrent Tasks and Plans on this Manager. Changes apply immediately
            without interrupting work already running. Remote Workers are configured separately.
          </p>
        </div>

        {loading || !capacity ? (
          <div className="flex items-center gap-2 py-6 text-sm text-gray-500">
            <Loader2 size={15} className="animate-spin" />
            Loading capacity…
          </div>
        ) : (
          <>
            <div className="grid gap-3 sm:grid-cols-3">
              <label className="sm:col-span-1">
                <span className="mb-1 block text-xs font-medium text-gray-300">
                  Maximum concurrent tasks
                </span>
                <input
                  aria-label="Maximum concurrent tasks"
                  type="number"
                  min={1}
                  max={64}
                  step={1}
                  value={capacityInput}
                  onChange={(event) => setCapacityInput(event.target.value)}
                  className="w-full rounded-lg border border-gray-700 bg-gray-950 px-3 py-2 text-sm text-gray-100 outline-none focus:border-indigo-500"
                />
              </label>
              <div className="rounded-lg border border-gray-800 bg-gray-950/60 px-3 py-2">
                <p className="text-[11px] uppercase tracking-wide text-gray-500">Active now</p>
                <p className="mt-1 text-lg font-semibold text-gray-100">{capacity.active_instances}</p>
              </div>
              <div className="rounded-lg border border-gray-800 bg-gray-950/60 px-3 py-2">
                <p className="text-[11px] uppercase tracking-wide text-gray-500">Waiting</p>
                <p className="mt-1 text-lg font-semibold text-gray-100">{capacity.pending_tasks}</p>
              </div>
            </div>
            <p className="mt-2 text-xs text-gray-500">
              Environment default: {capacity.env_default} · Minimum idle slots: {capacity.min_idle_instances}
              {capacity.configured_override !== null ? ' · Runtime override active' : ' · Following environment default'}
            </p>
          </>
        )}

        {capacityError && (
          <p className="mt-3 rounded border border-red-500/30 bg-red-500/10 px-3 py-2 text-xs text-red-400">
            {capacityError}
          </p>
        )}

        <div className="mt-4 flex justify-end gap-2">
          <button
            type="button"
            onClick={() => void saveCapacity(true)}
            disabled={loading || capacitySaving || !capacity || capacity.configured_override === null}
            className="rounded-lg border border-gray-700 px-3 py-2 text-sm text-gray-300 hover:border-gray-600 hover:text-white disabled:opacity-40"
          >
            Restore environment default
          </button>
          <button
            type="button"
            onClick={() => void saveCapacity(false)}
            disabled={loading || capacitySaving || !capacity}
            className="inline-flex items-center gap-1.5 rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-500 disabled:opacity-50"
          >
            {capacitySaving
              ? <Loader2 size={14} className="animate-spin" />
              : capacitySaved ? <Check size={14} /> : null}
            {capacitySaving ? 'Saving…' : capacitySaved ? 'Saved' : 'Save capacity'}
          </button>
        </div>
      </section>

      <section className="rounded-xl border border-gray-800 bg-gray-900/70 p-5 shadow-sm">
        <div className="mb-4">
          <h3 className="text-sm font-semibold text-gray-100">Plan Pipeline</h3>
          <p className="mt-1 text-xs leading-5 text-gray-500">
            These routes are snapshotted when a new Plan is created. Existing
            Plans keep the configuration they started with.
          </p>
        </div>

        {loading ? (
          <div className="flex items-center gap-2 py-8 text-sm text-gray-500">
            <Loader2 size={15} className="animate-spin" />
            Loading settings…
          </div>
        ) : (
          <PlanPipelineFields
            value={pipeline}
            onChange={setPipeline}
            systemConfig={systemConfig}
          />
        )}

        {error && (
          <p className="mt-3 rounded border border-red-500/30 bg-red-500/10 px-3 py-2 text-xs text-red-400">
            {error}
          </p>
        )}

        <div className="mt-4 flex justify-end">
          <button
            type="button"
            onClick={() => void save()}
            disabled={loading || saving}
            className="inline-flex items-center gap-1.5 rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-500 disabled:opacity-50"
          >
            {saving
              ? <Loader2 size={14} className="animate-spin" />
              : saved ? <Check size={14} /> : null}
            {saving ? 'Saving…' : saved ? 'Saved' : 'Save settings'}
          </button>
        </div>
      </section>
    </div>
  );
}
