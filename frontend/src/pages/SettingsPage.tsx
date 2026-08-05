import { useEffect, useState } from 'react';

import { api } from '../api/client';
import type { PlanPipelineConfig, SystemConfig } from '../api/client';
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

  useEffect(() => {
    Promise.all([api.config(), api.getPlanPipelineSettings()])
      .then(([config, persisted]) => {
        setSystemConfig(config);
        setPipeline(persisted);
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

  return (
    <div className="mx-auto max-w-4xl space-y-5">
      <div className="flex items-center gap-2">
        <Settings size={20} className="text-indigo-400" />
        <h2 className="text-lg font-semibold text-foreground">Settings</h2>
      </div>

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
