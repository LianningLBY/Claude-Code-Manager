import { useCallback, useEffect, useMemo, useState } from 'react';

import { api, isApiRequestError, type PlanResource, type PlanRun, type PlanVersion } from '../../api/client';
import { Check, ChevronRight, GitBranch, Loader2, Play, RefreshCw, X } from '../icons';
import { MarkdownContent } from '../MarkdownContent';
import { CollapsiblePlanningRequest } from './CollapsiblePlanningRequest';
import { PlanRunInputAudit } from './PlanRunInputAudit';

export function VersionedPlanPanel() {
  const [plans, setPlans] = useState<PlanResource[]>([]);
  const [selectedId, setSelectedId] = useState<number | null>(null);
  const [versions, setVersions] = useState<PlanVersion[]>([]);
  const [runs, setRuns] = useState<PlanRun[]>([]);
  const [versionId, setVersionId] = useState<number | null>(null);
  const [revision, setRevision] = useState('');
  const [compare, setCompare] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    try {
      const rows = (await api.listPlans()).filter((plan) => !plan.legacy && (
        plan.display_state === 'awaiting_review'
        || (plan.display_state === 'approved' && plan.target_task_id == null)
      ));
      setPlans(rows);
      setError(null);
      if (selectedId != null && !rows.some((plan) => plan.id === selectedId)) setSelectedId(null);
    } catch (fetchError) {
      setError(fetchError instanceof Error ? fetchError.message : String(fetchError));
    }
  }, [selectedId]);

  useEffect(() => {
    void refresh();
    const timer = window.setInterval(() => void refresh(), 5000);
    return () => window.clearInterval(timer);
  }, [refresh]);

  const selected = plans.find((plan) => plan.id === selectedId) || null;
  useEffect(() => {
    if (!selected) { setVersions([]); setRuns([]); return; }
    void Promise.all([
      api.listPlanVersions(selected.id),
      api.listPlanResourceRuns(selected.id),
    ]).then(([rows, runRows]) => {
      setVersions(rows);
      setRuns(runRows);
      setVersionId(selected.current_version_id);
    }).catch((fetchError) => setError(fetchError instanceof Error ? fetchError.message : String(fetchError)));
  }, [selected]);

  const shown = versions.find((item) => item.id === versionId) || selected?.current_version || null;
  const previous = useMemo(() => {
    if (!shown) return null;
    return versions.find((item) => item.version_number === shown.version_number - 1) || null;
  }, [shown, versions]);

  const mutate = async (operation: () => Promise<unknown>) => {
    setBusy(true);
    setError(null);
    try {
      await operation();
      await refresh();
      if (selectedId != null) {
        const detail = await api.getPlan(selectedId);
        if (detail.display_state !== 'awaiting_review' && !(detail.display_state === 'approved' && detail.target_task_id == null)) setSelectedId(null);
      }
    } catch (mutationError) {
      setError(mutationError instanceof Error ? mutationError.message : String(mutationError));
    } finally {
      setBusy(false);
    }
  };

  const decide = async (decision: 'approve' | 'reject') => {
    if (!selected?.current_version) return;
    const execute = (confirmStale: boolean) => decision === 'approve'
      ? api.approvePlanVersion(selected.current_version!.id, selected.current_version!.id, confirmStale)
      : api.rejectPlanVersion(selected.current_version!.id, selected.current_version!.id, confirmStale);
    await mutate(async () => {
      try { await execute(false); }
      catch (decisionError) {
        if (!isApiRequestError(decisionError) || decisionError.status !== 409 || !window.confirm('This Version is based on older context. Continue anyway?')) throw decisionError;
        await execute(true);
      }
    });
  };

  if (plans.length === 0 && !error) return null;
  return (
    <section className="space-y-3" aria-label="Versioned Plans awaiting review">
      <div className="flex items-center gap-2">
        <h2 className="font-semibold text-foreground">Plans Awaiting Review</h2>
        <span className="rounded-full bg-indigo-600 px-2 py-0.5 text-xs font-bold text-white">{plans.length}</span>
      </div>
      {error && <div className="rounded-lg border border-red-500/30 bg-red-500/10 px-3 py-2 text-xs text-red-400">{error}</div>}
      {plans.map((plan) => (
        <article key={plan.id} className="flex items-center gap-3 rounded-xl border border-gray-700/70 bg-gray-800 px-4 py-3.5">
          <div className="min-w-0 flex-1">
            <div className="flex flex-wrap items-center gap-2">
              <h3 className="truncate text-sm font-semibold text-gray-100">#{plan.id} {plan.title}</h3>
              {plan.current_version && <span className="rounded-full bg-indigo-500/15 px-2 py-0.5 text-[10px] font-semibold text-indigo-300">v{plan.current_version.version_number}</span>}
              <span className="rounded-full border border-gray-600 px-2 py-0.5 text-[10px] text-gray-400">{plan.target_task_id ? `Task #${plan.target_task_id}` : 'Standalone'}</span>
            </div>
          </div>
          <button type="button" onClick={() => setSelectedId(plan.id)} className="flex items-center gap-1 rounded-lg bg-indigo-600 px-3 py-2 text-xs font-medium text-white hover:bg-indigo-500">Review <ChevronRight size={13} /></button>
        </article>
      ))}

      {selected && shown && (
        <div className="fixed inset-0 z-[80] flex items-end justify-center bg-black/65 sm:items-center sm:p-5" onMouseDown={(event) => event.target === event.currentTarget && !busy && setSelectedId(null)}>
          <div role="dialog" aria-modal="true" className="flex h-[100dvh] w-full flex-col overflow-hidden border border-gray-700 bg-gray-900 shadow-2xl sm:h-[min(88vh,860px)] sm:max-w-5xl sm:rounded-2xl">
            <header className="flex items-center gap-3 border-b border-gray-800 px-4 py-3">
              <div className="min-w-0 flex-1">
                <div className="truncate text-sm font-semibold text-gray-100">Plan #{selected.id} · {selected.title}</div>
                <div className="mt-0.5 text-xs text-gray-500">Stable Plan · {versions.length} immutable {versions.length === 1 ? 'Version' : 'Versions'}</div>
              </div>
              <button type="button" onClick={() => setSelectedId(null)} disabled={busy} className="rounded-lg p-1.5 text-gray-500 hover:bg-gray-800"><X size={16} /></button>
            </header>
            <div className="min-h-0 flex-1 overflow-y-auto px-4 py-4 sm:px-6">
              {selected.latest_run_error && <div className="mb-4 rounded-lg border border-amber-500/30 bg-amber-500/10 px-3 py-2 text-xs text-amber-300">Latest Run {selected.latest_run_status}: {selected.latest_run_error}</div>}
              <CollapsiblePlanningRequest content={selected.initial_request} />
              {shown.id !== selected.current_version_id && <div className="mt-4 flex items-center justify-between gap-3 rounded-lg border border-gray-700 bg-gray-800/60 px-3 py-2 text-xs text-gray-400"><span>Historical Version · read only{shown.superseded_by_version_id ? ' · superseded by a newer Version' : ''}</span><div className="flex shrink-0 gap-3"><button type="button" onClick={() => void mutate(() => api.forkPlan(selected.id, { base_version_id: shown.id }))} className="text-indigo-300">Fork</button><button type="button" onClick={() => setVersionId(selected.current_version_id)} className="text-indigo-300">View current</button></div></div>}
              <div className="mt-4 flex flex-wrap items-center gap-2">
                <label className="text-xs text-gray-500" htmlFor="plan-version-select">Version</label>
                <select id="plan-version-select" value={shown.id} onChange={(event) => setVersionId(Number(event.target.value))} className="rounded-lg border border-gray-700 bg-gray-800 px-2 py-1.5 text-xs text-gray-200">
                  {versions.map((version) => <option key={version.id} value={version.id}>v{version.version_number} · {version.human_decision}{version.applied ? ' · applied' : ''}</option>)}
                </select>
                {previous && <button type="button" onClick={() => setCompare((value) => !value)} className="rounded-lg border border-gray-700 px-2.5 py-1.5 text-xs text-gray-300 hover:bg-gray-800">{compare ? 'Hide comparison' : `Compare with v${previous.version_number}`}</button>}
              </div>
              <div className={`mt-4 grid gap-4 ${compare && previous ? 'lg:grid-cols-2' : ''}`}>
                {compare && previous && <div className="rounded-xl border border-gray-700 bg-gray-950/60 p-4"><div className="mb-3 text-xs font-semibold text-gray-500">v{previous.version_number}</div><MarkdownContent content={previous.content} /></div>}
                <div className="rounded-xl border border-gray-700 bg-gray-950/60 p-4"><div className="mb-3 text-xs font-semibold text-indigo-300">v{shown.version_number}{shown.id === selected.current_version_id ? ' · current' : ''}</div><MarkdownContent content={shown.content} /></div>
              </div>
              {shown.review_feedback && <div className="mt-4 rounded-xl border border-gray-700 bg-gray-800/60 p-3 text-sm text-gray-300"><div className="mb-1 text-xs font-semibold text-gray-500">Reviewer feedback</div>{shown.review_feedback}</div>}
              <PlanRunInputAudit runs={runs} version={shown} />
              {shown.id === selected.current_version_id && selected.display_state === 'awaiting_review' && (
                <div className="mt-5 space-y-3 border-t border-gray-800 pt-4">
                  <textarea value={revision} onChange={(event) => setRevision(event.target.value)} rows={3} maxLength={50000} placeholder="Request changes to this Version…" className="w-full resize-y rounded-lg border border-gray-700 bg-gray-800 px-3 py-2 text-sm text-gray-100 outline-none focus:border-indigo-500" />
                  <div className="flex flex-wrap items-center gap-2">
                    <button type="button" disabled={busy} onClick={() => void decide('approve')} className="flex items-center gap-1.5 rounded-lg bg-emerald-600 px-3 py-2 text-xs font-semibold text-white disabled:opacity-40"><Check size={13} /> Approve v{shown.version_number}</button>
                    <button type="button" disabled={busy} onClick={() => void decide('reject')} className="rounded-lg border border-red-500/40 px-3 py-2 text-xs text-red-300 disabled:opacity-40">Reject v{shown.version_number}</button>
                    <button type="button" disabled={busy || !revision.trim()} onClick={() => void mutate(async () => { await api.createPlanRun(selected.id, { run_type: 'user_revision', request: revision.trim(), base_version_id: shown.id, expected_current_version_id: shown.id }); setRevision(''); })} className="rounded-lg border border-indigo-500/40 px-3 py-2 text-xs text-indigo-300 disabled:opacity-40">Revise</button>
                    <button type="button" disabled={busy} onClick={() => void mutate(() => api.createPlanRun(selected.id, { run_type: 'refresh_context', request: 'Refresh the Plan using the latest task context and repository state.', base_version_id: shown.id, expected_current_version_id: shown.id }))} className="flex items-center gap-1.5 rounded-lg border border-gray-700 px-3 py-2 text-xs text-gray-300 disabled:opacity-40"><RefreshCw size={12} /> Refresh context</button>
                    <button type="button" disabled={busy} onClick={() => void mutate(() => api.forkPlan(selected.id, { base_version_id: shown.id }))} className="flex items-center gap-1.5 rounded-lg border border-gray-700 px-3 py-2 text-xs text-gray-300 disabled:opacity-40"><GitBranch size={12} /> Fork as new Plan</button>
                    {busy && <Loader2 size={14} className="animate-spin text-gray-500" />}
                  </div>
                </div>
              )}
              {selected.display_state === 'approved' && selected.target_task_id == null && (
                <button type="button" disabled={busy} onClick={() => void mutate(async () => { const result = await api.createVersionExecutionTask(shown.id); window.location.hash = `#/tasks/chat/${result.execution_task_id}`; })} className="mt-5 flex items-center gap-1.5 rounded-lg bg-indigo-600 px-4 py-2 text-xs font-semibold text-white disabled:opacity-40"><Play size={13} /> Create execution Task from v{shown.version_number}</button>
              )}
            </div>
          </div>
        </div>
      )}
    </section>
  );
}
