import { useEffect, useState } from 'react';

import { api, type PlanResource, type PlanVersion, type Project } from '../../api/client';
import { Archive, ArchiveRestore, ChevronRight, Loader2, X } from '../icons';
import { MarkdownContent } from '../MarkdownContent';
import { CollapsiblePlanningRequest } from './CollapsiblePlanningRequest';

interface Props {
  plans: PlanResource[];
  projects: Project[];
  onRefresh: () => void;
}

function stateLabel(value: string) {
  return value.replaceAll('_', ' ');
}

export function PlanCatalog({ plans, projects, onRefresh }: Props) {
  const [selectedId, setSelectedId] = useState<number | null>(null);
  const [versions, setVersions] = useState<PlanVersion[]>([]);
  const [versionId, setVersionId] = useState<number | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const selected = plans.find((plan) => plan.id === selectedId) || null;

  useEffect(() => {
    if (!selected) {
      setVersions([]);
      setVersionId(null);
      return;
    }
    void api.listPlanVersions(selected.id).then((rows) => {
      setVersions(rows);
      setVersionId(selected.current_version_id);
    }).catch((reason) => setError(reason instanceof Error ? reason.message : String(reason)));
  }, [selected]);

  const shown = versions.find((version) => version.id === versionId) || selected?.current_version || null;
  const archive = async () => {
    if (!selected || selected.active_run_id != null) return;
    setBusy(true);
    setError(null);
    try {
      await api.updatePlan(selected.id, {
        archived: selected.archived_at == null,
        expected_lock_version: selected.lock_version,
      });
      setSelectedId(null);
      onRefresh();
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : String(reason));
    } finally {
      setBusy(false);
    }
  };

  if (plans.length === 0) {
    return <div className="rounded-xl border border-gray-800 bg-gray-900/50 px-4 py-10 text-center text-sm text-gray-500">No Plans match this filter.</div>;
  }

  return <>
    <div className="space-y-2">
      {plans.map((plan) => {
        const project = projects.find((item) => item.id === plan.project_id);
        return <button key={plan.id} type="button" onClick={() => setSelectedId(plan.id)} className="flex w-full items-center gap-3 rounded-xl border border-gray-800 bg-gray-900/70 px-4 py-3 text-left hover:border-gray-700 hover:bg-gray-800/70">
          <div className="min-w-0 flex-1">
            <div className="flex flex-wrap items-center gap-2">
              <span className="truncate text-sm font-semibold text-gray-100">#{plan.id} {plan.title}</span>
              {plan.current_version && <span className="rounded-full bg-indigo-500/15 px-2 py-0.5 text-[10px] text-indigo-300">v{plan.current_version.version_number}</span>}
              <span className="rounded-full border border-gray-700 px-2 py-0.5 text-[10px] capitalize text-gray-400">{stateLabel(plan.display_state)}</span>
              {plan.archived_at && <span className="rounded-full bg-amber-500/15 px-2 py-0.5 text-[10px] text-amber-300">Archived</span>}
            </div>
            <div className="mt-1 truncate text-xs text-gray-500">{plan.target_task_id != null ? `Related to Task #${plan.target_task_id}` : 'Standalone Plan'}{project ? ` · ${project.name}` : ''}</div>
          </div>
          <ChevronRight size={15} className="shrink-0 text-gray-600" />
        </button>;
      })}
    </div>

    {selected && <div className="fixed inset-0 z-[80] flex items-end justify-center bg-black/65 sm:items-center sm:p-5" onMouseDown={(event) => event.target === event.currentTarget && !busy && setSelectedId(null)}>
      <div role="dialog" aria-modal="true" aria-label={`Plan #${selected.id}`} className="flex h-[100dvh] w-full flex-col overflow-hidden border border-gray-700 bg-gray-900 shadow-2xl sm:h-[min(86vh,820px)] sm:max-w-4xl sm:rounded-2xl">
        <header className="flex items-center gap-3 border-b border-gray-800 px-4 py-3">
          <div className="min-w-0 flex-1"><div className="truncate text-sm font-semibold text-gray-100">Plan #{selected.id} · {selected.title}</div><div className="mt-0.5 text-xs capitalize text-gray-500">{stateLabel(selected.display_state)} · {selected.target_task_id != null ? `Task #${selected.target_task_id}` : 'Standalone'}</div></div>
          {selected.target_task_id != null && <button type="button" onClick={() => { window.location.hash = `#/tasks/chat/${selected.target_task_id}`; }} className="rounded-lg border border-gray-700 px-2.5 py-1.5 text-xs text-gray-300 hover:bg-gray-800">Open Task</button>}
          <button type="button" onClick={() => setSelectedId(null)} disabled={busy} className="rounded-lg p-1.5 text-gray-500 hover:bg-gray-800"><X size={16} /></button>
        </header>
        <div className="min-h-0 flex-1 overflow-y-auto p-4 sm:p-6">
          {error && <div className="mb-4 rounded-lg border border-red-500/30 bg-red-500/10 px-3 py-2 text-xs text-red-400">{error}</div>}
          <CollapsiblePlanningRequest content={selected.initial_request} />
          {shown ? <>
            <div className="mt-4 flex items-center gap-2"><span className="text-xs text-gray-500">Version</span><select value={shown.id} onChange={(event) => setVersionId(Number(event.target.value))} className="rounded-lg border border-gray-700 bg-gray-800 px-2 py-1.5 text-xs text-gray-200">{versions.map((version) => <option key={version.id} value={version.id}>v{version.version_number} · {version.human_decision}{version.applied ? ' · applied' : ''}</option>)}</select></div>
            <div className="mt-3 rounded-xl border border-gray-700 bg-gray-950/60 p-4"><MarkdownContent content={shown.content} /></div>
          </> : <div className="mt-4 rounded-xl border border-gray-800 bg-gray-950/40 px-4 py-8 text-center text-sm text-gray-500">{selected.active_run ? `Run #${selected.active_run.id} is ${stateLabel(selected.active_run.status)}.` : 'This Plan has no Version yet.'}</div>}
        </div>
        <footer className="flex justify-end border-t border-gray-800 px-4 py-3">
          <button type="button" disabled={busy || selected.active_run_id != null} onClick={() => void archive()} className="flex items-center gap-1.5 rounded-lg border border-gray-700 px-3 py-2 text-xs text-gray-300 disabled:opacity-40">{busy ? <Loader2 size={13} className="animate-spin" /> : selected.archived_at ? <ArchiveRestore size={13} /> : <Archive size={13} />}{selected.archived_at ? 'Restore Plan' : 'Archive Plan'}</button>
        </footer>
      </div>
    </div>}
  </>;
}
