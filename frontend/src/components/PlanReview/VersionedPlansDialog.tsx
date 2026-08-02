import { useCallback, useEffect, useMemo, useRef, useState } from 'react';

import { api, isApiRequestError, type PlanResource, type PlanRun, type PlanVersion, type UploadResult } from '../../api/client';
import { useFileUpload } from '../../hooks/useFileUpload';
import { Archive, Check, ChevronLeft, ChevronRight, GitBranch, ListTodo, Loader2, Paperclip, RefreshCw, X } from '../icons';
import { MarkdownContent } from '../MarkdownContent';
import { CollapsiblePlanningRequest } from './CollapsiblePlanningRequest';
import { PlanInputForm } from './PlanInputForm';
import { PlanRunInputAudit } from './PlanRunInputAudit';

type Filter = 'all' | 'input' | 'review' | 'running' | 'approved';

interface Props {
  open: boolean;
  taskId: number;
  refreshGeneration?: number;
  selectedVersionIds: number[];
  onToggleVersion: (versionId: number) => void;
  onPlansChange: (plans: PlanResource[]) => void;
  onClose: () => void;
}

const RUNNING = new Set(['planner', 'reviewer', 'queued', 'running']);

function statusLabel(plan: PlanResource) {
  if (plan.display_state === 'waiting_user') return 'Needs input';
  if (plan.display_state === 'awaiting_review') return 'Awaiting review';
  if (plan.display_state === 'approved') return 'Approved';
  if (plan.display_state === 'applied') return 'Applied';
  if (plan.display_state === 'rejected') return 'Rejected';
  if (plan.display_state === 'failed') return 'Failed';
  if (RUNNING.has(plan.display_state)) return plan.display_state === 'reviewer' ? 'Reviewing' : 'Planning';
  return plan.display_state;
}

function filterPlan(plan: PlanResource, filter: Filter) {
  if (filter === 'input') return plan.display_state === 'waiting_user';
  if (filter === 'review') return plan.display_state === 'awaiting_review';
  if (filter === 'running') return RUNNING.has(plan.display_state);
  if (filter === 'approved') return ['approved', 'applied'].includes(plan.display_state);
  return true;
}

function uploadPayload(results: UploadResult[]) {
  return results.length ? {
    file_paths: results.map((item) => item.path),
    image_paths: results.filter((item) => item.is_image).map((item) => item.path),
    attachments: results.map((item) => ({
      url: item.url,
      name: item.filename || item.url.split('/').pop() || 'file',
      is_image: item.is_image,
    })),
  } : {};
}

export function VersionedPlansDialog({ open, taskId, refreshGeneration = 0, selectedVersionIds, onToggleVersion, onPlansChange, onClose }: Props) {
  const [plans, setPlans] = useState<PlanResource[]>([]);
  const [selectedId, setSelectedId] = useState<number | null>(null);
  const [versions, setVersions] = useState<PlanVersion[]>([]);
  const [runs, setRuns] = useState<PlanRun[]>([]);
  const [versionId, setVersionId] = useState<number | null>(null);
  const [filter, setFilter] = useState<Filter>('all');
  const [requestText, setRequestText] = useState('');
  const [revision, setRevision] = useState('');
  const [compare, setCompare] = useState(false);
  const [loading, setLoading] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const fileInput = useRef<HTMLInputElement>(null);
  const uploads = useFileUpload();

  const refresh = useCallback(async (showLoading = false) => {
    if (showLoading) setLoading(true);
    try {
      const rows = (await api.listPlans({ target_task_id: taskId })).filter((plan) => !plan.legacy);
      setPlans(rows);
      onPlansChange(rows);
      setError(null);
      if (selectedId != null && !rows.some((plan) => plan.id === selectedId)) setSelectedId(null);
    } catch (fetchError) {
      setError(fetchError instanceof Error ? fetchError.message : String(fetchError));
    } finally {
      if (showLoading) setLoading(false);
    }
  }, [onPlansChange, selectedId, taskId]);

  useEffect(() => {
    if (!open) return;
    void refresh(true);
    const timer = window.setInterval(() => void refresh(), 3500);
    return () => window.clearInterval(timer);
  }, [open, refresh]);

  useEffect(() => {
    if (open && refreshGeneration > 0) void refresh();
  }, [open, refresh, refreshGeneration]);

  const selected = plans.find((plan) => plan.id === selectedId) || plans.find((plan) => ['waiting_user', 'awaiting_review'].includes(plan.display_state)) || plans[0] || null;
  useEffect(() => {
    if (!selected) { setVersions([]); setRuns([]); return; }
    void Promise.all([
      api.listPlanVersions(selected.id),
      api.listPlanResourceRuns(selected.id),
    ]).then(([versionRows, runRows]) => {
      setVersions(versionRows);
      setRuns(runRows);
      setVersionId((current) => versionRows.some((item) => item.id === current) ? current : selected.current_version_id);
    }).catch(() => {});
  }, [selected]);
  const shown = versions.find((version) => version.id === versionId) || selected?.current_version || null;
  const previous = shown ? versions.find((version) => version.version_number === shown.version_number - 1) || null : null;
  const filtered = useMemo(() => plans.filter((plan) => filterPlan(plan, filter)), [filter, plans]);

  const perform = async (operation: () => Promise<unknown>) => {
    setBusy(true); setError(null);
    try { await operation(); await refresh(); }
    catch (operationError) { setError(operationError instanceof Error ? operationError.message : String(operationError)); }
    finally { setBusy(false); }
  };

  const create = async () => {
    if (!requestText.trim() || busy || uploads.isUploading || uploads.hasFailed) return;
    await perform(async () => {
      const created = await api.createPlan({ input: requestText.trim(), target_task_id: taskId, ...uploadPayload(uploads.uploadedResults) });
      setRequestText(''); uploads.clear(); setSelectedId(created.id);
    });
  };

  const decide = async (decision: 'approve' | 'reject', attach: boolean) => {
    if (!selected?.current_version) return;
    const version = selected.current_version;
    await perform(async () => {
      const invoke = (confirm: boolean) => decision === 'approve'
        ? api.approvePlanVersion(version.id, version.id, confirm)
        : api.rejectPlanVersion(version.id, version.id, confirm);
      try { await invoke(false); }
      catch (decisionError) {
        if (!isApiRequestError(decisionError) || decisionError.status !== 409 || !window.confirm('This Version uses older context. Continue anyway?')) throw decisionError;
        await invoke(true);
      }
      if (decision === 'approve' && attach) onToggleVersion(version.id);
    });
  };

  if (!open) return null;
  const filters: { id: Filter; label: string }[] = [
    { id: 'all', label: 'All' }, { id: 'input', label: 'Input' },
    { id: 'review', label: 'Review' }, { id: 'running', label: 'Running' },
    { id: 'approved', label: 'Approved' },
  ];
  return (
    <div className="fixed inset-0 z-[75] flex items-end justify-center bg-black/65 sm:items-center sm:p-5" onMouseDown={(event) => event.target === event.currentTarget && onClose()}>
      <div role="dialog" aria-modal="true" aria-label={`Plans for Task #${taskId}`} className="relative flex h-[100dvh] w-full overflow-hidden border border-gray-700 bg-gray-900 shadow-2xl sm:h-[min(86vh,820px)] sm:max-w-6xl sm:rounded-2xl">
        <button type="button" onClick={onClose} className="absolute right-3 top-3 z-20 rounded-lg bg-gray-900/90 p-1.5 text-gray-500 hover:bg-gray-800" aria-label="Close Plans"><X size={16} /></button>
        <section className={`${selectedId == null ? 'flex' : 'hidden'} w-full flex-col border-gray-800 sm:flex sm:w-80 sm:shrink-0 sm:border-r`}>
          <div className="border-b border-gray-800 p-4 pr-12">
            <div className="flex items-center gap-2 text-sm font-semibold text-gray-100"><ListTodo size={16} className="text-indigo-300" /> Plans <span className="text-xs font-normal text-gray-500">Task #{taskId}</span></div>
            <form className="mt-3 space-y-2" onSubmit={(event) => { event.preventDefault(); void create(); }}>
              <textarea value={requestText} onChange={(event) => setRequestText(event.target.value)} rows={4} maxLength={200000} placeholder="Create an independent Plan…" className="w-full resize-none rounded-lg border border-gray-700 bg-gray-800 px-3 py-2 text-sm text-gray-100 outline-none focus:border-indigo-500" />
              {uploads.uploads.length > 0 && <div className="flex flex-wrap gap-1.5">{uploads.uploads.map((item) => <span key={item.id} className="flex items-center gap-1 rounded border border-gray-700 px-2 py-1 text-[10px] text-gray-400"><span className="max-w-32 truncate">{item.file.name}</span>{item.status === 'uploading' && <Loader2 size={10} className="animate-spin" />}{item.status === 'failed' && <button type="button" onClick={() => uploads.retryFile(item.id)} className="text-red-300">Retry</button>}<button type="button" onClick={() => uploads.removeFile(item.id)}><X size={10} /></button></span>)}</div>}
              <div className="flex items-center justify-between">
                <input ref={fileInput} type="file" multiple className="hidden" onChange={(event) => { uploads.addFiles(Array.from(event.target.files || []), setError); event.target.value = ''; }} />
                <button type="button" onClick={() => fileInput.current?.click()} className="rounded-lg border border-gray-700 p-2 text-gray-400"><Paperclip size={13} /></button>
                <button type="submit" disabled={!requestText.trim() || busy || uploads.isUploading || uploads.hasFailed} className="rounded-lg bg-indigo-600 px-3 py-2 text-xs font-semibold text-white disabled:opacity-40">Create Plan</button>
              </div>
            </form>
          </div>
          <div className="flex gap-1 overflow-x-auto border-b border-gray-800 px-3 py-2">{filters.map((item) => <button key={item.id} type="button" onClick={() => setFilter(item.id)} className={`rounded-full px-2 py-1 text-[10px] ${filter === item.id ? 'bg-indigo-500/20 text-indigo-300' : 'text-gray-500 hover:bg-gray-800'}`}>{item.label} {plans.filter((plan) => filterPlan(plan, item.id)).length}</button>)}</div>
          <div className="min-h-0 flex-1 space-y-2 overflow-y-auto p-3">{loading && <div className="flex justify-center p-6"><Loader2 className="animate-spin text-gray-500" /></div>}{filtered.map((plan) => <button key={plan.id} type="button" onClick={() => setSelectedId(plan.id)} className={`w-full rounded-xl border p-3 text-left ${selected?.id === plan.id ? 'border-indigo-500/60 bg-indigo-500/10' : 'border-gray-700 bg-gray-800/60 hover:border-gray-600'}`}><div className="flex items-start gap-2"><div className="min-w-0 flex-1"><div className="flex flex-wrap gap-1"><span className="rounded-full bg-gray-700 px-2 py-0.5 text-[10px] text-gray-300">{statusLabel(plan)}</span>{plan.current_version && <span className="rounded-full bg-indigo-500/15 px-2 py-0.5 text-[10px] text-indigo-300">v{plan.current_version.version_number}</span>}{plan.current_version && selectedVersionIds.includes(plan.current_version.id) && <span className="rounded-full bg-teal-500/15 px-2 py-0.5 text-[10px] text-teal-300">Attached</span>}</div><div className="mt-2 truncate text-sm font-medium text-gray-100">#{plan.id} {plan.title}</div><div className="mt-1 line-clamp-2 text-[11px] leading-4 text-gray-500">{plan.initial_request}</div></div><ChevronRight size={14} className="mt-1 text-gray-600" /></div></button>)}</div>
        </section>
        <section className={`${selectedId != null ? 'flex' : 'hidden'} min-w-0 flex-1 flex-col sm:flex`}>
          {!selected ? <div className="m-auto text-sm text-gray-500">Select or create a Plan</div> : <>
            <header className="flex items-center gap-3 border-b border-gray-800 px-4 py-3 pr-12"><button type="button" onClick={() => setSelectedId(null)} className="text-gray-500 sm:hidden"><ChevronLeft size={17} /></button><div className="min-w-0 flex-1"><div className="truncate text-sm font-semibold text-gray-100">Plan #{selected.id} · {selected.title}</div><div className="mt-0.5 text-xs text-gray-500">{statusLabel(selected)}{selected.active_run ? ` · Run #${selected.active_run.id} · round ${selected.active_run.round}` : ''}</div></div></header>
            <div className="min-h-0 flex-1 overflow-y-auto p-4 sm:p-6">
              {error && <div className="mb-4 rounded-lg border border-red-500/30 bg-red-500/10 px-3 py-2 text-xs text-red-400">{error}</div>}
              {selected.latest_run_error && <div className="mb-4 rounded-lg border border-amber-500/30 bg-amber-500/10 px-3 py-2 text-xs text-amber-300">Latest Run {selected.latest_run_status}: {selected.latest_run_error}</div>}
              <CollapsiblePlanningRequest content={selected.initial_request} />
              {selected.active_run?.status === 'waiting_user' && selected.open_input_request && <div className="mt-4"><PlanInputForm run={selected.active_run} request={selected.open_input_request} onAnswered={refresh} /></div>}
              {shown && <>
                {shown.id !== selected.current_version_id && <div className="mt-4 flex items-center justify-between gap-3 rounded-lg border border-gray-700 bg-gray-800/60 px-3 py-2 text-xs text-gray-400"><span>Historical Version · read only{shown.superseded_by_version_id ? ' · superseded by a newer Version' : ''}</span><button type="button" onClick={() => setVersionId(selected.current_version_id)} className="shrink-0 text-indigo-300">View current</button></div>}
                <div className="mt-4 flex flex-wrap items-center gap-2"><select value={shown.id} onChange={(event) => setVersionId(Number(event.target.value))} className="rounded-lg border border-gray-700 bg-gray-800 px-2 py-1.5 text-xs text-gray-200">{versions.map((version) => <option key={version.id} value={version.id}>v{version.version_number} · {version.human_decision}{version.applied ? ' · applied' : ''}</option>)}</select>{previous && <button type="button" onClick={() => setCompare((value) => !value)} className="rounded-lg border border-gray-700 px-2.5 py-1.5 text-xs text-gray-300">{compare ? 'Hide comparison' : `Compare with v${previous.version_number}`}</button>}</div>
                <div className={`mt-3 grid gap-3 ${compare && previous ? 'lg:grid-cols-2' : ''}`}>{compare && previous && <div className="rounded-xl border border-gray-700 bg-gray-950/60 p-4"><div className="mb-2 text-xs text-gray-500">v{previous.version_number}</div><MarkdownContent content={previous.content} /></div>}<div className="rounded-xl border border-gray-700 bg-gray-950/60 p-4"><div className="mb-2 text-xs text-indigo-300">v{shown.version_number}{shown.id === selected.current_version_id ? ' · current' : ''}</div><MarkdownContent content={shown.content} /></div></div>
                {shown.review_feedback && <div className="mt-3 rounded-xl border border-gray-700 bg-gray-800/60 p-3 text-sm text-gray-300"><div className="mb-1 text-xs font-semibold text-gray-500">Reviewer</div>{shown.review_feedback}</div>}
                <PlanRunInputAudit runs={runs} version={shown} />
              </>}
              {selected.current_version && shown?.id === selected.current_version_id && selected.display_state === 'awaiting_review' && <div className="mt-4 space-y-3 border-t border-gray-800 pt-4"><textarea value={revision} onChange={(event) => setRevision(event.target.value)} rows={3} maxLength={50000} placeholder="Revision feedback…" className="w-full resize-y rounded-lg border border-gray-700 bg-gray-800 px-3 py-2 text-sm text-gray-100 outline-none focus:border-indigo-500" /><div className="flex flex-wrap gap-2"><button type="button" disabled={busy} onClick={() => void decide('approve', true)} className="flex items-center gap-1 rounded-lg bg-emerald-600 px-3 py-2 text-xs font-semibold text-white disabled:opacity-40"><Check size={12} /> Approve & attach v{selected.current_version.version_number}</button><button type="button" disabled={busy} onClick={() => void decide('approve', false)} className="rounded-lg border border-emerald-500/40 px-3 py-2 text-xs text-emerald-300 disabled:opacity-40">Approve only</button><button type="button" disabled={busy} onClick={() => void decide('reject', false)} className="rounded-lg border border-red-500/40 px-3 py-2 text-xs text-red-300 disabled:opacity-40">Reject</button><button type="button" disabled={busy || !revision.trim()} onClick={() => void perform(async () => { await api.createPlanRun(selected.id, { run_type: 'user_revision', request: revision.trim(), base_version_id: selected.current_version!.id, expected_current_version_id: selected.current_version!.id }); setRevision(''); })} className="rounded-lg border border-indigo-500/40 px-3 py-2 text-xs text-indigo-300 disabled:opacity-40">Revise</button></div></div>}
              {shown && selected.current_version && ['approved', 'applied', 'rejected', 'awaiting_review'].includes(selected.display_state) && <div className="mt-4 flex flex-wrap gap-2">{shown.id === selected.current_version_id && <button type="button" disabled={busy || selected.active_run_id != null} onClick={() => void perform(() => api.createPlanRun(selected.id, { run_type: 'refresh_context', request: 'Refresh this Plan using the latest task context and repository state.', base_version_id: selected.current_version!.id, expected_current_version_id: selected.current_version!.id }))} className="flex items-center gap-1 rounded-lg border border-gray-700 px-3 py-2 text-xs text-gray-300 disabled:opacity-40"><RefreshCw size={12} /> Refresh context</button>}<button type="button" disabled={busy} onClick={() => void perform(() => api.forkPlan(selected.id, { base_version_id: shown.id }))} className="flex items-center gap-1 rounded-lg border border-gray-700 px-3 py-2 text-xs text-gray-300 disabled:opacity-40"><GitBranch size={12} /> Fork as new Plan</button>{shown.id === selected.current_version_id && selected.current_version.human_decision === 'approved' && !selected.current_version.applied && <button type="button" onClick={() => onToggleVersion(selected.current_version!.id)} className="rounded-lg border border-teal-500/40 px-3 py-2 text-xs text-teal-300">{selectedVersionIds.includes(selected.current_version.id) ? 'Detach from next message' : 'Attach to next message'}</button>}</div>}
              {selected.active_run && ['queued', 'running', 'waiting_user'].includes(selected.active_run.status) && <button type="button" disabled={busy} onClick={() => void perform(() => api.cancelPlanRun(selected.active_run!.id))} className="mt-4 rounded-lg border border-red-500/40 px-3 py-2 text-xs text-red-300 disabled:opacity-40">Cancel Run</button>}
              {!selected.active_run && <button type="button" disabled={busy} onClick={() => void perform(() => api.updatePlan(selected.id, { archived: true, expected_lock_version: selected.lock_version }))} className="mt-4 flex items-center gap-1 rounded-lg border border-gray-700 px-3 py-2 text-xs text-gray-400 disabled:opacity-40"><Archive size={12} /> Archive Plan</button>}
            </div>
          </>}
        </section>
      </div>
    </div>
  );
}
