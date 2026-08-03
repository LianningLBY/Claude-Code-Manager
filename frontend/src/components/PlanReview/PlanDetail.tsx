import { useCallback, useEffect, useMemo, useRef, useState } from 'react';

import {
  api,
  isApiRequestError,
  type PlanResource,
  type PlanRun,
  type PlanStaleness,
  type PlanVersion,
  type UploadResult,
} from '../../api/client';
import { useFileUpload } from '../../hooks/useFileUpload';
import { AlertCircle, Archive, ArchiveRestore, Check, GitBranch, Loader2, Paperclip, Play, RefreshCw, X } from '../icons';
import { MarkdownContent } from '../MarkdownContent';
import { CollapsiblePlanningRequest } from './CollapsiblePlanningRequest';
import { PlanInputForm } from './PlanInputForm';
import { planDisplayStateLabel } from './planResourceStatus';
import { PlanRunInputAudit } from './PlanRunInputAudit';
import {
  planHardConflictMessages,
  planStalenessConfirmationMessage,
  planStalenessMessages,
} from './planStaleness';

interface Props {
  plan: PlanResource;
  onRefresh: () => void | Promise<void>;
  onClose?: () => void;
  selectedVersionIds?: number[];
  onToggleVersion?: (versionId: number) => void;
  onNavigateTask?: (taskId: number) => void;
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

function confirmableStaleness(error: unknown): PlanStaleness | null {
  if (!isApiRequestError(error) || error.status !== 409 || !error.detail || typeof error.detail !== 'object') return null;
  const detail = error.detail as Record<string, unknown>;
  return detail.stale === true && detail.can_confirm !== false && detail.hard_conflict !== true
    ? detail as unknown as PlanStaleness
    : null;
}

export function PlanDetail({ plan, onRefresh, onClose, selectedVersionIds = [], onToggleVersion, onNavigateTask }: Props) {
  const [versions, setVersions] = useState<PlanVersion[]>([]);
  const [runs, setRuns] = useState<PlanRun[]>([]);
  const [versionId, setVersionId] = useState<number | null>(plan.current_version_id);
  const [revision, setRevision] = useState('');
  const [compare, setCompare] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [staleness, setStaleness] = useState<PlanStaleness | null>(null);
  const uploads = useFileUpload();
  const fileInput = useRef<HTMLInputElement>(null);
  const previousCurrentVersionId = useRef(plan.current_version_id);

  const load = useCallback(async () => {
    const [versionRows, runRows] = await Promise.all([
      api.listPlanVersions(plan.id),
      api.listPlanResourceRuns(plan.id),
    ]);
    setVersions(versionRows);
    setRuns(runRows);
    setVersionId((current) => versionRows.some((item) => item.id === current) ? current : plan.current_version_id);
  }, [plan.current_version_id, plan.id]);

  useEffect(() => { void load().catch((reason) => setError(reason instanceof Error ? reason.message : String(reason))); }, [load]);
  useEffect(() => {
    setVersionId((current) => current === previousCurrentVersionId.current ? plan.current_version_id : current);
    previousCurrentVersionId.current = plan.current_version_id;
  }, [plan.current_version_id]);
  const shown = versions.find((item) => item.id === versionId) || plan.current_version || null;
  const previous = useMemo(() => shown ? versions.find((item) => item.version_number === shown.version_number - 1) || null : null, [shown, versions]);
  const appliedVersion = versions.find((item) => item.applied) || null;
  const executionApplications = (plan.applications || []).filter((item) => item.execution_task_id != null);

  useEffect(() => {
    if (!shown) { setStaleness(null); return; }
    void api.getPlanVersionStaleness(shown.id).then(setStaleness).catch(() => setStaleness(null));
  }, [shown]);

  const mutate = async (operation: () => Promise<unknown>) => {
    setBusy(true); setError(null);
    try { await operation(); await onRefresh(); await load(); }
    catch (reason) { setError(reason instanceof Error ? reason.message : String(reason)); }
    finally { setBusy(false); }
  };

  const decide = async (decision: 'approve' | 'reject', attach = false) => {
    if (!shown || shown.id !== plan.current_version_id) return;
    await mutate(async () => {
      const invoke = (confirm: boolean) => decision === 'approve'
        ? api.approvePlanVersion(shown.id, shown.id, confirm)
        : api.rejectPlanVersion(shown.id, shown.id, confirm);
      try { await invoke(false); }
      catch (reason) {
        const stale = confirmableStaleness(reason);
        if (!stale || !window.confirm(planStalenessConfirmationMessage(stale, 'approve'))) throw reason;
        await invoke(true);
      }
      if (decision === 'approve' && attach) onToggleVersion?.(shown.id);
    });
  };

  const createExecution = async (approveIfPending: boolean) => {
    if (!shown || shown.id !== plan.current_version_id) return;
    await mutate(async () => {
      const invoke = (confirm: boolean) => api.createVersionExecutionTask(shown.id, shown.id, confirm, approveIfPending);
      let result;
      try { result = await invoke(false); }
      catch (reason) {
        const stale = confirmableStaleness(reason);
        if (!stale || !window.confirm(planStalenessConfirmationMessage(stale, 'execute'))) throw reason;
        result = await invoke(true);
      }
      onNavigateTask?.(result.execution_task_id);
    });
  };

  const revise = async () => {
    if (!shown || !revision.trim() || plan.active_run_id != null) return;
    await mutate(async () => {
      await api.createPlanRun(plan.id, {
        run_type: 'user_revision',
        request: revision.trim(),
        base_version_id: shown.id,
        expected_current_version_id: plan.current_version_id || undefined,
        ...uploadPayload(uploads.uploadedResults),
      });
      setRevision(''); uploads.clear();
    });
  };

  const current = shown?.id === plan.current_version_id;
  const route = plan.pipeline_config;
  const staleMessages = planStalenessMessages(staleness);
  const hardConflictMessages = planHardConflictMessages(staleness);
  return <div className="flex h-full min-h-0 flex-col">
    <header className="flex items-start gap-3 border-b border-gray-800 px-4 py-3">
      <div className="min-w-0 flex-1">
        <div className="truncate text-sm font-semibold text-gray-100">Plan #{plan.id} · {plan.title}</div>
        <div className="mt-1 flex flex-wrap gap-1.5 text-[10px] text-gray-400">
          <span className="rounded-full border border-gray-700 px-2 py-0.5">{planDisplayStateLabel(plan.display_state)}</span>
          {plan.current_version && <span className="rounded-full bg-indigo-500/15 px-2 py-0.5 text-indigo-300">v{plan.current_version.version_number} current</span>}
          {appliedVersion && appliedVersion.id !== plan.current_version_id && <span className="rounded-full bg-teal-500/15 px-2 py-0.5 text-teal-300">v{appliedVersion.version_number} applied</span>}
          {staleness?.stale && <span className="rounded-full bg-amber-500/15 px-2 py-0.5 text-amber-300">stale context</span>}
          {staleness?.hard_conflict && <span className="rounded-full bg-red-500/15 px-2 py-0.5 text-red-300">target conflict</span>}
        </div>
      </div>
      {onClose && <button type="button" onClick={onClose} aria-label="Close Plan" className="rounded-lg p-1.5 text-gray-500 hover:bg-gray-800"><X size={16} /></button>}
    </header>

    <div className="min-h-0 flex-1 overflow-y-auto p-4 sm:p-6">
      {error && <div className="mb-4 rounded-lg border border-red-500/30 bg-red-500/10 px-3 py-2 text-xs text-red-300">{error}</div>}
      {plan.latest_run_error && <div className="mb-4 rounded-lg border border-amber-500/30 bg-amber-500/10 px-3 py-2 text-xs text-amber-300">Latest Run {plan.latest_run_status}: {plan.latest_run_error}</div>}
      {staleness?.stale && !staleness.hard_conflict && <div role="alert" className="mb-4 flex items-start gap-2.5 rounded-lg border border-amber-500/50 bg-amber-500/15 px-3.5 py-3 text-gray-200">
        <AlertCircle size={18} className="mt-0.5 shrink-0 text-amber-400" />
        <div className="min-w-0">
          <div className="text-sm font-semibold text-amber-300">Confirmation required</div>
          <div className="mt-0.5 text-sm leading-5">{staleMessages.join(' ')} You may continue after explicit confirmation; refreshing or re-planning is optional.</div>
        </div>
      </div>}
      {staleness?.hard_conflict && <div className="mb-4 rounded-lg border border-red-500/30 bg-red-500/10 px-3 py-2 text-xs text-red-300"><span className="font-semibold">This action is blocked.</span> {hardConflictMessages.join(' ')}</div>}
      <CollapsiblePlanningRequest content={plan.initial_request} />

      <details className="mt-3 rounded-lg border border-gray-800 bg-gray-900/60 p-3 text-xs text-gray-400">
        <summary className="cursor-pointer font-medium text-gray-300">Frozen Pipeline routes</summary>
        <div className="mt-2 grid gap-2 sm:grid-cols-2">
          <div>Planner: {route.planner.primary.provider} / {route.planner.primary.model} / {route.planner.primary.effort || 'default'}<br />Fallback: {route.planner.fallback.provider} / {route.planner.fallback.model}</div>
          <div>Reviewer: {route.reviewer.enabled ? `${route.reviewer.primary.provider} / ${route.reviewer.primary.model} / ${route.reviewer.primary.effort || 'default'}` : 'disabled'}<br />Input pauses: {route.max_interactions} · revision rounds: {route.max_revision_cycles}</div>
        </div>
      </details>

      {plan.active_run?.status === 'waiting_user' && plan.open_input_request && <div className="mt-4"><PlanInputForm run={plan.active_run} request={plan.open_input_request} onAnswered={onRefresh} /></div>}

      {shown ? <>
        {!current && <div className="mt-4 rounded-lg border border-gray-700 bg-gray-800/60 px-3 py-2 text-xs text-gray-400">Historical Version. You can revise or fork it explicitly; approval remains limited to the current Version.</div>}
        <div className="mt-4 flex flex-wrap items-center gap-2">
          <select aria-label="Plan Version" value={shown.id} onChange={(event) => setVersionId(Number(event.target.value))} className="rounded-lg border border-gray-700 bg-gray-800 px-2 py-1.5 text-xs text-gray-200">
            {versions.map((version) => <option key={version.id} value={version.id}>v{version.version_number} · {version.human_decision}{version.applied ? ' · applied' : ''}</option>)}
          </select>
          {previous && <button type="button" onClick={() => setCompare((value) => !value)} className="rounded-lg border border-gray-700 px-2.5 py-1.5 text-xs text-gray-300">{compare ? 'Hide comparison' : `Compare with v${previous.version_number}`}</button>}
        </div>
        <div className={`mt-3 grid gap-3 ${compare && previous ? 'lg:grid-cols-2' : ''}`}>
          {compare && previous && <div className="rounded-xl border border-gray-700 bg-gray-950/60 p-4"><div className="mb-2 text-xs text-gray-500">v{previous.version_number}</div><MarkdownContent content={previous.content} /></div>}
          <div className="rounded-xl border border-gray-700 bg-gray-950/60 p-4"><div className="mb-2 text-xs text-indigo-300">v{shown.version_number}{current ? ' · current' : ''}</div><MarkdownContent content={shown.content} /></div>
        </div>
        {shown.review_feedback && <div className="mt-3 rounded-xl border border-gray-700 bg-gray-800/60 p-3 text-sm text-gray-300"><div className="mb-1 text-xs font-semibold text-gray-500">Reviewer feedback</div>{shown.review_feedback}</div>}
        {(shown.repo_revision || shown.reviewer_repo_revision) && <details className="mt-3 rounded-lg border border-gray-800 bg-gray-900/50 p-3 text-xs text-gray-400"><summary className="cursor-pointer font-medium text-gray-300">Repository audit</summary><div className="mt-2 grid gap-2 lg:grid-cols-2"><div><div className="mb-1 text-[10px] uppercase tracking-wide text-gray-500">Planner snapshot</div><pre className="overflow-x-auto whitespace-pre-wrap break-all">{JSON.stringify(shown.repo_revision, null, 2)}</pre></div><div><div className="mb-1 text-[10px] uppercase tracking-wide text-gray-500">Reviewer snapshot</div><pre className="overflow-x-auto whitespace-pre-wrap break-all">{JSON.stringify(shown.reviewer_repo_revision, null, 2)}</pre></div></div></details>}
        <PlanRunInputAudit runs={runs} version={shown} />
      </> : <div className="mt-4 rounded-xl border border-gray-800 px-4 py-8 text-center text-sm text-gray-500">No Version has been produced yet.</div>}

      {runs.length > 0 && <details className="mt-4 rounded-xl border border-gray-800 bg-gray-900/50 p-3 text-xs text-gray-400"><summary className="cursor-pointer font-semibold text-gray-300">Run timeline ({runs.length})</summary><div className="mt-2 space-y-2">{runs.map((run) => <div key={run.id} className="border-t border-gray-800 pt-2 first:border-0">Run #{run.id} · {run.run_type} · {run.status} · round {run.round}{run.steps.map((step) => <div key={step.id} className="ml-3 text-gray-500">{step.step_type}: {step.provider}/{step.model || 'default'} ({step.route_slot || 'primary'}) · {step.status}</div>)}</div>)}</div></details>}

      {plan.applications.length > 0 && <details className="mt-4 rounded-xl border border-gray-800 bg-gray-900/50 p-3 text-xs text-gray-400"><summary className="cursor-pointer font-semibold text-gray-300">Application history ({plan.applications.length})</summary><div className="mt-2 space-y-1">{plan.applications.map((application) => { const applied = versions.find((item) => item.id === application.plan_version_id); return <div key={application.id}>v{applied?.version_number || '?'} · {application.application_type === 'execution_task' ? `execution Task #${application.execution_task_id}` : `chat message #${application.user_log_id}`}</div>; })}</div></details>}

      {shown && !plan.active_run_id && <div className="mt-5 space-y-2 border-t border-gray-800 pt-4">
        <textarea value={revision} onChange={(event) => setRevision(event.target.value)} rows={3} maxLength={50000} placeholder={`Revise from v${shown.version_number}…`} disabled={busy} className="w-full resize-y rounded-lg border border-gray-700 bg-gray-800 px-3 py-2 text-sm text-gray-100 outline-none focus:border-indigo-500 disabled:opacity-60" />
        {uploads.uploads.length > 0 && <div className="flex flex-wrap gap-2">{uploads.uploads.map((upload) => <span key={upload.id} className="flex items-center gap-1 rounded-lg border border-gray-700 bg-gray-800 px-2 py-1 text-xs text-gray-300">{upload.preview && <img src={upload.preview} alt="" className="h-8 w-8 rounded object-cover" />}<span className="max-w-40 truncate">{upload.file.name}</span>{upload.status === 'uploading' && <Loader2 size={11} className="animate-spin" />}<button type="button" disabled={busy} onClick={() => uploads.removeFile(upload.id)}><X size={11} /></button></span>)}</div>}
        <div className="flex flex-wrap gap-2">
          <input ref={fileInput} type="file" multiple className="hidden" onChange={(event) => { uploads.addFiles(Array.from(event.target.files || []), setError); event.target.value = ''; }} />
          <button type="button" disabled={busy} onClick={() => fileInput.current?.click()} className="flex items-center gap-1 rounded-lg border border-gray-700 px-3 py-2 text-xs text-gray-300"><Paperclip size={12} /> Revision files</button>
          <button type="button" disabled={busy || !revision.trim() || uploads.isUploading || uploads.hasFailed} onClick={() => void revise()} className="rounded-lg border border-indigo-500/40 px-3 py-2 text-xs text-indigo-300 disabled:opacity-40">Revise from v{shown.version_number}</button>
        </div>
      </div>}

      <div className="mt-4 flex flex-wrap gap-2">
        {shown && current && plan.display_state === 'awaiting_review' && plan.target_task_id != null && <><button type="button" disabled={busy || staleness?.hard_conflict} onClick={() => void decide('approve', true)} className="flex items-center gap-1 rounded-lg bg-emerald-600 px-3 py-2 text-xs font-semibold text-white disabled:opacity-40"><Check size={12} /> Approve & attach v{shown.version_number}</button><button type="button" disabled={busy || staleness?.hard_conflict} onClick={() => void decide('approve')} className="rounded-lg border border-emerald-500/40 px-3 py-2 text-xs text-emerald-300 disabled:opacity-40">Approve v{shown.version_number} only</button><button type="button" disabled={busy} onClick={() => void decide('reject')} className="rounded-lg border border-red-500/40 px-3 py-2 text-xs text-red-300">Reject v{shown.version_number}</button></>}
        {shown && current && plan.display_state === 'awaiting_review' && plan.target_task_id == null && <><button type="button" disabled={busy || staleness?.hard_conflict} onClick={() => void createExecution(true)} className="flex items-center gap-1 rounded-lg bg-indigo-600 px-3 py-2 text-xs font-semibold text-white disabled:opacity-40"><Play size={12} /> Approve v{shown.version_number} & create execution Task</button><button type="button" disabled={busy || staleness?.hard_conflict} onClick={() => void decide('approve')} className="rounded-lg border border-emerald-500/40 px-3 py-2 text-xs text-emerald-300">Approve v{shown.version_number} only</button><button type="button" disabled={busy} onClick={() => void decide('reject')} className="rounded-lg border border-red-500/40 px-3 py-2 text-xs text-red-300">Reject v{shown.version_number}</button></>}
        {shown && current && plan.display_state === 'approved' && plan.target_task_id == null && <button type="button" disabled={busy || staleness?.hard_conflict} onClick={() => void createExecution(false)} className="flex items-center gap-1 rounded-lg bg-indigo-600 px-3 py-2 text-xs font-semibold text-white disabled:opacity-40"><Play size={12} /> Create execution Task</button>}
        {executionApplications.map((application) => { const applied = versions.find((item) => item.id === application.plan_version_id); return <button key={application.id} type="button" onClick={() => onNavigateTask?.(application.execution_task_id!)} className="rounded-lg border border-teal-500/40 px-3 py-2 text-xs text-teal-300">Open v{applied?.version_number || '?'} execution Task #{application.execution_task_id}</button>; })}
        {shown && current && plan.target_task_id != null && shown.human_decision === 'approved' && !shown.applied && onToggleVersion && <button type="button" onClick={() => onToggleVersion(shown.id)} className="rounded-lg border border-teal-500/40 px-3 py-2 text-xs text-teal-300">{selectedVersionIds.includes(shown.id) ? 'Detach from next message' : 'Attach to next message'}</button>}
        {shown && current && !plan.active_run_id && <button type="button" disabled={busy} onClick={() => void mutate(() => api.createPlanRun(plan.id, { run_type: 'refresh_context', request: 'Refresh this Plan using the latest task context and repository state.', base_version_id: shown.id, expected_current_version_id: shown.id }))} className="flex items-center gap-1 rounded-lg border border-gray-700 px-3 py-2 text-xs text-gray-300"><RefreshCw size={12} /> Refresh context</button>}
        {shown && <button type="button" disabled={busy} onClick={() => void mutate(() => api.forkPlan(plan.id, { base_version_id: shown.id }))} className="flex items-center gap-1 rounded-lg border border-gray-700 px-3 py-2 text-xs text-gray-300"><GitBranch size={12} /> Fork</button>}
        {plan.active_run && ['queued', 'running', 'waiting_user'].includes(plan.active_run.status) && <button type="button" disabled={busy} onClick={() => void mutate(() => api.cancelPlanRun(plan.active_run!.id))} className="rounded-lg border border-red-500/40 px-3 py-2 text-xs text-red-300">Cancel Run</button>}
        {!plan.active_run_id && <button type="button" disabled={busy} onClick={() => void mutate(() => api.updatePlan(plan.id, { archived: plan.archived_at == null, expected_lock_version: plan.lock_version }))} className="flex items-center gap-1 rounded-lg border border-gray-700 px-3 py-2 text-xs text-gray-400">{plan.archived_at ? <ArchiveRestore size={12} /> : <Archive size={12} />}{plan.archived_at ? 'Restore' : 'Archive'}</button>}
      </div>
    </div>
  </div>;
}
