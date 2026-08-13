import { useCallback, useEffect, useMemo, useState } from 'react';

import { api, type DeliveryRunDetail, type PlanVersion, type PRMonitorRun, type Project, type Task } from '../../api/client';
import { MarkdownRenderer } from '../Markdown/MarkdownRenderer';
import { CheckCircle2, ChevronDown, ChevronRight, Circle, GitBranch, GitPullRequest, Loader2, MessageCircle, RefreshCw, X, XCircle } from '../icons';
import { DeliveryRunPanel } from '../Tasks/DeliveryRunPanel';

type StageKey = 'planning' | 'coding' | 'pre_review' | 'publishing' | 'monitoring' | 'deployment';
const STAGES: { key: StageKey; label: string; description: string }[] = [
  { key: 'planning', label: 'Plan', description: 'Versioned Plan and reviewer decision' },
  { key: 'coding', label: 'Development', description: 'Developer Task turns and workspace result' },
  { key: 'pre_review', label: 'Pre-review', description: 'Exact commit-range review evidence' },
  { key: 'publishing', label: 'Publish PR', description: 'Push and pull request publication' },
  { key: 'monitoring', label: 'CI & Review', description: 'Exact-head checks, findings and merge readiness' },
  { key: 'deployment', label: 'Deployment', description: 'Not part of Delivery V1' },
];
const PHASE_INDEX: Record<string, number> = { planning: 0, coding: 1, pre_review: 2, publishing: 3, monitoring: 4, done: 5 };

interface Props {
  runId: number;
  project?: Project;
  onClose: () => void;
  onOpenTask: (taskId: number) => void;
  onOpenPlan: (planId: number) => void;
  onOpenPRMonitor: () => void;
}

function titleCase(value: string): string {
  return value.split('_').map((part) => part.charAt(0).toUpperCase() + part.slice(1)).join(' ');
}

export function DeliveryRunDialog({ runId, project, onClose, onOpenTask, onOpenPlan, onOpenPRMonitor }: Props) {
  const [run, setRun] = useState<DeliveryRunDetail | null>(null);
  const [task, setTask] = useState<Task | null>(null);
  const [plans, setPlans] = useState<Record<number, PlanVersion>>({});
  const [monitor, setMonitor] = useState<PRMonitorRun | null>(null);
  const [expanded, setExpanded] = useState<Set<StageKey>>(() => new Set(['planning']));
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const detail = await api.getDeliveryRun(runId);
      setRun(detail);
      const versionIds = Array.from(new Set(detail.cycles.map((cycle) => cycle.plan_version_id).filter((id): id is number => id != null)));
      const [developer, versions, prRun] = await Promise.all([
        detail.developer_task_id != null ? api.getTask(detail.developer_task_id) : Promise.resolve(null),
        Promise.all(versionIds.map((id) => api.getPlanVersion(id))),
        detail.pr_monitor_run_id != null ? api.getPRMonitorRun(detail.pr_monitor_run_id) : Promise.resolve(null),
      ]);
      setTask(developer);
      setPlans(Object.fromEntries(versions.map((version) => [version.id, version])));
      setMonitor(prRun);
      setError('');
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : String(reason));
    } finally {
      setLoading(false);
    }
  }, [runId]);

  useEffect(() => { void load(); }, [load]);
  useEffect(() => {
    const timer = window.setInterval(() => void load(), 5000);
    return () => window.clearInterval(timer);
  }, [load]);

  const currentIndex = run ? PHASE_INDEX[run.phase] ?? 0 : 0;
  const cyclesByPlan = useMemo(() => run?.cycles.filter((cycle) => cycle.plan_version_id != null) || [], [run]);
  const toggle = (key: StageKey) => setExpanded((current) => {
    const next = new Set(current);
    if (next.has(key)) next.delete(key); else next.add(key);
    return next;
  });
  const stageState = (key: StageKey, index: number) => {
    if (key === 'deployment') return 'future';
    if (!run) return 'pending';
    if (run.activity === 'terminal' && run.outcome !== 'success' && index === currentIndex) return 'failed';
    if (run.phase === 'done' || index < currentIndex) return 'completed';
    if (index === currentIndex) return run.activity;
    return 'pending';
  };

  const content = (key: StageKey) => {
    if (!run) return null;
    if (key === 'planning') return <div className="space-y-3">{cyclesByPlan.length === 0 ? <p className="text-xs text-gray-500">The Plan capability has not produced a Version yet.</p> : cyclesByPlan.map((cycle) => { const plan = cycle.plan_version_id ? plans[cycle.plan_version_id] : null; return <div key={cycle.id} className="rounded-lg border border-gray-800 bg-gray-950/60 p-3"><div className="flex flex-wrap items-center justify-between gap-2"><span className="text-xs font-medium text-gray-300">Cycle {cycle.cycle_number} · {plan ? `Plan #${plan.plan_id} v${plan.version_number}` : `Version #${cycle.plan_version_id}`}</span>{plan && <button type="button" onClick={() => onOpenPlan(plan.plan_id)} className="text-xs text-indigo-300 hover:underline">Open in Plans</button>}</div>{plan ? <div className="prose prose-invert mt-3 max-w-none text-xs text-gray-300"><MarkdownRenderer content={plan.content} /></div> : <Loader2 size={14} className="mt-3 animate-spin text-gray-500" />}</div>; })}</div>;
    if (key === 'coding') return <div className="space-y-3"><div className="grid gap-2 sm:grid-cols-3"><Metric label="Developer Task" value={task ? `#${task.id}` : 'Not created'} /><Metric label="Turns" value={String(run.turn_count)} /><Metric label="Head" value={run.head_sha?.slice(0, 12) || 'Pending'} mono /></div>{task && <button type="button" onClick={() => onOpenTask(task.id)} className="inline-flex items-center gap-1.5 rounded bg-indigo-600/20 px-2.5 py-1.5 text-xs text-indigo-300 hover:bg-indigo-600/30"><MessageCircle size={14} /> Open real Task Chat</button>}<div className="space-y-2">{run.turns.map((turn) => <div key={turn.id} className="rounded-lg border border-gray-800 px-3 py-2 text-xs text-gray-400"><span className="font-medium text-gray-300">Turn {turn.generation}</span> · {titleCase(turn.status)} · attempt {turn.attempts}{turn.last_error && <div className="mt-1 text-red-300">{turn.last_error}</div>}</div>)}</div></div>;
    if (key === 'pre_review') return <div className="space-y-2">{run.cycles.map((cycle) => <div key={cycle.id} className="rounded-lg border border-gray-800 px-3 py-2 text-xs"><div className="flex justify-between gap-2 text-gray-300"><span>Cycle {cycle.cycle_number}</span><span>{cycle.review_verdict ? titleCase(cycle.review_verdict) : 'Pending'}</span></div>{cycle.review_summary && <p className="mt-2 leading-5 text-gray-500">{cycle.review_summary}</p>}{cycle.error_message && <p className="mt-2 text-red-300">{cycle.error_message}</p>}</div>)}</div>;
    if (key === 'publishing') return <div className="grid gap-2 sm:grid-cols-2"><Metric label="Delivery branch" value={run.delivery_branch} mono /><Metric label="Pull request" value={run.pr_number ? `#${run.pr_number}` : 'Pending'} />{run.pr_url && <a href={run.pr_url} target="_blank" rel="noreferrer" className="inline-flex items-center gap-1 text-xs text-indigo-300 hover:underline"><GitPullRequest size={13} /> Open GitHub PR</a>}</div>;
    if (key === 'monitoring') return <div className="space-y-3"><div className="grid gap-2 sm:grid-cols-3"><Metric label="Monitor Run" value={monitor ? `#${monitor.id}` : 'Pending'} /><Metric label="Status" value={monitor ? titleCase(monitor.status) : titleCase(run.wait_reason || 'pending')} /><Metric label="Repairs" value={monitor ? `${monitor.repair_attempts}/${monitor.max_repair_attempts}` : '—'} /></div>{monitor && <button type="button" onClick={onOpenPRMonitor} className="inline-flex items-center gap-1.5 rounded bg-emerald-600/20 px-2.5 py-1.5 text-xs text-emerald-300 hover:bg-emerald-600/30"><GitPullRequest size={14} /> Open in PR Monitor</button>}{run.wait_reason && <div className="rounded-lg border border-amber-500/20 bg-amber-500/10 px-3 py-2 text-xs text-amber-300">Waiting: {titleCase(run.wait_reason)}</div>}</div>;
    return <div className="rounded-lg border border-dashed border-gray-700 px-3 py-3 text-xs leading-5 text-gray-500">Deployment and rollback are not part of Delivery V1. This stage is shown only to make the product boundary explicit; no deployment action is simulated.</div>;
  };

  return <div className="fixed inset-0 z-[85] flex items-end justify-center bg-black/70 sm:items-center sm:p-5" onMouseDown={(event) => event.target === event.currentTarget && onClose()}><div role="dialog" aria-modal="true" aria-label={`Delivery #${runId}`} className="flex h-[92dvh] w-full max-w-5xl flex-col overflow-hidden border border-gray-700 bg-gray-900 shadow-2xl sm:h-[88vh] sm:rounded-2xl"><header className="flex shrink-0 items-start justify-between gap-3 border-b border-gray-800 px-4 py-3 sm:px-5"><div className="min-w-0"><div className="flex flex-wrap items-center gap-2"><h2 className="truncate text-lg font-semibold text-gray-100">Delivery #{runId}</h2>{run && <span className="rounded-full bg-indigo-500/15 px-2 py-0.5 text-[11px] text-indigo-300">{titleCase(run.phase)} · {titleCase(run.activity)}</span>}</div><p className="mt-1 truncate text-xs text-gray-500">{run?.title || 'Loading…'}{project ? ` · ${project.name}` : ''}</p></div><div className="flex gap-1"><button type="button" onClick={() => void load()} className="rounded p-2 text-gray-500 hover:bg-gray-800 hover:text-gray-200" title="Refresh"><RefreshCw size={16} /></button><button type="button" onClick={onClose} className="rounded p-2 text-gray-500 hover:bg-gray-800 hover:text-gray-200" aria-label="Close Delivery"><X size={18} /></button></div></header><div className="min-h-0 flex-1 overflow-y-auto p-4 sm:p-5">{loading && !run ? <div className="flex h-full items-center justify-center gap-2 text-sm text-gray-500"><Loader2 size={17} className="animate-spin" /> Loading Delivery…</div> : error && !run ? <div className="rounded-lg border border-red-500/30 bg-red-500/10 p-3 text-sm text-red-300">{error}</div> : run && <div className="space-y-4"><DeliveryRunPanel runId={run.id} /><div className="space-y-2">{STAGES.map((stage, index) => { const state = stageState(stage.key, index); const open = expanded.has(stage.key); return <section key={stage.key} className="rounded-xl border border-gray-800 bg-gray-950/35"><button type="button" onClick={() => toggle(stage.key)} aria-expanded={open} className="flex w-full items-center gap-3 px-3 py-3 text-left sm:px-4"><StageIcon state={state} /><div className="min-w-0 flex-1"><div className="flex flex-wrap items-center gap-2"><span className="text-sm font-medium text-gray-200">{stage.label}</span><span className="rounded px-1.5 py-0.5 text-[10px] text-gray-500">{titleCase(state)}</span></div><p className="mt-0.5 truncate text-xs text-gray-600">{stage.description}</p></div>{open ? <ChevronDown size={16} className="text-gray-500" /> : <ChevronRight size={16} className="text-gray-500" />}</button>{open && <div className="border-t border-gray-800 px-3 py-3 sm:px-4">{content(stage.key)}</div>}</section>; })}</div><div className="flex items-center gap-2 rounded-lg border border-gray-800 bg-gray-950/50 px-3 py-2 text-xs text-gray-500"><GitBranch size={14} />{run.delivery_branch}</div></div>}</div></div></div>;
}

function Metric({ label, value, mono = false }: { label: string; value: string; mono?: boolean }) { return <div className="rounded-lg border border-gray-800 bg-gray-950/60 p-2.5"><div className="text-[10px] uppercase tracking-wide text-gray-600">{label}</div><div className={`mt-1 break-all text-xs text-gray-300 ${mono ? 'font-mono' : ''}`}>{value}</div></div>; }
function StageIcon({ state }: { state: string }) { if (state === 'completed') return <CheckCircle2 size={19} className="shrink-0 text-emerald-400" />; if (state === 'failed') return <XCircle size={19} className="shrink-0 text-red-400" />; return <Circle size={19} className={`shrink-0 ${['running', 'waiting', 'paused', 'ready'].includes(state) ? 'fill-indigo-500/20 text-indigo-400' : 'text-gray-700'}`} />; }
