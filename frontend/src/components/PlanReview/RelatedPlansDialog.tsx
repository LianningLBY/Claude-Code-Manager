import { useEffect, useMemo, useState } from 'react';

import type { Task } from '../../api/client';
import {
  Check,
  ChevronLeft,
  ChevronRight,
  ListPlus,
  ListTodo,
  Loader2,
  Paperclip,
  Trash2,
  X,
} from '../icons';
import { PlanPipelineBadge, PlanRevisionBadge } from '../Tasks/TaskBadges';
import { PlanProgress } from './PlanProgress';
import { getPlanStatusMeta } from './planStatus';

type PlanFilter = 'all' | 'decision' | 'running' | 'approved';

interface RelatedPlansDialogProps {
  open: boolean;
  taskId: number;
  plans: Task[];
  loading: boolean;
  error: string | null;
  creating: boolean;
  busyId: number | null;
  selectedPlanIds: number[];
  staleIds: Set<number>;
  createInput: string;
  onCreateInputChange: (value: string) => void;
  onCreate: () => Promise<void>;
  onApprove: (plan: Task, attach: boolean) => Promise<void>;
  onReject: (planId: number) => Promise<void>;
  onRevise: (plan: Task, feedback: string) => Promise<number | null>;
  onCancel: (planId: number) => Promise<void>;
  onDelete: (planId: number) => Promise<boolean>;
  onToggleAttachment: (planId: number) => void;
  onClose: () => void;
}

const RUNNING_STATUSES = new Set(['pending', 'in_progress', 'executing']);
const DELETABLE_PLAN_STATUSES = new Set([
  'pending',
  'plan_review',
  'superseded',
  'failed',
  'cancelled',
  'conflict',
  'completed',
]);

function matchesFilter(plan: Task, filter: PlanFilter) {
  if (filter === 'decision') return plan.status === 'plan_review';
  if (filter === 'running') return RUNNING_STATUSES.has(plan.status);
  if (filter === 'approved') return plan.status === 'completed' && plan.plan_approved === true;
  return true;
}

function PlanListItem({
  plan,
  active,
  selected,
  stale,
  onClick,
}: {
  plan: Task;
  active: boolean;
  selected: boolean;
  stale: boolean;
  onClick: () => void;
}) {
  const status = getPlanStatusMeta(plan);
  return (
    <button
      type="button"
      onClick={onClick}
      className={`w-full rounded-xl border p-3 text-left transition-colors ${
        active
          ? 'border-indigo-500/60 bg-indigo-500/10'
          : 'border-gray-700 bg-gray-800/70 hover:border-gray-600'
      }`}
    >
      <div className="flex items-start gap-2">
        <div className="min-w-0 flex-1">
          <div className="flex flex-wrap items-center gap-1.5">
            <span className={`rounded-full px-2 py-0.5 text-[10px] font-medium ${status.className}`}>
              {status.label}
            </span>
            {selected && (
              <span className="rounded-full bg-teal-500/15 px-2 py-0.5 text-[10px] font-medium text-teal-300">
                Attached
              </span>
            )}
            {stale && (
              <span className="rounded-full bg-amber-500/15 px-2 py-0.5 text-[10px] font-medium text-amber-300">
                stale
              </span>
            )}
          </div>
          <div className="mt-2 truncate text-sm font-medium text-gray-100">
            #{plan.id} {plan.title || 'Untitled Plan'}
          </div>
          <p className="mt-1 line-clamp-2 whitespace-pre-wrap text-[11px] leading-4 text-gray-500">
            {plan.description || 'No planning request recorded.'}
          </p>
          <div className="mt-1.5 text-[10px] text-gray-600">
            Round {Math.max(1, plan.plan_stage_round || 1)}
            {plan.plan_stage_model ? ` · ${plan.plan_stage_model}` : ''}
          </div>
        </div>
        <ChevronRight size={15} className="mt-1 shrink-0 text-gray-600" />
      </div>
    </button>
  );
}

export function RelatedPlansDialog({
  open,
  taskId,
  plans,
  loading,
  error,
  creating,
  busyId,
  selectedPlanIds,
  staleIds,
  createInput,
  onCreateInputChange,
  onCreate,
  onApprove,
  onReject,
  onRevise,
  onCancel,
  onDelete,
  onToggleAttachment,
  onClose,
}: RelatedPlansDialogProps) {
  const [selectedPlanId, setSelectedPlanId] = useState<number | null>(null);
  const [filter, setFilter] = useState<PlanFilter>('all');
  const [reviseText, setReviseText] = useState('');

  useEffect(() => {
    if (!open) return;
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') onClose();
    };
    const previousOverflow = document.body.style.overflow;
    document.body.style.overflow = 'hidden';
    document.addEventListener('keydown', onKeyDown);
    return () => {
      document.body.style.overflow = previousOverflow;
      document.removeEventListener('keydown', onKeyDown);
    };
  }, [onClose, open]);

  const filteredPlans = useMemo(
    () => plans.filter((plan) => matchesFilter(plan, filter)),
    [filter, plans],
  );
  const fallbackPlan = plans.find((plan) => plan.status === 'plan_review') || plans[0] || null;
  const selectedPlan = plans.find((plan) => plan.id === selectedPlanId) || fallbackPlan;
  const effectiveSelectedId = selectedPlanId ?? fallbackPlan?.id ?? null;

  const submitRevision = async (plan: Task) => {
    const revisedPlanId = await onRevise(plan, reviseText.trim());
    if (revisedPlanId != null) {
      setSelectedPlanId(revisedPlanId);
      setReviseText('');
    }
  };

  const deletePlan = async (planId: number) => {
    if (await onDelete(planId)) {
      setSelectedPlanId(null);
      setReviseText('');
    }
  };

  if (!open) return null;

  const filters: { id: PlanFilter; label: string; count: number }[] = [
    { id: 'all', label: 'All', count: plans.length },
    { id: 'decision', label: 'Decision', count: plans.filter((plan) => matchesFilter(plan, 'decision')).length },
    { id: 'running', label: 'Running', count: plans.filter((plan) => matchesFilter(plan, 'running')).length },
    { id: 'approved', label: 'Approved', count: plans.filter((plan) => matchesFilter(plan, 'approved')).length },
  ];

  return (
    <div
      className="fixed inset-0 z-[70] flex items-end justify-center bg-black/60 sm:items-center sm:p-5"
      onMouseDown={(event) => {
        if (event.target === event.currentTarget) onClose();
      }}
    >
      <div
        role="dialog"
        aria-modal="true"
        aria-label={`Plans for Task #${taskId}`}
        className={`flex w-full overflow-hidden border border-gray-700 bg-gray-900 shadow-2xl transition-[height] sm:h-[min(82vh,760px)] sm:max-w-5xl sm:rounded-2xl ${
          selectedPlanId == null
            ? 'h-[72dvh] rounded-t-2xl'
            : 'h-[100dvh] rounded-none'
        }`}
      >
        <section className={`${selectedPlanId == null ? 'flex' : 'hidden'} w-full flex-col bg-gray-900 sm:flex sm:w-80 sm:shrink-0 sm:border-r sm:border-gray-800`}>
          <div className="shrink-0 border-b border-gray-800 px-4 pb-3 pt-3">
            <div className="mx-auto mb-2 h-1 w-9 rounded-full bg-gray-700 sm:hidden" />
            <div className="flex items-center gap-2">
              <ListTodo size={16} className="text-indigo-300" />
              <h2 className="text-sm font-semibold text-gray-100">Plans</h2>
              <span className="text-xs text-gray-500">Task #{taskId}</span>
              <span className="flex-1" />
              <button type="button" onClick={onClose} className="p-1 text-gray-500 hover:text-gray-300 sm:hidden" aria-label="Close Plans">
                <X size={16} />
              </button>
            </div>
            <form
              className="mt-3 flex gap-2"
              onSubmit={(event) => {
                event.preventDefault();
                void onCreate();
              }}
            >
              <input
                value={createInput}
                onChange={(event) => onCreateInputChange(event.target.value)}
                placeholder="Create an independent Plan…"
                maxLength={200000}
                className="min-w-0 flex-1 rounded-lg border border-gray-700 bg-gray-800 px-3 py-2 text-xs text-gray-100 outline-none focus:border-indigo-500"
              />
              <button
                type="submit"
                disabled={!createInput.trim() || creating}
                className="flex h-8 w-8 shrink-0 items-center justify-center rounded-lg bg-indigo-600 text-white hover:bg-indigo-500 disabled:opacity-40"
                aria-label="Create Plan"
                title="Create Plan"
              >
                {creating ? <Loader2 size={13} className="animate-spin" /> : <ListPlus size={14} />}
              </button>
            </form>
          </div>

          <div className="flex shrink-0 gap-1.5 overflow-x-auto px-3 py-2">
            {filters.map((item) => (
              <button
                key={item.id}
                type="button"
                onClick={() => setFilter(item.id)}
                className={`shrink-0 rounded-full border px-2.5 py-1 text-[10px] font-medium ${
                  filter === item.id
                    ? 'border-indigo-500 bg-indigo-500/15 text-indigo-300'
                    : 'border-gray-700 text-gray-500 hover:text-gray-300'
                }`}
              >
                {item.label} {item.count}
              </button>
            ))}
          </div>

          {error && (
            <div className="mx-3 mb-2 rounded-lg border border-red-500/30 bg-red-500/10 px-3 py-2 text-xs text-red-400 sm:hidden">
              {error}
            </div>
          )}

          <div className="flex-1 space-y-2 overflow-y-auto px-3 pb-3">
            {loading && plans.length === 0 && (
              <div className="flex items-center justify-center gap-2 py-10 text-xs text-gray-500">
                <Loader2 size={13} className="animate-spin" /> Loading Plan history…
              </div>
            )}
            {!loading && plans.length === 0 && (
              <div className="rounded-xl border border-dashed border-gray-700 px-4 py-8 text-center text-xs leading-5 text-gray-500">
                No Plans yet. Creating one will not interrupt the current session.
              </div>
            )}
            {plans.length > 0 && filteredPlans.length === 0 && (
              <div className="py-8 text-center text-xs text-gray-600">No Plans in this view.</div>
            )}
            {filteredPlans.map((plan) => (
              <PlanListItem
                key={plan.id}
                plan={plan}
                active={plan.id === effectiveSelectedId}
                selected={selectedPlanIds.includes(plan.id)}
                stale={staleIds.has(plan.id)}
                onClick={() => {
                  setSelectedPlanId(plan.id);
                  setReviseText('');
                }}
              />
            ))}
          </div>
        </section>

        <section className={`${selectedPlanId == null ? 'hidden' : 'flex'} min-w-0 flex-1 flex-col bg-gray-900 sm:flex`}>
          {selectedPlan ? (
            <>
              <header className="shrink-0 border-b border-gray-800 px-4 pb-3 pt-[max(0.75rem,env(safe-area-inset-top))] sm:px-5 sm:pt-4">
                <div className="flex items-center gap-2">
                  <button
                    type="button"
                    onClick={() => {
                      setSelectedPlanId(null);
                      setReviseText('');
                    }}
                    className="flex items-center gap-1 text-xs font-medium text-indigo-300 sm:hidden"
                  >
                    <ChevronLeft size={15} /> Plans
                  </button>
                  <span className="flex-1 sm:hidden" />
                  <span className={`rounded-full px-2.5 py-1 text-[10px] font-medium sm:hidden ${getPlanStatusMeta(selectedPlan).className}`}>
                    {getPlanStatusMeta(selectedPlan).label}
                  </span>
                  {DELETABLE_PLAN_STATUSES.has(selectedPlan.status) && (
                    <button
                      type="button"
                      onClick={() => void deletePlan(selectedPlan.id)}
                      disabled={busyId === selectedPlan.id}
                      className="flex items-center gap-1 rounded-lg border border-red-500/30 px-2.5 py-1.5 text-xs font-medium text-red-300 hover:bg-red-500/10 disabled:opacity-40"
                      aria-label={`Delete Plan #${selectedPlan.id}`}
                      title="Delete Plan"
                    >
                      {busyId === selectedPlan.id
                        ? <Loader2 size={12} className="animate-spin" />
                        : <Trash2 size={12} />}
                      <span className="hidden md:inline">Delete</span>
                    </button>
                  )}
                  <button type="button" onClick={onClose} className="hidden p-1 text-gray-500 hover:text-gray-300 sm:block" aria-label="Close Plans">
                    <X size={16} />
                  </button>
                </div>
                <div className="mt-3 flex flex-wrap items-center gap-2 sm:mt-0">
                  <h2 className="min-w-0 text-base font-semibold text-gray-100 sm:text-lg">
                    #{selectedPlan.id} {selectedPlan.title || 'Untitled Plan'}
                  </h2>
                  <div className="hidden sm:block">
                    <span className={`rounded-full px-2.5 py-1 text-[10px] font-medium ${getPlanStatusMeta(selectedPlan).className}`}>
                      {getPlanStatusMeta(selectedPlan).label}
                    </span>
                  </div>
                  <PlanPipelineBadge task={selectedPlan} />
                  <PlanRevisionBadge task={selectedPlan} />
                </div>
                <div className="mt-3">
                  <PlanProgress task={selectedPlan} />
                </div>
              </header>

              <div className="min-h-0 flex-1 overflow-y-auto px-4 py-3 sm:px-5 sm:py-4">
                {error && (
                  <div className="mb-3 rounded-lg border border-red-500/30 bg-red-500/10 px-3 py-2 text-xs text-red-400">
                    {error}
                  </div>
                )}
                {staleIds.has(selectedPlan.id) && (
                  <div className="mb-3 rounded-lg border border-amber-500/30 bg-amber-500/10 px-3 py-2 text-xs text-amber-300">
                    This Plan is based on older conversation or repository state. You will be asked to confirm before using it.
                  </div>
                )}
                {selectedPlan.metadata_?.plan_review_exhausted && (
                  <div className="mb-3 rounded-lg border border-amber-500/30 bg-amber-500/10 px-3 py-2 text-xs leading-5 text-amber-300">
                    <span className="font-medium">Reviewer revision limit reached:</span>{' '}
                    {selectedPlan.metadata_.plan_review_feedback || 'Unresolved feedback remains.'}
                  </div>
                )}
                {selectedPlan.description && (
                  <div className="mb-3 rounded-lg border border-gray-800 bg-gray-950/40 px-3 py-2">
                    <div className="text-[10px] font-medium uppercase tracking-wide text-gray-600">Planning request</div>
                    <p className="mt-1 whitespace-pre-wrap text-xs leading-5 text-gray-400">{selectedPlan.description}</p>
                  </div>
                )}
                <div className="min-h-48 rounded-xl border border-gray-800 bg-gray-950/55 p-4">
                  {RUNNING_STATUSES.has(selectedPlan.status) && !selectedPlan.plan_content ? (
                    <div className="flex items-center gap-2 text-sm text-indigo-300">
                      <Loader2 size={14} className="animate-spin" />
                      {getPlanStatusMeta(selectedPlan).label}…
                    </div>
                  ) : selectedPlan.plan_content ? (
                    <pre className="whitespace-pre-wrap font-mono text-xs leading-6 text-gray-300 sm:text-[13px]">
                      {selectedPlan.plan_content}
                    </pre>
                  ) : (
                    <div className="py-12 text-center text-xs text-gray-600">No Plan content was produced.</div>
                  )}
                </div>
                {selectedPlan.plan_applied_at && (
                  <div className="mt-3 text-xs text-gray-500">Applied to a user message.</div>
                )}
              </div>

              <footer className="shrink-0 border-t border-gray-800 bg-gray-900 px-4 pb-[max(1rem,env(safe-area-inset-bottom))] pt-3 sm:px-5 sm:pb-4">
                {selectedPlan.status === 'plan_review' && (
                  <div className="space-y-2.5">
                    <div className="flex gap-2">
                      <input
                        value={reviseText}
                        onChange={(event) => setReviseText(event.target.value)}
                        onKeyDown={(event) => {
                          if (event.key === 'Enter' && !event.nativeEvent.isComposing && reviseText.trim()) {
                            event.preventDefault();
                            void submitRevision(selectedPlan);
                          }
                        }}
                        placeholder="Describe changes for a new revision…"
                        className="min-w-0 flex-1 rounded-lg border border-gray-700 bg-gray-800 px-3 py-2 text-xs text-gray-100 outline-none focus:border-indigo-500"
                      />
                      <button
                        type="button"
                        onClick={() => void submitRevision(selectedPlan)}
                        disabled={!reviseText.trim() || busyId === selectedPlan.id}
                        className="rounded-lg border border-gray-600 px-3 py-2 text-xs font-medium text-gray-300 hover:bg-gray-800 disabled:opacity-40"
                      >
                        Revise
                      </button>
                    </div>
                    <div className="flex flex-wrap items-center gap-2">
                      <button
                        type="button"
                        onClick={() => void onReject(selectedPlan.id)}
                        disabled={busyId === selectedPlan.id}
                        className="rounded-lg border border-red-500/40 px-3 py-2 text-xs font-medium text-red-300 hover:bg-red-500/10 disabled:opacity-40"
                      >
                        Reject
                      </button>
                      <span className="flex-1" />
                      <button
                        type="button"
                        onClick={() => void onApprove(selectedPlan, false)}
                        disabled={busyId === selectedPlan.id}
                        className="rounded-lg border border-green-500/40 px-3 py-2 text-xs font-medium text-green-300 hover:bg-green-500/10 disabled:opacity-40"
                      >
                        Approve only
                      </button>
                      <button
                        type="button"
                        onClick={() => void onApprove(selectedPlan, true)}
                        disabled={busyId === selectedPlan.id}
                        className="flex items-center gap-1.5 rounded-lg bg-green-600 px-3 py-2 text-xs font-medium text-white hover:bg-green-500 disabled:opacity-40"
                      >
                        {busyId === selectedPlan.id ? <Loader2 size={12} className="animate-spin" /> : <Check size={12} />}
                        Approve & attach
                      </button>
                    </div>
                  </div>
                )}
                {RUNNING_STATUSES.has(selectedPlan.status) && (
                  <div className="flex justify-end">
                    <button
                      type="button"
                      onClick={() => void onCancel(selectedPlan.id)}
                      disabled={busyId === selectedPlan.id}
                      className="rounded-lg border border-red-500/40 px-3 py-2 text-xs font-medium text-red-300 hover:bg-red-500/10 disabled:opacity-40"
                    >
                      Cancel run
                    </button>
                  </div>
                )}
                {selectedPlan.status === 'completed'
                  && selectedPlan.plan_approved === true
                  && selectedPlan.plan_applied_at == null && (
                  <div className="flex justify-end">
                    <button
                      type="button"
                      onClick={() => onToggleAttachment(selectedPlan.id)}
                      className={`flex items-center gap-1.5 rounded-lg px-3 py-2 text-xs font-medium ${
                        selectedPlanIds.includes(selectedPlan.id)
                          ? 'border border-indigo-500/40 bg-indigo-500/10 text-indigo-200'
                          : 'bg-indigo-600 text-white hover:bg-indigo-500'
                      }`}
                    >
                      {selectedPlanIds.includes(selectedPlan.id) ? <Check size={12} /> : <Paperclip size={12} />}
                      {selectedPlanIds.includes(selectedPlan.id) ? 'Attached to next message' : 'Attach to next message'}
                    </button>
                  </div>
                )}
              </footer>
            </>
          ) : (
            <div className="flex h-full items-center justify-center text-sm text-gray-600">Select a Plan to view details.</div>
          )}
        </section>
      </div>
    </div>
  );
}
