import { useCallback, useState } from 'react';

import type { PlanResource, Project } from '../../api/client';
import { useDialogA11y } from '../../hooks/useDialogA11y';
import { ChevronRight } from '../icons';
import { PlanDetail } from './PlanDetail';
import { planDisplayStateLabel } from './planResourceStatus';
import { usePlanEvents } from './usePlanEvents';

interface Props {
  plans: PlanResource[];
  projects: Project[];
  onRefresh: () => void;
}

export function PlanCatalog({ plans, projects, onRefresh }: Props) {
  const [selectedId, setSelectedId] = useState<number | null>(null);
  const [expanded, setExpanded] = useState(false);
  const close = useCallback(() => { setSelectedId(null); setExpanded(false); }, []);
  const dialogRef = useDialogA11y(selectedId != null, close);
  usePlanEvents(plans, onRefresh);
  const selected = plans.find((plan) => plan.id === selectedId) || null;

  if (plans.length === 0) {
    return <div className="rounded-xl border border-gray-800 bg-gray-900/50 px-4 py-10 text-center text-sm text-gray-500">No Plans match this filter.</div>;
  }

  return <>
    <div className="space-y-2">
      {plans.map((plan) => {
        const project = projects.find((item) => item.id === plan.project_id);
        const appliedOlder = Boolean(
          plan.current_version
          && !plan.current_version.applied
          && plan.applications.some((item) => item.plan_version_id !== plan.current_version!.id),
        );
        return <button key={plan.id} type="button" onClick={() => setSelectedId(plan.id)} className="flex w-full items-center gap-3 rounded-xl border border-gray-800 bg-gray-900/70 px-4 py-3 text-left hover:border-gray-700 hover:bg-gray-800/70">
          <div className="min-w-0 flex-1">
            <div className="flex flex-wrap items-center gap-2">
              <span className="truncate text-sm font-semibold text-gray-100">#{plan.id} {plan.title}</span>
              {plan.current_version && <span className="rounded-full bg-indigo-500/15 px-2 py-0.5 text-[10px] text-indigo-300">v{plan.current_version.version_number}</span>}
              <span className="rounded-full border border-gray-700 px-2 py-0.5 text-[10px] text-gray-400">{planDisplayStateLabel(plan.display_state)}</span>
              {appliedOlder && <span className="rounded-full bg-teal-500/15 px-2 py-0.5 text-[10px] text-teal-300">earlier Version applied</span>}
            </div>
            <div className="mt-1 truncate text-xs text-gray-500">{plan.target_task_id != null ? `Related to Task #${plan.target_task_id}` : 'Standalone Plan'}{project ? ` · ${project.name}` : ''}</div>
          </div>
          <ChevronRight size={15} className="shrink-0 text-gray-600" />
        </button>;
      })}
    </div>

    {selected && <div className="fixed inset-0 z-[80] flex items-end justify-center bg-black/65 sm:items-center sm:p-5" onMouseDown={(event) => event.target === event.currentTarget && close()}>
      <div ref={dialogRef} role="dialog" aria-modal="true" aria-label={`Plan #${selected.id}`} className={`w-full overflow-hidden border border-gray-700 bg-gray-900 shadow-2xl transition-[height] sm:h-[min(86vh,820px)] sm:max-w-5xl sm:rounded-2xl ${expanded ? 'h-[100dvh]' : 'h-[70dvh]'}`}>
        <div className="absolute left-1/2 top-2 z-10 -translate-x-1/2 sm:hidden"><button type="button" onClick={() => setExpanded((value) => !value)} className="h-1.5 w-12 rounded-full bg-gray-600" aria-label={expanded ? 'Collapse Plan detail' : 'Expand Plan detail'} /></div>
        <PlanDetail plan={selected} onRefresh={onRefresh} onClose={close} onNavigateTask={(taskId) => { window.location.hash = `#/tasks/chat/${taskId}`; close(); }} />
      </div>
    </div>}
  </>;
}
