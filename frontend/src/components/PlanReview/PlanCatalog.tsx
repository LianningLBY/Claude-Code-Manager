import type { PlanResource, Project } from '../../api/client';
import { ChevronRight } from '../icons';
import { planDisplayStateLabel } from './planResourceStatus';

interface Props {
  plans: PlanResource[];
  projects: Project[];
  selectedPlanId: number | null;
  onSelectPlan: (planId: number) => void;
}

export function PlanCatalog({ plans, projects, selectedPlanId, onSelectPlan }: Props) {
  if (plans.length === 0) {
    return <div className="rounded-xl border border-gray-800 bg-gray-900/50 px-4 py-10 text-center text-sm text-gray-500">No Plans match this filter.</div>;
  }

  return <div className="space-y-2">
      {plans.map((plan) => {
        const project = projects.find((item) => item.id === plan.project_id);
        const selected = plan.id === selectedPlanId;
        const appliedOlder = Boolean(
          plan.current_version
          && !plan.current_version.applied
          && plan.applications.some((item) => item.plan_version_id !== plan.current_version!.id),
        );
        return <button key={plan.id} type="button" onClick={() => onSelectPlan(plan.id)} aria-current={selected ? 'true' : undefined} className={`flex w-full items-center gap-3 rounded-xl border px-4 py-3 text-left transition-colors ${selected ? 'border-indigo-500/70 bg-indigo-500/15 ring-1 ring-inset ring-indigo-400/30' : 'border-gray-800 bg-gray-900/70 hover:border-gray-700 hover:bg-gray-800/70'}`}>
          <div className="min-w-0 flex-1">
            <div className="flex flex-wrap items-center gap-2">
              <span className="text-xs text-gray-500">#{plan.id}</span>
              {plan.current_version && <span className="rounded-full bg-indigo-500/15 px-2 py-0.5 text-[10px] text-indigo-300">v{plan.current_version.version_number}</span>}
              <span className="rounded-full border border-gray-700 px-2 py-0.5 text-[10px] text-gray-400">{planDisplayStateLabel(plan.display_state)}</span>
              {appliedOlder && <span className="rounded-full bg-teal-500/15 px-2 py-0.5 text-[10px] text-teal-300">earlier Version applied</span>}
            </div>
            <div className="mt-1 truncate text-sm font-semibold text-gray-100">{plan.title}</div>
            <div className="mt-1 truncate text-xs text-gray-500">{plan.target_task_id != null ? `Related to Task #${plan.target_task_id}` : 'Standalone Plan'}{project ? ` · ${project.name}` : ''}</div>
          </div>
          <ChevronRight size={15} className={`shrink-0 ${selected ? 'text-indigo-300' : 'text-gray-600'}`} />
        </button>;
      })}
    </div>;
}
