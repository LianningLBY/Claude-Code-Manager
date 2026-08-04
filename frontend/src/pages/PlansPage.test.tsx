import { useState } from 'react';
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { api, type PlanResource } from '../api/client';
import { PlansPage } from './PlansPage';

vi.mock('../api/client', () => ({
  api: {
    listPlans: vi.fn(),
    countPlans: vi.fn(),
    listProjects: vi.fn(),
    getPlan: vi.fn(),
  },
}));

vi.mock('../components/PlanReview/PlanCreateForm', () => ({
  PlanCreateForm: ({ onCreated }: { onCreated: (plan: PlanResource) => void }) => (
    <button type="button" onClick={() => onCreated(createdPlan)}>Create standalone Plan</button>
  ),
}));
vi.mock('../components/PlanReview/PlanNeedsInputPanel', () => ({
  PlanNeedsInputPanel: ({ onVisibilityChange }: { onVisibilityChange?: (visible: boolean) => void }) => (
    <button type="button" onClick={() => onVisibilityChange?.(true)}>Show input actions</button>
  ),
}));
vi.mock('../components/PlanReview/VersionedPlanPanel', () => ({
  VersionedPlanPanel: () => <div>Review panel</div>,
}));
vi.mock('../components/PlanReview/PlanCatalog', () => ({
  PlanCatalog: ({
    plans,
    selectedPlanId,
    onSelectPlan,
  }: {
    plans: PlanResource[];
    selectedPlanId: number | null;
    onSelectPlan: (id: number) => void;
  }) => <div>{plans.map((item) => (
    <button
      key={item.id}
      type="button"
      aria-pressed={selectedPlanId === item.id}
      onClick={() => onSelectPlan(item.id)}
    >
      {item.title}
    </button>
  ))}</div>,
}));
vi.mock('../components/PlanReview/PlanDetail', () => ({
  PlanDetail: ({ plan, onClose }: { plan: PlanResource; onClose?: () => void }) => <div>
    <span>Detail for {plan.title}</span>
    <button type="button" onClick={onClose}>Close detail</button>
  </div>,
}));
vi.mock('../components/PlanReview/usePlanEvents', () => ({
  usePlanEvents: vi.fn(),
}));
vi.mock('../components/ProjectSelect', () => ({
  ProjectSelect: () => null,
}));
vi.mock('../hooks/useDialogA11y', () => ({
  useDialogA11y: () => ({ current: null }),
}));

const plan = {
  id: 14,
  title: 'Standalone architecture',
  initial_request: 'Design it',
  initial_attachments: null,
  target_task_id: null,
  project_id: 3,
  target_repo: '/repo',
  target_branch: 'main',
  worker_id: null,
  priority: 0,
  timeout_hours: null,
  created_by: 1,
  current_version_id: null,
  active_run_id: null,
  forked_from_version_id: null,
  archived_at: null,
  closed_at: null,
  lock_version: 0,
  created_at: '2026-08-03T00:00:00Z',
  updated_at: '2026-08-03T00:00:00Z',
  display_state: 'queued',
  legacy: false,
  latest_run_status: 'queued',
  latest_run_error: null,
  pipeline_config: {},
  application: null,
  applications: [],
  current_version: null,
  active_run: null,
  open_input_request: null,
} as PlanResource;

const createdPlan = {
  ...plan,
  id: 15,
  title: 'Newly created plan',
  initial_request: 'Plan the next iteration',
} as PlanResource;

function StatefulPlansPage() {
  const [selectedPlanId, setSelectedPlanId] = useState<number | null>(null);
  return <PlansPage selectedPlanId={selectedPlanId} onSelectedPlanChange={setSelectedPlanId} />;
}

describe('PlansPage', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(api.listPlans).mockResolvedValue([plan]);
    vi.mocked(api.countPlans).mockResolvedValue({ total: 1 });
    vi.mocked(api.listProjects).mockResolvedValue([]);
    vi.mocked(api.getPlan).mockResolvedValue(plan);
  });

  it('owns the Plan catalog, hides an empty action heading, and supports deep-link selection', async () => {
    const onSelectedPlanChange = vi.fn();
    render(<PlansPage selectedPlanId={null} onSelectedPlanChange={onSelectedPlanChange} />);

    expect(await screen.findByRole('button', { name: plan.title })).toBeInTheDocument();
    expect(screen.queryByRole('region', { name: 'Plans requiring action' })).not.toBeInTheDocument();
    expect(screen.queryByRole('heading', { name: 'Plans requiring action' })).not.toBeInTheDocument();
    expect(api.listPlans).toHaveBeenCalledWith(expect.objectContaining({ limit: 20, offset: 0 }));

    await userEvent.click(screen.getByRole('button', { name: 'Show input actions' }));
    expect(screen.getByRole('region', { name: 'Plans requiring action' })).toBeInTheDocument();
    expect(screen.getByRole('heading', { name: 'Plans requiring action' })).toBeInTheDocument();

    await userEvent.click(screen.getByRole('button', { name: plan.title }));
    expect(onSelectedPlanChange).toHaveBeenCalledWith(plan.id);
    expect(screen.getByText(`Detail for ${plan.title}`)).toBeInTheDocument();
  });

  it('uses canonical Plan search/archive filters and keeps a newly created Plan in the catalog', async () => {
    const onSelectedPlanChange = vi.fn();
    render(<PlansPage selectedPlanId={null} onSelectedPlanChange={onSelectedPlanChange} />);

    await screen.findByRole('button', { name: plan.title });
    await userEvent.type(screen.getByPlaceholderText('Search Plans'), 'architecture');
    await userEvent.click(screen.getByRole('button', { name: 'Archived only' }));

    await waitFor(() => expect(api.listPlans).toHaveBeenCalledWith(expect.objectContaining({
      archived_only: true,
      q: 'architecture',
    })));

    await userEvent.click(screen.getByRole('button', { name: 'Create standalone Plan' }));
    expect(onSelectedPlanChange).not.toHaveBeenCalled();
    expect(screen.queryByText(`Detail for ${plan.title}`)).not.toBeInTheDocument();
  });

  it('keeps the existing catalog visible while Plan detail opens and closes', async () => {
    let resolveRefresh!: (rows: PlanResource[]) => void;
    const pendingRefresh = new Promise<PlanResource[]>((resolve) => { resolveRefresh = resolve; });
    vi.mocked(api.listPlans)
      .mockResolvedValueOnce([plan])
      .mockReturnValueOnce(pendingRefresh)
      .mockResolvedValue([plan]);

    render(<StatefulPlansPage />);
    await screen.findByRole('button', { name: plan.title });

    await userEvent.click(screen.getByRole('button', { name: plan.title }));
    expect(screen.getByText(`Detail for ${plan.title}`)).toBeInTheDocument();
    expect(screen.getByRole('button', { name: plan.title })).toBeInTheDocument();
    expect(screen.queryByText('Loading Plans…')).not.toBeInTheDocument();

    resolveRefresh([plan]);
    await waitFor(() => expect(api.listPlans).toHaveBeenCalledTimes(2));
    await userEvent.click(screen.getByRole('button', { name: 'Close detail' }));
    expect(screen.queryByText(`Detail for ${plan.title}`)).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: plan.title })).toBeInTheDocument();
    expect(screen.queryByText('Loading Plans…')).not.toBeInTheDocument();
  });

  it('inserts a newly created Plan without blanking the catalog during reconciliation', async () => {
    let resolveRefresh!: (rows: PlanResource[]) => void;
    const pendingRefresh = new Promise<PlanResource[]>((resolve) => { resolveRefresh = resolve; });
    vi.mocked(api.listPlans)
      .mockResolvedValueOnce([plan])
      .mockReturnValueOnce(pendingRefresh);

    render(<StatefulPlansPage />);
    await screen.findByRole('button', { name: plan.title });

    await userEvent.click(screen.getByRole('button', { name: 'Create standalone Plan' }));
    expect(screen.getByRole('button', { name: createdPlan.title })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: plan.title })).toBeInTheDocument();
    expect(screen.queryByText('Loading Plans…')).not.toBeInTheDocument();

    resolveRefresh([createdPlan, plan]);
    await waitFor(() => expect(api.listPlans).toHaveBeenCalledTimes(2));
  });
});
