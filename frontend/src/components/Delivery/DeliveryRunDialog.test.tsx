import { afterEach, describe, expect, it, vi } from 'vitest';
import { cleanup, render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { api } from '../../api/client';
import { DeliveryRunDialog } from './DeliveryRunDialog';

vi.mock('../../api/client', async (importOriginal) => { const actual = await importOriginal<typeof import('../../api/client')>(); return { ...actual, api: { ...actual.api, getDeliveryRun: vi.fn(), getTask: vi.fn(), getPlanVersion: vi.fn(), getPRMonitorRun: vi.fn() } }; });
vi.mock('../Tasks/DeliveryRunPanel', () => ({ DeliveryRunPanel: ({ runId }: { runId: number }) => <div>Controls for {runId}</div> }));

describe('DeliveryRunDialog', () => {
  afterEach(() => { cleanup(); vi.clearAllMocks(); });
  it('expands authoritative Plan and Task links without duplicating their data', async () => {
    vi.mocked(api.getDeliveryRun).mockResolvedValue({ id: 7, title: 'Ship', phase: 'coding', activity: 'waiting', outcome: null, terminal: 'ready_to_merge', developer_task_id: 12, pr_monitor_run_id: null, cycles: [{ id: 1, cycle_number: 1, plan_version_id: 31 }], turns: [{ id: 2, generation: 1, status: 'completed', attempts: 1, last_error: null }], delivery_branch: 'ccm/delivery/7', turn_count: 1, head_sha: 'a'.repeat(40), wait_reason: null } as never);
    vi.mocked(api.getTask).mockResolvedValue({ id: 12 } as never);
    vi.mocked(api.getPlanVersion).mockResolvedValue({ id: 31, plan_id: 21, version_number: 2, content: '# Real Plan' } as never);
    const onOpenTask = vi.fn(); const onOpenPlan = vi.fn();
    render(<DeliveryRunDialog runId={7} onClose={() => {}} onOpenTask={onOpenTask} onOpenPlan={onOpenPlan} onOpenPRMonitor={() => {}} />);
    expect(await screen.findByText('Real Plan')).toBeInTheDocument();
    await userEvent.click(screen.getByRole('button', { name: /Open in Plans/ }));
    expect(onOpenPlan).toHaveBeenCalledWith(21);
    await userEvent.click(screen.getByRole('button', { name: /Development/ }));
    await userEvent.click(screen.getByRole('button', { name: /Open real Task Chat/ }));
    expect(onOpenTask).toHaveBeenCalledWith(12);
  });
});
