import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import type { Task } from '../../api/client';
import { PlanPanel } from './PlanPanel';

vi.mock('../../api/client', () => ({
  api: {
    approvePlan: vi.fn().mockResolvedValue({}),
    rejectPlan: vi.fn().mockResolvedValue({}),
  },
}));

import { api } from '../../api/client';

describe('PlanPanel', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('fences approval with the routing tuple displayed by the UI', async () => {
    const task = {
      id: 41,
      title: 'Fast plan',
      status: 'plan_review',
      plan_content: 'Do the work',
      provider: 'codex',
      model: 'gpt-5.6-sol',
      codex_service_tier: 'priority',
    } as Task;
    const onRefresh = vi.fn();

    render(<PlanPanel tasks={[task]} onRefresh={onRefresh} />);
    await userEvent.click(screen.getByRole('button', { name: /Approve/ }));

    expect(api.approvePlan).toHaveBeenCalledWith(41, {
      provider: 'codex',
      model: 'gpt-5.6-sol',
      codex_service_tier: 'priority',
    });
    await waitFor(() => expect(onRefresh).toHaveBeenCalled());
  });
});
