import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';

import type { PlanResource } from '../../api/client';
import { PlanCatalog } from './PlanCatalog';

const plan = (id: number, title: string) => ({
  id,
  title,
  target_task_id: null,
  project_id: null,
  display_state: 'awaiting_review',
  current_version: null,
  applications: [],
} as PlanResource);

describe('PlanCatalog', () => {
  it('visibly marks the selected Plan and reports selection changes', async () => {
    const onSelectPlan = vi.fn();
    render(
      <PlanCatalog
        plans={[plan(1, 'First Plan'), plan(2, 'Second Plan')]}
        projects={[]}
        selectedPlanId={2}
        onSelectPlan={onSelectPlan}
      />,
    );

    const selected = screen.getByRole('button', { name: /Second Plan/ });
    expect(selected).toHaveAttribute('aria-current', 'true');
    expect(selected.className).toContain('border-indigo-500/70');
    expect(screen.getByRole('button', { name: /First Plan/ })).not.toHaveAttribute('aria-current');

    await userEvent.click(screen.getByRole('button', { name: /First Plan/ }));
    expect(onSelectPlan).toHaveBeenCalledWith(1);
  });
});
