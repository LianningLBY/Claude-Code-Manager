import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { api } from '../api/client';
import { FALLBACK_PLAN_PIPELINE_CONFIG } from '../components/PlanReview/planPipelineDefaults';
import { SettingsPage } from './SettingsPage';

vi.mock('../api/client', () => ({
  api: {
    config: vi.fn(),
    getPlanPipelineSettings: vi.fn(),
    updatePlanPipelineSettings: vi.fn(),
  },
}));

describe('SettingsPage', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(api.config).mockResolvedValue({
      default_provider: 'codex',
      provider_options: ['claude', 'codex'],
      default_model: 'claude-opus-4-6',
      model_options: ['claude-fable-5', 'claude-sonnet-5'],
      default_codex_model: 'gpt-5.6-sol',
      codex_model_options: ['gpt-5.6-sol', 'gpt-5.6-terra'],
      default_effort: 'medium',
      effort_options: ['low', 'medium', 'high'],
      claude_model_efforts: {},
      claude_model_context_windows: {},
      codex_effort_options: ['low', 'medium', 'high', 'xhigh'],
      codex_model_efforts: {},
      codex_model_service_tiers: {},
    });
    vi.mocked(api.getPlanPipelineSettings).mockResolvedValue(
      FALLBACK_PLAN_PIPELINE_CONFIG,
    );
    vi.mocked(api.updatePlanPipelineSettings).mockImplementation(
      async (value) => value,
    );
  });

  it('persists the global maximum Plan rounds', async () => {
    render(<SettingsPage />);

    const rounds = await screen.findByLabelText('Plan maximum rounds');
    await userEvent.clear(rounds);
    await userEvent.type(rounds, '1');
    await userEvent.click(screen.getByRole('button', { name: 'Save settings' }));

    await waitFor(() => expect(api.updatePlanPipelineSettings).toHaveBeenCalledWith(
      expect.objectContaining({ max_revision_cycles: 1 }),
    ));
    expect(await screen.findByRole('button', { name: 'Saved' })).toBeInTheDocument();
  });
});
