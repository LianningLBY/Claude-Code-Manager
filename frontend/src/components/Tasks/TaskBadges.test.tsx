import { beforeEach, describe, expect, it, vi } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import type { Task } from '../../api/client';
import { ModelBadge, PluginsBadge, TaskConfigBadge } from './TaskBadges';

vi.mock('../../api/client', () => ({
  api: {
    config: vi.fn().mockResolvedValue({
      default_codex_model: 'gpt-5.5',
      model_options: ['claude-opus-4-6'],
      codex_model_options: ['gpt-5.5', 'gpt-5.4-mini'],
      effort_options: ['low', 'medium', 'high'],
      codex_effort_options: ['low', 'medium', 'high', 'xhigh'],
      codex_model_efforts: {},
      codex_model_service_tiers: {
        'gpt-5.5': ['default', 'priority'],
        'gpt-5.4-mini': ['default'],
      },
    }),
    listSkills: vi.fn().mockResolvedValue([
      { key: 'monitor', label: 'Monitor' },
      { key: 'code-review', label: 'Code Review' },
      { key: 'sub-agent', label: 'Sub-Agent' },
    ]),
    getRuntimeSettings: vi.fn().mockResolvedValue({
      codex_main_mcp_enabled: true,
      codex_monitor_enabled: true,
    }),
    updateTask: vi.fn().mockResolvedValue({}),
  },
}));

import { api } from '../../api/client';

function makeCodexTask(overrides: Partial<Task> = {}): Task {
  return {
    id: 7,
    provider: 'codex',
    model: 'gpt-5.5',
    effort_level: 'medium',
    codex_service_tier: 'priority',
    timeout_hours: null,
    thinking_budget: null,
    system_prompt_mode: null,
    shared_from_id: null,
    ...overrides,
  } as Task;
}

describe('PluginsBadge capability failures', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('preserves hidden Skills when runtime capability discovery fails', async () => {
    vi.mocked(api.getRuntimeSettings)
      .mockRejectedValueOnce(new Error('temporary settings failure'));
    const onRefresh = vi.fn();
    const task = makeCodexTask({
      enabled_skills: {
        'code-review': true,
        'sub-agent': false,
      },
    });

    const first = render(
      <PluginsBadge task={task} onRefresh={onRefresh} />,
    );
    await userEvent.click(screen.getByTitle('Plugins'));
    const subAgent = await screen.findByRole(
      'button',
      { name: /Sub-Agent/ },
    );
    expect(screen.queryByText('Code Review')).not.toBeInTheDocument();

    await userEvent.click(subAgent);

    expect(api.updateTask).toHaveBeenCalledWith(7, {
      enabled_skills: {
        'code-review': true,
        'sub-agent': true,
      },
    });
    await waitFor(() => expect(onRefresh).toHaveBeenCalledTimes(1));

    first.unmount();
    vi.mocked(api.getRuntimeSettings).mockResolvedValueOnce({
      codex_main_mcp_enabled: true,
      codex_monitor_enabled: true,
    } as Awaited<ReturnType<typeof api.getRuntimeSettings>>);
    render(<PluginsBadge task={task} onRefresh={vi.fn()} />);
    await userEvent.click(screen.getByTitle('Plugins'));

    expect(await screen.findByText('Code Review')).toBeInTheDocument();
    expect(api.getRuntimeSettings).toHaveBeenCalledTimes(2);
  });

  it('shows Monitor for local Codex but hides it for Worker scope', async () => {
    const local = render(
      <PluginsBadge
        task={makeCodexTask({ worker_id: null })}
        onRefresh={vi.fn()}
      />,
    );
    await userEvent.click(screen.getByTitle('Plugins'));
    expect(await screen.findByText('Monitor')).toBeInTheDocument();
    local.unmount();

    render(
      <PluginsBadge
        task={makeCodexTask({ worker_id: 9 })}
        onRefresh={vi.fn()}
      />,
    );
    await userEvent.click(screen.getByTitle('Plugins'));
    await screen.findByText('Sub-Agent');
    expect(screen.queryByText('Monitor')).not.toBeInTheDocument();
  });
});

describe('TaskConfigBadge Codex Fast', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('clears Fast in the same update that selects an unsupported model', async () => {
    render(<TaskConfigBadge task={makeCodexTask()} onRefresh={vi.fn()} />);
    await userEvent.click(screen.getByRole('button', { name: /Config/ }));

    const modelSelect = await waitFor(() => screen.getByDisplayValue('gpt-5.5'));
    await userEvent.selectOptions(modelSelect, 'gpt-5.4-mini');

    expect(api.updateTask).toHaveBeenCalledTimes(1);
    expect(api.updateTask).toHaveBeenCalledWith(7, {
      model: 'gpt-5.4-mini',
      codex_service_tier: 'default',
    });
  });

  it('persists a supported Fast selection for the next turn', async () => {
    const onRefresh = vi.fn();
    render(
      <TaskConfigBadge
        task={makeCodexTask({ codex_service_tier: 'default' })}
        onRefresh={onRefresh}
      />,
    );
    await userEvent.click(screen.getByRole('button', { name: /Config/ }));

    const speedSelect = await waitFor(() => screen.getByLabelText('Codex speed'));
    await userEvent.selectOptions(speedSelect, 'priority');

    expect(api.updateTask).toHaveBeenCalledWith(7, {
      codex_service_tier: 'priority',
    });
    await waitFor(() => expect(onRefresh).toHaveBeenCalledTimes(1));
    expect(screen.getByText('修改在下一轮对话生效')).toBeInTheDocument();
  });

  it('resolves the literal default model alias before gating Fast', async () => {
    render(
      <TaskConfigBadge
        task={makeCodexTask({
          model: 'default',
          codex_service_tier: 'default',
        })}
        onRefresh={vi.fn()}
      />,
    );
    await userEvent.click(screen.getByRole('button', { name: /Config/ }));

    const speedSelect = await waitFor(() => screen.getByLabelText('Codex speed'));
    expect(screen.getByRole('option', { name: 'Fast' })).toBeEnabled();
    await userEvent.selectOptions(speedSelect, 'priority');

    expect(api.updateTask).toHaveBeenCalledWith(7, {
      codex_service_tier: 'priority',
    });
  });

  it('shows the backend error and keeps the server value when Fast cannot be saved', async () => {
    const onRefresh = vi.fn();
    vi.mocked(api.updateTask).mockRejectedValueOnce(
      new Error('Worker Task config cannot change while it is pending or active'),
    );
    render(
      <TaskConfigBadge
        task={makeCodexTask({ codex_service_tier: 'default' })}
        onRefresh={onRefresh}
      />,
    );
    await userEvent.click(screen.getByRole('button', { name: /Config/ }));

    const speedSelect = await waitFor(() => screen.getByLabelText('Codex speed'));
    await userEvent.selectOptions(speedSelect, 'priority');

    expect(await screen.findByRole('alert')).toHaveTextContent(
      '配置保存失败：Worker Task config cannot change while it is pending or active',
    );
    expect(speedSelect).toHaveValue('default');
    expect(onRefresh).toHaveBeenCalledTimes(1);
  });

  it('clears a previous save error after a successful retry', async () => {
    const onRefresh = vi.fn();
    vi.mocked(api.updateTask).mockRejectedValueOnce(new Error('Worker upstream HTTP 502'));
    render(
      <TaskConfigBadge
        task={makeCodexTask({ codex_service_tier: 'default' })}
        onRefresh={onRefresh}
      />,
    );
    await userEvent.click(screen.getByRole('button', { name: /Config/ }));

    const speedSelect = await waitFor(() => screen.getByLabelText('Codex speed'));
    await userEvent.selectOptions(speedSelect, 'priority');
    expect(await screen.findByRole('alert')).toHaveTextContent(
      '配置保存失败：Worker upstream HTTP 502',
    );

    await userEvent.selectOptions(speedSelect, 'priority');

    await waitFor(() => expect(screen.queryByRole('alert')).not.toBeInTheDocument());
    expect(onRefresh).toHaveBeenCalledTimes(2);
  });
});

describe('ModelBadge Codex Fast reconciliation', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('refreshes the authoritative task when a Fast-clearing update response is lost', async () => {
    const onRefresh = vi.fn();
    vi.mocked(api.updateTask).mockRejectedValueOnce(new Error('response lost'));
    render(<ModelBadge task={makeCodexTask()} onRefresh={onRefresh} />);

    await userEvent.click(screen.getByRole('button', { name: 'gpt-5.5' }));
    await userEvent.click(
      await screen.findByRole('button', { name: 'gpt-5.4-mini' }),
    );

    expect(api.updateTask).toHaveBeenCalledWith(7, {
      model: 'gpt-5.4-mini',
      codex_service_tier: 'default',
    });
    await waitFor(() => expect(onRefresh).toHaveBeenCalledTimes(1));
  });
});
