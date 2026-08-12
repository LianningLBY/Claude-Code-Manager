import { afterEach, describe, expect, it, vi } from 'vitest';
import { cleanup, render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';

import { api, type MonitoredRepo, type Project, type SystemConfig } from '../../api/client';
import { DeliveryCreateForm } from './DeliveryCreateForm';

vi.mock('../../api/client', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../../api/client')>();
  return { ...actual, api: { ...actual.api, createDeliveryRun: vi.fn() } };
});

const project: Project = { id: 1, name: 'CCM', worker_id: null, git_url: 'git@github.com:acme/ccm.git', has_remote: true, local_path: '/srv/ccm', default_branch: 'main', status: 'ready', error_message: null, show_in_selector: true, sort_order: 0, tags: [], env_files: [], git_author_name: null, git_author_email: null, git_credential_type: null, git_ssh_key_path: null, git_https_username: null, git_https_token: null, badge_color: null, created_at: '2026-08-12T00:00:00Z' };
const repo: MonitoredRepo = { id: 2, repo_full_name: 'acme/ccm', project_id: 1, worker_id: null, enabled: true, auto_merge: false, webhook_secret: 'masked', provider: 'codex', review_model: null, review_effort: null, review_mode: 'panel', wait_for_ci: true, required_checks: [{ kind: 'check_run', name: 'tests', app_slug: 'github-actions' }], auto_repair: true, max_repair_attempts: 3, merge_queue_mode: 'manual', default_branch: 'main', allowed_authors: [], status: 'active', error_message: null, created_at: '', updated_at: '' };
const config = { delivery_loop_enabled: true, provider_options: ['codex'], default_provider: 'codex', default_model: 'claude-opus-4-6', default_codex_model: 'gpt-5.6-sol', default_effort: 'high', default_codex_service_tier: 'default' } as SystemConfig;

describe('DeliveryCreateForm', () => {
  afterEach(() => { cleanup(); vi.clearAllMocks(); sessionStorage.clear(); });

  it('automatically uses the one compatible repository and server defaults', async () => {
    vi.mocked(api.createDeliveryRun).mockResolvedValue({ id: 9 } as never);
    render(<DeliveryCreateForm projects={[project]} repos={[repo]} config={config} onCreated={() => {}} onNavigateProjects={() => {}} onNavigatePRMonitor={() => {}} />);
    await userEvent.click(screen.getByRole('button', { name: 'Select Project' }));
    await userEvent.click(screen.getByText('CCM'));
    expect(screen.getByText('acme/ccm')).toBeInTheDocument();
    await userEvent.type(screen.getByLabelText('Delivery title'), 'Ship workspace');
    await userEvent.type(screen.getByLabelText('Delivery requirements'), 'Implement and test it.');
    await userEvent.click(screen.getByRole('button', { name: 'Start Delivery' }));
    expect(api.createDeliveryRun).toHaveBeenCalledWith(expect.objectContaining({ project_id: 1, monitored_repo_id: 2, provider: 'codex', model: 'gpt-5.6-sol' }));
  });

  it('fails closed when a project has multiple compatible repositories', async () => {
    render(<DeliveryCreateForm projects={[project]} repos={[repo, { ...repo, id: 3, repo_full_name: 'acme/other' }]} config={config} onCreated={() => {}} onNavigateProjects={() => {}} onNavigatePRMonitor={() => {}} />);
    await userEvent.click(screen.getByRole('button', { name: 'Select Project' }));
    await userEvent.click(screen.getByText('CCM'));
    expect(screen.getByText(/Multiple compatible PR Monitor repositories/)).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Start Delivery' })).toBeDisabled();
  });
});
