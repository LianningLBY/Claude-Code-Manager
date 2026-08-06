import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { cleanup, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';

import { BrowserReviewPage } from './BrowserReviewPage';
import type { BrowserReviewConfig, BrowserReviewJob } from '../api/client';


vi.mock('../api/client', () => ({
  api: {
    getBrowserReviewConfig: vi.fn(),
    listBrowserReviews: vi.fn(),
    createBrowserReview: vi.fn(),
    getBrowserReview: vi.fn(),
    cancelBrowserReview: vi.fn(),
    getBrowserReviewArtifact: vi.fn(),
  },
}));


import { api } from '../api/client';


const config: BrowserReviewConfig = {
  default_goal: 'Find frontend regressions',
  default_provider: 'codex',
  providers: ['codex', 'claude'],
  default_models: { codex: 'gpt-5.6-terra', claude: 'claude-opus-4-6' },
  models_by_provider: {
    codex: ['gpt-5.6-sol', 'gpt-5.6-terra', 'gpt-5.6-luna'],
    claude: ['claude-opus-4-6'],
  },
  default_effort: 'medium',
  effort_options: {
    codex: ['low', 'medium', 'high'],
    claude: ['low', 'medium', 'high'],
  },
  model_efforts: {
    codex: { 'gpt-5.6-terra': ['low', 'medium', 'high'] },
    claude: {},
  },
  codex_service_tiers: ['default', 'priority'],
  codex_model_service_tiers: { 'gpt-5.6-terra': ['default', 'priority'] },
  browser_channels: ['chrome', 'chromium'],
  max_concurrent_jobs: 1,
  execution: 'ccm_task_account_pool',
};

const completedJob: BrowserReviewJob = {
  id: 'job-1',
  task_id: 73,
  inline_tool: false,
  status: 'completed',
  stage: 'completed',
  url: 'http://127.0.0.1:5173',
  goal: 'Find frontend regressions',
  provider: 'codex',
  model: 'gpt-5.6-terra',
  reasoning_effort: 'medium',
  codex_service_tier: 'default',
  allow_actions: false,
  capture_only: false,
  browser_channel: 'chrome',
  viewport_width: 1440,
  viewport_height: 900,
  max_steps: 20,
  max_actions: 60,
  created_at: '2026-08-04T00:00:00Z',
  started_at: '2026-08-04T00:00:01Z',
  completed_at: '2026-08-04T00:00:02Z',
  error: null,
  response_id: null,
  steps: 1,
  actions: 1,
  latest_screenshot: null,
  telemetry: {
    page_errors: [{ message: 'render exploded' }],
  },
  action_batches: [{
    step: 1,
    actions: [{ type: 'scroll', scroll_y: 500 }],
  }],
  trace: [
    {
      id: 1,
      kind: 'decision',
      title: '模型观察与决策',
      detail: '首屏表单已检查，下一步滚动查看错误面板。',
      timestamp: '2026-08-04T00:00:01Z',
    },
    {
      id: 2,
      kind: 'tool',
      title: '滚动查看页面',
      detail: '{"delta_y": 500}',
      tool_name: 'browser_scroll',
      timestamp: '2026-08-04T00:00:02Z',
    },
  ],
  artifacts: ['report.md'],
  report: '# Result\n\nPass with issues',
};


describe('BrowserReviewPage', () => {
  beforeEach(() => {
    vi.mocked(api.getBrowserReviewConfig).mockResolvedValue(config);
    vi.mocked(api.listBrowserReviews).mockResolvedValue([]);
    vi.mocked(api.createBrowserReview).mockResolvedValue(completedJob);
  });

  afterEach(() => {
    cleanup();
    vi.clearAllMocks();
  });

  it('creates a review through the selected CCM provider', async () => {
    const user = userEvent.setup();
    render(<BrowserReviewPage />);

    expect(await screen.findByText(/复用现有 Claude\/Codex 账号池/)).toBeInTheDocument();
    expect(screen.getByLabelText('Provider')).toHaveValue('codex');

    await user.type(screen.getByLabelText('待检测网站'), 'http://127.0.0.1:5173');
    await user.click(screen.getByRole('button', { name: '开始检测' }));

    await waitFor(() => {
      expect(api.createBrowserReview).toHaveBeenCalledWith(expect.objectContaining({
        url: 'http://127.0.0.1:5173',
        goal: 'Find frontend regressions',
        provider: 'codex',
        model: 'gpt-5.6-terra',
        codex_service_tier: 'default',
        allow_actions: false,
        browser_channel: 'chrome',
        viewport_width: 1440,
        viewport_height: 900,
      }));
    });
  });

  it('renders recorded actions, telemetry and the final report', async () => {
    vi.mocked(api.listBrowserReviews).mockResolvedValue([completedJob]);
    render(<BrowserReviewPage />);

    expect(await screen.findByText('Pass with issues')).toBeInTheDocument();
    expect(screen.getByText(/render exploded/)).toBeInTheDocument();
    expect(screen.getByText('模型轨迹')).toBeInTheDocument();
    expect(screen.getByText(/首屏表单已检查/)).toBeInTheDocument();
    expect(screen.getByText('browser_scroll')).toBeInTheDocument();
    expect(screen.getByText(/查看原始动作数据/)).toBeInTheDocument();
    expect(screen.getByText('report.md')).toBeInTheDocument();
    expect(screen.getByText('查看 CCM Task #73')).toBeInTheDocument();
  });
});
