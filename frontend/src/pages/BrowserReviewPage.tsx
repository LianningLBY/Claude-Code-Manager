import { useCallback, useEffect, useMemo, useState } from 'react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';

import { api } from '../api/client';
import type {
  BrowserReviewConfig,
  BrowserReviewCreate,
  BrowserReviewJob,
} from '../api/client';
import {
  Activity,
  AlertCircle,
  Clock,
  Download,
  Eye,
  FileText,
  Image,
  Loader2,
  Play,
  RefreshCw,
  Shield,
  Sparkles,
  StopCircle,
} from '../components/icons';


const DEFAULT_GOAL = '审查这个前端页面的视觉布局、交互反馈、明显的可访问性问题，以及控制台和网络错误。按严重程度输出问题、证据和复现步骤。';
const TERMINAL_STATUSES = new Set(['completed', 'failed', 'cancelled']);
const STAGE_LABELS: Record<string, string> = {
  queued: '等待启动',
  waiting_for_agent: '等待 CCM Agent',
  agent_starting: 'CCM Agent 启动中',
  launching_browser: '启动隔离浏览器',
  browser_ready: '页面已打开',
  model_thinking: '模型正在识别页面',
  executing_actions: '执行浏览器动作',
  agent_reported: '报告已保存',
  browser_closed: '浏览器已关闭',
  cancelling: '正在停止',
  completed: '检测完成',
  failed: '检测失败',
  cancelled: '已取消',
};
const STATUS_STYLES: Record<string, string> = {
  queued: 'bg-amber-500/15 text-amber-300 border-amber-500/25',
  running: 'bg-blue-500/15 text-blue-300 border-blue-500/25',
  completed: 'bg-emerald-500/15 text-emerald-300 border-emerald-500/25',
  failed: 'bg-red-500/15 text-red-300 border-red-500/25',
  cancelled: 'bg-gray-500/15 text-gray-300 border-gray-500/25',
};


function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}


export function BrowserReviewPage() {
  const [config, setConfig] = useState<BrowserReviewConfig | null>(null);
  const [history, setHistory] = useState<BrowserReviewJob[]>([]);
  const [job, setJob] = useState<BrowserReviewJob | null>(null);
  const [url, setUrl] = useState('');
  const [goal, setGoal] = useState(DEFAULT_GOAL);
  const [provider, setProvider] = useState<'claude' | 'codex'>('codex');
  const [model, setModel] = useState('gpt-5.6-terra');
  const [effort, setEffort] = useState('medium');
  const [codexServiceTier, setCodexServiceTier] = useState<'default' | 'priority'>('default');
  const [allowActions, setAllowActions] = useState(false);
  const [browserChannel, setBrowserChannel] = useState<'chrome' | 'chromium'>('chrome');
  const [viewport, setViewport] = useState('1440x900');
  const [submitting, setSubmitting] = useState(false);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [screenshotUrl, setScreenshotUrl] = useState<string | null>(null);

  const loadHistory = useCallback(async () => {
    const jobs = await api.listBrowserReviews();
    setHistory(jobs);
    setJob((current) => current ?? jobs[0] ?? null);
  }, []);

  useEffect(() => {
    let active = true;
    Promise.all([api.getBrowserReviewConfig(), api.listBrowserReviews()])
      .then(([nextConfig, jobs]) => {
        if (!active) return;
        setConfig(nextConfig);
        setGoal(nextConfig.default_goal || DEFAULT_GOAL);
        const nextProvider = nextConfig.default_provider || 'codex';
        setProvider(nextProvider);
        setModel(nextConfig.default_models[nextProvider]);
        setEffort(nextConfig.default_effort || 'medium');
        setHistory(jobs);
        setJob(jobs[0] ?? null);
      })
      .catch((nextError) => {
        if (active) setError(errorMessage(nextError));
      });
    return () => { active = false; };
  }, []);

  const availableModels = useMemo(
    () => config?.models_by_provider[provider] || [model],
    [config, model, provider],
  );
  const availableEfforts = useMemo(
    () => config?.model_efforts[provider]?.[model]
      || config?.effort_options[provider]
      || ['medium'],
    [config, model, provider],
  );
  const fastSupported = provider === 'codex'
    && (config?.codex_model_service_tiers[model] || ['default']).includes('priority');

  useEffect(() => {
    if (!config) return;
    if (!availableModels.includes(model)) {
      setModel(config.default_models[provider] || availableModels[0]);
    }
  }, [availableModels, config, model, provider]);

  useEffect(() => {
    if (!availableEfforts.includes(effort)) {
      setEffort(availableEfforts.includes(config?.default_effort || '')
        ? (config?.default_effort || 'medium')
        : availableEfforts[0]);
    }
  }, [availableEfforts, config?.default_effort, effort]);

  useEffect(() => {
    if (!fastSupported && codexServiceTier === 'priority') {
      setCodexServiceTier('default');
    }
  }, [codexServiceTier, fastSupported]);

  const refreshJob = useCallback(async (id: string) => {
    const next = await api.getBrowserReview(id);
    setJob(next);
    setHistory((items) => items.map((item) => item.id === next.id ? next : item));
    return next;
  }, []);

  useEffect(() => {
    if (!job || TERMINAL_STATUSES.has(job.status)) return;
    let active = true;
    const poll = async () => {
      try {
        const next = await api.getBrowserReview(job.id);
        if (!active) return;
        setJob(next);
        setHistory((items) => items.map((item) => item.id === next.id ? next : item));
        if (TERMINAL_STATUSES.has(next.status)) await loadHistory();
      } catch (nextError) {
        if (active) setError(errorMessage(nextError));
      }
    };
    const timer = window.setInterval(poll, 1000);
    return () => {
      active = false;
      window.clearInterval(timer);
    };
  }, [job, loadHistory]);

  useEffect(() => {
    setScreenshotUrl(null);
    if (!job?.latest_screenshot) return;
    let active = true;
    let objectUrl: string | null = null;
    api.getBrowserReviewArtifact(job.id, job.latest_screenshot)
      .then((blob) => {
        objectUrl = URL.createObjectURL(blob);
        if (active) setScreenshotUrl(objectUrl);
        else URL.revokeObjectURL(objectUrl);
      })
      .catch((nextError) => {
        if (active) setError(errorMessage(nextError));
      });
    return () => {
      active = false;
      if (objectUrl) URL.revokeObjectURL(objectUrl);
    };
  }, [job?.id, job?.latest_screenshot]);

  const activeJob = Boolean(job && !TERMINAL_STATUSES.has(job.status));
  const telemetryGroups = useMemo(() => {
    if (!job) return [];
    return Object.entries(job.telemetry || {})
      .filter((entry): entry is [string, Record<string, unknown>[]] => Array.isArray(entry[1]))
      .filter(([, entries]) => entries.length > 0);
  }, [job]);
  const sortedTrace = useMemo(
    () => [...(job?.trace || [])].sort((left, right) => {
      const timestampOrder = (left.timestamp || '').localeCompare(right.timestamp || '');
      return timestampOrder || left.id - right.id;
    }),
    [job?.trace],
  );

  const handleSubmit = async (event: React.FormEvent) => {
    event.preventDefault();
    setSubmitting(true);
    setError(null);
    const [viewportWidth, viewportHeight] = viewport.split('x').map(Number);
    const payload: BrowserReviewCreate = {
      url: url.trim(),
      goal: goal.trim(),
      provider,
      model,
      reasoning_effort: effort,
      codex_service_tier: provider === 'codex' ? codexServiceTier : 'default',
      allow_actions: allowActions,
      browser_channel: browserChannel,
      viewport_width: viewportWidth,
      viewport_height: viewportHeight,
      max_steps: 20,
      max_actions: 60,
    };
    try {
      const created = await api.createBrowserReview(payload);
      setJob(created);
      setHistory((items) => [created, ...items.filter((item) => item.id !== created.id)]);
    } catch (nextError) {
      setError(errorMessage(nextError));
    } finally {
      setSubmitting(false);
    }
  };

  const handleRefresh = async () => {
    setRefreshing(true);
    setError(null);
    try {
      if (job) await refreshJob(job.id);
      await loadHistory();
    } catch (nextError) {
      setError(errorMessage(nextError));
    } finally {
      setRefreshing(false);
    }
  };

  const handleCancel = async () => {
    if (!job) return;
    setError(null);
    try {
      setJob(await api.cancelBrowserReview(job.id));
      await loadHistory();
    } catch (nextError) {
      setError(errorMessage(nextError));
    }
  };

  const downloadArtifact = async (name: string) => {
    if (!job) return;
    try {
      const blob = await api.getBrowserReviewArtifact(job.id, name);
      const objectUrl = URL.createObjectURL(blob);
      const anchor = document.createElement('a');
      anchor.href = objectUrl;
      anchor.download = name;
      anchor.click();
      URL.revokeObjectURL(objectUrl);
    } catch (nextError) {
      setError(errorMessage(nextError));
    }
  };

  return (
    <div className="space-y-5">
      <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
        <div>
          <div className="flex items-center gap-2">
            <Eye size={22} className="text-indigo-400" />
            <h1 className="text-xl font-semibold text-foreground">Browser Review</h1>
            <span className="rounded-full border border-indigo-500/25 bg-indigo-500/10 px-2 py-0.5 text-[10px] font-medium uppercase tracking-wide text-indigo-300">Demo</span>
          </div>
          <p className="mt-1 text-sm text-gray-400">
            在 CCM 服务器拉起隔离的真实浏览器，记录模型识别、操作、截图以及前端运行错误。
          </p>
        </div>
        <button
          type="button"
          onClick={handleRefresh}
          disabled={refreshing}
          className="inline-flex items-center justify-center gap-2 rounded-lg border border-gray-700 bg-gray-800 px-3 py-2 text-sm text-gray-300 hover:bg-gray-700 disabled:opacity-50"
        >
          <RefreshCw size={15} className={refreshing ? 'animate-spin' : ''} />
          刷新
        </button>
      </div>

      {error && (
        <div role="alert" className="flex items-start gap-2 rounded-lg border border-red-500/30 bg-red-500/10 px-4 py-3 text-sm text-red-300">
          <AlertCircle size={17} className="mt-0.5 shrink-0" />
          <span className="break-words">{error}</span>
        </div>
      )}

      {config && (
        <div className="flex items-start gap-2 rounded-lg border border-indigo-500/25 bg-indigo-500/10 px-4 py-3 text-sm text-indigo-200">
          <Shield size={17} className="mt-0.5 shrink-0" />
          <span>检测会创建普通 CCM Task，复用现有 Claude/Codex 账号池；浏览器能力由任务专属 MCP 提供，不需要额外配置 OpenAI API Key。</span>
        </div>
      )}

      <form onSubmit={handleSubmit} className="rounded-xl border border-gray-800 bg-gray-950/55 p-4 sm:p-5">
        <div className="grid gap-4 lg:grid-cols-12">
          <div className="lg:col-span-7">
            <label htmlFor="browser-review-url" className="mb-1.5 block text-xs font-medium text-gray-300">待检测网站</label>
            <input
              id="browser-review-url"
              type="url"
              required
              value={url}
              onChange={(event) => setUrl(event.target.value)}
              placeholder="https://example.com 或 http://127.0.0.1:5173"
              className="w-full rounded-lg border border-gray-700 bg-gray-900 px-3 py-2.5 text-sm text-foreground outline-none placeholder:text-gray-600 focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500"
            />
          </div>
          <div className="grid grid-cols-2 gap-3 lg:col-span-5">
            <div>
              <label htmlFor="browser-review-viewport" className="mb-1.5 block text-xs font-medium text-gray-300">视口</label>
              <select id="browser-review-viewport" value={viewport} onChange={(event) => setViewport(event.target.value)} className="w-full rounded-lg border border-gray-700 bg-gray-900 px-3 py-2.5 text-sm text-foreground outline-none focus:border-indigo-500">
                <option value="1440x900">桌面 1440×900</option>
                <option value="1280x720">桌面 1280×720</option>
                <option value="768x1024">平板 768×1024</option>
              </select>
            </div>
            <div>
              <label htmlFor="browser-review-channel" className="mb-1.5 block text-xs font-medium text-gray-300">浏览器</label>
              <select id="browser-review-channel" value={browserChannel} onChange={(event) => setBrowserChannel(event.target.value as 'chrome' | 'chromium')} className="w-full rounded-lg border border-gray-700 bg-gray-900 px-3 py-2.5 text-sm text-foreground outline-none focus:border-indigo-500">
                <option value="chrome">系统 Chrome</option>
                <option value="chromium">Playwright Chromium</option>
              </select>
            </div>
          </div>
          <div className="lg:col-span-12">
            <label htmlFor="browser-review-goal" className="mb-1.5 block text-xs font-medium text-gray-300">审查目标</label>
            <textarea
              id="browser-review-goal"
              required
              rows={3}
              value={goal}
              onChange={(event) => setGoal(event.target.value)}
              className="w-full resize-y rounded-lg border border-gray-700 bg-gray-900 px-3 py-2.5 text-sm text-foreground outline-none focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500"
            />
          </div>
          <div className="grid gap-3 sm:grid-cols-2 lg:col-span-9 lg:grid-cols-5">
            <div>
              <label htmlFor="browser-review-provider" className="mb-1.5 block text-xs font-medium text-gray-300">Provider</label>
              <select
                id="browser-review-provider"
                value={provider}
                onChange={(event) => {
                  const nextProvider = event.target.value as 'claude' | 'codex';
                  setProvider(nextProvider);
                  if (config) setModel(config.default_models[nextProvider]);
                  if (nextProvider !== 'codex') setCodexServiceTier('default');
                }}
                className="w-full rounded-lg border border-gray-700 bg-gray-900 px-3 py-2 text-sm text-foreground outline-none"
              >
                {(config?.providers || ['codex', 'claude']).map((item) => <option key={item} value={item}>{item === 'codex' ? 'Codex' : 'Claude'}</option>)}
              </select>
            </div>
            <div>
              <label htmlFor="browser-review-model" className="mb-1.5 block text-xs font-medium text-gray-300">识别模型</label>
              <select id="browser-review-model" value={model} onChange={(event) => setModel(event.target.value)} className="w-full rounded-lg border border-gray-700 bg-gray-900 px-3 py-2 text-sm text-foreground outline-none">
                {availableModels.map((item) => <option key={item} value={item}>{item}</option>)}
              </select>
            </div>
            <div>
              <label htmlFor="browser-review-effort" className="mb-1.5 block text-xs font-medium text-gray-300">思考强度</label>
              <select id="browser-review-effort" value={effort} onChange={(event) => setEffort(event.target.value)} className="w-full rounded-lg border border-gray-700 bg-gray-900 px-3 py-2 text-sm text-foreground outline-none">
                {availableEfforts.map((item) => <option key={item} value={item}>{item}</option>)}
              </select>
            </div>
            <div>
              <label htmlFor="browser-review-tier" className="mb-1.5 block text-xs font-medium text-gray-300">Codex 速度</label>
              <select id="browser-review-tier" value={codexServiceTier} disabled={provider !== 'codex'} onChange={(event) => setCodexServiceTier(event.target.value as 'default' | 'priority')} className="w-full rounded-lg border border-gray-700 bg-gray-900 px-3 py-2 text-sm text-foreground outline-none disabled:opacity-50">
                <option value="default">Standard</option>
                <option value="priority" disabled={!fastSupported}>Fast</option>
              </select>
            </div>
            <div className="flex items-end">
              <button type="submit" disabled={submitting || activeJob || !url.trim()} className="inline-flex w-full items-center justify-center gap-2 rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-500 disabled:cursor-not-allowed disabled:opacity-45">
                {submitting ? <Loader2 size={16} className="animate-spin" /> : <Play size={16} />}
                开始检测
              </button>
            </div>
          </div>
          <div className="flex flex-wrap items-end gap-x-5 gap-y-2 lg:col-span-3 lg:justify-end">
            <label className="flex cursor-pointer items-center gap-2 text-sm text-gray-300">
              <input type="checkbox" checked={allowActions} onChange={(event) => setAllowActions(event.target.checked)} className="rounded border-gray-600 bg-gray-800 text-indigo-500" />
              允许点击和输入
            </label>
          </div>
        </div>
        <p className="mt-3 text-xs text-gray-500">
          默认只允许截图、等待、移动和滚动。开启交互后也会阻止跨域顶层跳转、弹窗和下载，请不要对含敏感账号或生产写操作的页面启用。
        </p>
      </form>

      {job && (
        <div className="grid gap-5 xl:grid-cols-[minmax(0,1.65fr)_minmax(300px,1fr)]">
          <section className="overflow-hidden rounded-xl border border-gray-800 bg-gray-950/55">
            <div className="flex flex-wrap items-center justify-between gap-3 border-b border-gray-800 px-4 py-3">
              <div className="min-w-0">
                <div className="flex items-center gap-2">
                  <span className={`rounded-full border px-2 py-0.5 text-xs ${STATUS_STYLES[job.status] || STATUS_STYLES.queued}`}>{STAGE_LABELS[job.stage] || job.stage}</span>
                  {activeJob && <Loader2 size={14} className="animate-spin text-blue-400" />}
                </div>
                <p className="mt-1 truncate text-xs text-gray-500" title={job.url}>{job.url}</p>
                {job.task_id && <a href={`#/tasks/chat/${job.task_id}`} className="mt-1 inline-block text-xs text-indigo-400 hover:text-indigo-300">查看 CCM Task #{job.task_id}</a>}
              </div>
              <div className="flex items-center gap-2">
                <span className="text-xs text-gray-500">{job.steps} 步 / {job.actions} 个动作</span>
                {activeJob && (
                  <button type="button" onClick={handleCancel} className="inline-flex items-center gap-1.5 rounded-md border border-red-500/30 px-2.5 py-1.5 text-xs text-red-300 hover:bg-red-500/10">
                    <StopCircle size={14} />停止
                  </button>
                )}
              </div>
            </div>
            <div className="relative flex min-h-72 items-center justify-center bg-black/60 p-3">
              {screenshotUrl ? (
                <img src={screenshotUrl} alt={`浏览器检测截图 ${job.latest_screenshot || ''}`} className="max-h-[650px] w-full rounded border border-gray-800 object-contain" />
              ) : (
                <div className="flex flex-col items-center gap-2 text-gray-600">
                  {activeJob ? <Loader2 size={26} className="animate-spin" /> : <Image size={28} />}
                  <span className="text-sm">{activeJob ? '正在等待第一张截图…' : '没有可用截图'}</span>
                </div>
              )}
            </div>
            {job.artifacts.length > 0 && (
              <div className="flex flex-wrap items-center gap-2 border-t border-gray-800 px-4 py-3">
                <span className="mr-1 text-xs text-gray-500">产物</span>
                {job.artifacts.map((name) => (
                  <button key={name} type="button" onClick={() => downloadArtifact(name)} className="inline-flex items-center gap-1 rounded-md bg-gray-800 px-2 py-1 text-xs text-gray-400 hover:text-gray-200">
                    <Download size={12} />{name}
                  </button>
                ))}
              </div>
            )}
          </section>

          <aside className="space-y-5">
            <section className="rounded-xl border border-gray-800 bg-gray-950/55 p-4">
              <div className="mb-1 flex items-center gap-2 text-sm font-medium text-gray-200"><Sparkles size={16} className="text-indigo-400" />模型轨迹</div>
              <p className="mb-4 text-[11px] leading-4 text-gray-600">展示模型主动输出的观察与决策摘要，以及随后执行的浏览器工具；不包含内部隐藏推理。</p>
              {sortedTrace.length ? (
                <div className="max-h-[34rem] overflow-y-auto pr-1">
                  <div className="relative ml-2 border-l border-gray-800 pl-5">
                    {sortedTrace.map((event) => (
                      <div key={event.id} className="relative mb-4 last:mb-0">
                        <div className={`absolute -left-[27px] top-0.5 flex h-3.5 w-3.5 items-center justify-center rounded-full border ${event.kind === 'decision' ? 'border-indigo-400/60 bg-indigo-500/25' : 'border-cyan-400/50 bg-cyan-500/20'}`}>
                          <span className={`h-1.5 w-1.5 rounded-full ${event.kind === 'decision' ? 'bg-indigo-300' : 'bg-cyan-300'}`} />
                        </div>
                        <div className="rounded-lg border border-gray-800 bg-gray-900/75 p-3">
                          <div className="flex items-start justify-between gap-3">
                            <div className="flex min-w-0 items-center gap-2">
                              {event.kind === 'decision' ? <Sparkles size={13} className="shrink-0 text-indigo-400" /> : <Activity size={13} className="shrink-0 text-cyan-400" />}
                              <span className="truncate text-xs font-medium text-gray-300">{event.title}</span>
                            </div>
                            {event.timestamp && <span className="shrink-0 text-[10px] text-gray-700">{new Date(event.timestamp).toLocaleTimeString()}</span>}
                          </div>
                          {event.tool_name && <code className="mt-1.5 block text-[10px] text-cyan-500/70">{event.tool_name}</code>}
                          {event.detail && <p className="mt-2 whitespace-pre-wrap break-words text-[11px] leading-5 text-gray-500">{event.detail}</p>}
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              ) : <p className="text-xs text-gray-600">Agent 启动后，公开的观察摘要和浏览器决策会依次显示在这里。</p>}
              {job.action_batches.length > 0 && (
                <details className="mt-4 border-t border-gray-800 pt-3">
                  <summary className="cursor-pointer text-[11px] text-gray-600 hover:text-gray-400">查看原始动作数据（{job.action_batches.length}）</summary>
                  <div className="mt-3 max-h-60 space-y-2 overflow-y-auto">
                    {job.action_batches.map((batch, index) => (
                      <pre key={`${batch.step}-${index}`} className="overflow-x-auto whitespace-pre-wrap break-words rounded-md bg-black/25 p-2 text-[10px] leading-4 text-gray-600">{JSON.stringify(batch.actions, null, 2)}</pre>
                    ))}
                  </div>
                </details>
              )}
            </section>

            <section className="rounded-xl border border-gray-800 bg-gray-950/55 p-4">
              <div className="mb-3 flex items-center gap-2 text-sm font-medium text-gray-200"><AlertCircle size={16} className="text-amber-400" />浏览器错误</div>
              {telemetryGroups.length ? (
                <div className="max-h-72 space-y-3 overflow-y-auto pr-1">
                  {telemetryGroups.map(([name, entries]) => (
                    <div key={name}>
                      <div className="mb-1 text-xs font-medium text-gray-400">{name} ({entries.length})</div>
                      {entries.slice(-10).map((entry, index) => <pre key={index} className="mb-1 whitespace-pre-wrap break-all rounded bg-gray-900 px-2 py-1.5 text-[10px] leading-4 text-gray-500">{JSON.stringify(entry)}</pre>)}
                    </div>
                  ))}
                </div>
              ) : <p className="text-xs text-gray-600">当前没有捕获到控制台、页面、网络或 HTTP 错误。</p>}
            </section>
          </aside>
        </div>
      )}

      {job?.error && (
        <div className="rounded-xl border border-red-500/25 bg-red-500/10 p-4 text-sm text-red-300">
          <div className="mb-1 font-medium">检测失败</div>
          <div className="whitespace-pre-wrap break-words text-xs">{job.error}</div>
        </div>
      )}

      {job?.report && (
        <section className="rounded-xl border border-gray-800 bg-gray-950/55 p-4 sm:p-5">
          <div className="mb-4 flex items-center gap-2 text-sm font-medium text-gray-200"><FileText size={17} className="text-emerald-400" />审查报告</div>
          <div className="prose prose-sm max-w-none prose-invert overflow-x-auto text-sm text-gray-300">
            <ReactMarkdown remarkPlugins={[remarkGfm]}>{job.report}</ReactMarkdown>
          </div>
        </section>
      )}

      <section className="rounded-xl border border-gray-800 bg-gray-950/55 p-4">
        <div className="mb-3 flex items-center gap-2 text-sm font-medium text-gray-200"><Clock size={16} className="text-gray-400" />本进程检测记录</div>
        {history.length ? (
          <div className="divide-y divide-gray-800">
            {history.map((item) => (
              <button key={item.id} type="button" onClick={() => setJob(item)} className={`flex w-full items-center justify-between gap-3 py-3 text-left ${job?.id === item.id ? 'text-indigo-300' : 'text-gray-400 hover:text-gray-200'}`}>
                <div className="min-w-0">
                  <div className="truncate text-sm">{item.url}</div>
                  <div className="mt-0.5 text-xs text-gray-600">{new Date(item.created_at).toLocaleString()} · {item.provider} / {item.model}</div>
                </div>
                <span className={`shrink-0 rounded-full border px-2 py-0.5 text-[11px] ${STATUS_STYLES[item.status] || STATUS_STYLES.queued}`}>{STAGE_LABELS[item.stage] || item.status}</span>
              </button>
            ))}
          </div>
        ) : <p className="text-xs text-gray-600">服务重启后记录会清空。输入网站并开始第一次检测即可看到完整过程。</p>}
      </section>
    </div>
  );
}
