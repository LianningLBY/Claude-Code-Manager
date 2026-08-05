import { useState, useEffect, useCallback } from 'react';
import { api } from '../api/client';
import type { MonitoredRepo, PRFinding, PRMonitorRun, PRReview, RequiredCheckPolicy } from '../api/client';
import { Plus, ArrowLeft, X, Copy, RefreshCw, ToggleLeft, ToggleRight, Trash2, GitPullRequest, Check } from '../components/icons';
import { FindingActions } from '../components/PRReview/FindingActions';

const DEFAULT_WEBHOOK_URL = `${window.location.origin}/api/github/webhook`;
const FINDING_SEVERITY_ORDER: Record<string, number> = { critical: 0, high: 1, medium: 2, low: 3 };

function parseRequiredChecks(value: string): RequiredCheckPolicy[] {
  return value.split('\n').map((line) => line.trim()).filter(Boolean).map((line) => {
    const [kind, name, appSlug, ...extra] = line.split(',').map((part) => part.trim());
    if (extra.length || !name || !appSlug || (kind !== 'check_run' && kind !== 'status')) {
      throw new Error('Required CI 每行格式必须是：check_run,检查名,GitHub App slug（或 status,context,机器人登录名）');
    }
    return { kind: kind as RequiredCheckPolicy['kind'], name, app_slug: appSlug };
  });
}

function renderRequiredChecks(repo: MonitoredRepo) {
  return (repo.required_checks || []).map((item) => `${item.kind},${item.name},${item.app_slug}`).join('\n');
}

function useProviderModels(): {
  providers: string[];
  defaultProvider: string;
  modelsFor: (p: string) => string[];
  effortsFor: (p: string, model: string) => string[];
} {
  const [cfg, setCfg] = useState<{
    providers: string[];
    defaultProvider: string;
    defaultClaudeModel: string;
    defaultCodexModel: string;
    claude: string[];
    codex: string[];
    claudeEfforts: Record<string, string[]>;
    codexEfforts: Record<string, string[]>;
    defaultEfforts: string[];
    codexDefaultEfforts: string[];
  }>({
    providers: ['claude', 'codex'], defaultProvider: 'codex',
    defaultClaudeModel: '', defaultCodexModel: '',
    claude: [], codex: [], claudeEfforts: {}, codexEfforts: {},
    defaultEfforts: [], codexDefaultEfforts: [],
  });
  useEffect(() => {
    api.config().then((c) => setCfg({
      providers: c.provider_options?.length ? c.provider_options : ['claude', 'codex'],
      defaultProvider: c.default_provider || 'codex',
      defaultClaudeModel: c.default_model,
      defaultCodexModel: c.default_codex_model,
      claude: c.model_options.filter((m) => m !== 'default'),
      codex: (c.codex_model_options || []).filter((m) => m !== 'default'),
      claudeEfforts: c.claude_model_efforts || {},
      codexEfforts: c.codex_model_efforts || {},
      defaultEfforts: c.effort_options || [],
      codexDefaultEfforts: c.codex_effort_options || [],
    })).catch(() => {});
  }, []);
  return {
    providers: cfg.providers,
    defaultProvider: cfg.defaultProvider,
    modelsFor: (p: string) => (p === 'codex' ? cfg.codex : cfg.claude),
    effortsFor: (p: string, model: string) => {
      const effectiveModel = model || (p === 'codex' ? cfg.defaultCodexModel : cfg.defaultClaudeModel);
      const mapped = (p === 'codex' ? cfg.codexEfforts : cfg.claudeEfforts)[effectiveModel];
      return mapped?.length
        ? mapped
        : (p === 'codex' ? cfg.codexDefaultEfforts : cfg.defaultEfforts);
    },
  };
}

const STATUS_COLORS: Record<string, string> = {
  pending: 'bg-yellow-500/20 text-yellow-400',
  waiting_ci: 'bg-yellow-500/20 text-yellow-400',
  reviewing: 'bg-blue-500/20 text-blue-400',
  passed: 'bg-green-500/20 text-green-400',
  changes_required: 'bg-orange-500/20 text-orange-400',
  merged: 'bg-green-500/20 text-green-400',
  approved: 'bg-green-500/20 text-green-400',
  commented: 'bg-orange-500/20 text-orange-400',
  error: 'bg-red-500/20 text-red-400',
  superseded: 'bg-gray-500/20 text-gray-400',
};

const TERMINAL_RUN_STATUSES = new Set(['merged', 'closed']);
const READY_RUN_STATUSES = new Set(['ready_to_merge', 'merge_group_passed']);
const BUSY_RUN_STATUSES = new Set([
  'adjudicating',
  'repair_migrating',
  'repairing',
  'resolving_fixed_threads',
  'merge_queued',
  'merge_group_checking',
]);
const ACTIVE_REVIEW_STATUSES = new Set([
  'pending',
  'waiting_ci',
  'reviewing',
  'publishing',
  'superseding',
]);
const ACTIVE_PUBLICATION_STATUSES = new Set(['publishing', 'superseding']);
const STARTED_REPAIR_STATUSES = new Set(['delivering', 'accepted', 'awaiting_push', 'running']);
const STARTED_MERGE_STATUSES = new Set(['enqueuing', 'queued', 'checking']);
const ACTIVE_ADJUDICATION_STATUSES = new Set(['pending', 'adjudicating', 'accepted']);

function copyToClipboard(text: string) {
  navigator.clipboard.writeText(text);
}

function FindingRebuttalForm({ finding, onSubmitted }: { finding: PRFinding; onSubmitted: () => Promise<void> }) {
  const [evidence, setEvidence] = useState('');
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const active = finding.rebuttals?.some(item => ['pending', 'adjudicating', 'accepted'].includes(item.status));
  if (finding.status !== 'open' || finding.severity === 'low') return null;
  return (
    <div className="mt-2 space-y-1">
      {finding.rebuttals?.map(item => (
        <p key={item.id} className="text-gray-500">Rebuttal #{item.attempt}: {item.status}{item.result_body ? ` · ${item.result_body}` : ''}</p>
      ))}
      <textarea value={evidence} onChange={(event) => setEvidence(event.target.value)}
        disabled={active || submitting} rows={3} placeholder="Concrete code/test/policy evidence for this exact head"
        className="w-full bg-gray-700 rounded px-2 py-1 text-xs" />
      {error && <p className="text-red-400">{error}</p>}
      <button disabled={active || submitting || evidence.trim().length < 20}
        className="bg-indigo-600 text-white rounded px-2 py-1 disabled:opacity-50"
        onClick={async () => {
          setSubmitting(true); setError(null);
          try { await api.submitPRFindingRebuttal(finding.id, evidence.trim()); setEvidence(''); await onSubmitted(); }
          catch (caught) { setError(String(caught)); }
          finally { setSubmitting(false); }
        }}>{active ? 'Adjudicating…' : 'Submit rebuttal'}</button>
    </div>
  );
}

function AddRepoModal({ onClose, onSaved }: { onClose: () => void; onSaved: () => void }) {
  const [repoName, setRepoName] = useState('');
  const [autoMerge, setAutoMerge] = useState(false);
  const [autoRepair, setAutoRepair] = useState(false);
  const [mergeQueueMode, setMergeQueueMode] = useState<'manual' | 'shadow' | 'auto'>('manual');
  const [reviewMode, setReviewMode] = useState<'single' | 'panel'>('panel');
  const [waitForCi, setWaitForCi] = useState(true);
  const [requiredChecks, setRequiredChecks] = useState('');
  const [provider, setProvider] = useState('codex');
  const [reviewModel, setReviewModel] = useState('');
  const [reviewEffort, setReviewEffort] = useState('');
  const { providers, defaultProvider, modelsFor, effortsFor } = useProviderModels();
  const modelOptions = modelsFor(provider);
  const effortOptions = effortsFor(provider, reviewModel);
  const [defaultBranch, setDefaultBranch] = useState('main');
  const [allowedAuthors, setAllowedAuthors] = useState('');
  const [workerId, setWorkerId] = useState('');
  const [workers, setWorkers] = useState<{ id: number; name: string }[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const ccUser = JSON.parse(localStorage.getItem('cc_user') || '{}');
  const isAdmin = ccUser.role === 'admin' || ccUser.role === 'super_admin' || !ccUser.id;

  useEffect(() => {
    api.listWorkers().then(w => setWorkers(w.filter(wk => wk.status !== 'terminated'))).catch(() => {});
  }, []);

  useEffect(() => {
    setProvider(defaultProvider);
  }, [defaultProvider]);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setSubmitting(true);
    setError(null);
    try {
      const authors = allowedAuthors.trim() ? allowedAuthors.split(',').map(a => a.trim()).filter(Boolean) : [];
      const checks = reviewMode === 'panel' ? parseRequiredChecks(requiredChecks) : [];
      if (reviewMode === 'panel' && waitForCi && checks.length === 0) {
        throw new Error('启用 CI Gate 时至少配置一个 required check');
      }
      await api.createMonitoredRepo({
        repo_full_name: repoName.trim(),
        auto_merge: reviewMode === 'single' && autoMerge,
        auto_repair: reviewMode === 'panel' && autoRepair,
        max_repair_attempts: 3,
        merge_queue_mode: reviewMode === 'panel' && waitForCi ? mergeQueueMode : 'manual',
        provider,
        review_model: reviewModel.trim() || undefined,
        review_effort: reviewEffort || undefined,
        review_mode: reviewMode,
        wait_for_ci: reviewMode === 'panel' && waitForCi,
        required_checks: reviewMode === 'panel' ? checks : [],
        default_branch: defaultBranch.trim() || 'main',
        allowed_authors: authors,
        worker_id: workerId ? Number(workerId) : undefined,
      });
      onSaved();
      onClose();
    } catch (e) {
      setError(String(e));
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="fixed inset-0 bg-black/60 flex items-center justify-center z-50 p-4">
      <div className="bg-gray-800 rounded-xl shadow-2xl w-full max-w-md">
        <div className="flex items-center justify-between px-5 py-4 border-b border-gray-700">
          <h3 className="text-foreground font-semibold">Add Repository</h3>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-200"><X size={18} /></button>
        </div>
        <form onSubmit={handleSubmit} className="p-5 space-y-4">
          {error && <p className="text-red-400 text-sm">{error}</p>}
          <div>
            <label className="block text-xs text-gray-400 mb-1">Repository (owner/repo)</label>
            <input
              className="w-full bg-gray-700 text-foreground text-sm rounded px-3 py-2 outline-none focus:ring-1 focus:ring-indigo-500"
              value={repoName} onChange={(e) => setRepoName(e.target.value)}
              placeholder="owner/repo" required
            />
          </div>
          <div className="flex items-center gap-2">
            <input type="checkbox" id="autoMerge" checked={reviewMode === 'single' && autoMerge}
              disabled={reviewMode !== 'single'} onChange={(e) => { setAutoMerge(e.target.checked); if (e.target.checked) setMergeQueueMode('manual'); }}
              className="rounded bg-gray-700 border-gray-600" />
            <label htmlFor="autoMerge" className="text-sm text-gray-300">Legacy auto-merge (single reviewer only)</label>
          </div>
          <div>
            <label className="block text-xs text-gray-400 mb-1">Merge Queue</label>
            <select value={mergeQueueMode} onChange={(e) => { const value = e.target.value as 'manual' | 'shadow' | 'auto'; setMergeQueueMode(value); if (value !== 'manual') setAutoMerge(false); }}
              disabled={reviewMode !== 'panel' || !waitForCi}
              className="w-full bg-gray-700 text-foreground text-sm rounded px-3 py-2">
              <option value="manual">Manual</option><option value="shadow">Shadow only</option><option value="auto">Automatic enqueue</option>
            </select>
          </div>
          <div className="flex items-center gap-2">
            <input type="checkbox" id="autoRepair" checked={autoRepair}
              disabled={reviewMode !== 'panel'} onChange={(e) => setAutoRepair(e.target.checked)} />
            <label htmlFor="autoRepair" className="text-sm text-gray-300">Auto-resume bound local Developer Task (max 3 heads)</label>
          </div>
          {reviewMode === 'panel' && waitForCi && (
            <div>
              <label className="block text-xs text-gray-400 mb-1">Required CI identities（每行一个）</label>
              <textarea className="w-full bg-gray-700 text-foreground text-xs rounded px-3 py-2 font-mono"
                rows={3} value={requiredChecks} onChange={(e) => setRequiredChecks(e.target.value)}
                placeholder={'check_run,tests,github-actions\nstatus,lint,ci-bot'} required />
              <p className="text-xs text-gray-500 mt-1">精确匹配当前 commit 的检查名和发布者身份，避免同名假绿。</p>
            </div>
          )}
          <div>
            <label className="block text-xs text-gray-400 mb-1">Review Harness</label>
            <select className="w-full bg-gray-700 text-foreground text-sm rounded px-3 py-2"
              value={reviewMode} onChange={(e) => {
                const value = e.target.value as 'single' | 'panel';
                setReviewMode(value);
                if (value === 'panel') setAutoMerge(false);
                else { setAutoRepair(false); setWaitForCi(false); setMergeQueueMode('manual'); }
              }}>
              <option value="panel">Independent Principal / Senior / QA panel</option>
              <option value="single">Legacy single reviewer</option>
            </select>
          </div>
          <div className="flex items-center gap-2">
            <input type="checkbox" id="newWaitForCi" checked={waitForCi}
              disabled={reviewMode !== 'panel'} onChange={(e) => {
                setWaitForCi(e.target.checked);
                if (!e.target.checked) setMergeQueueMode('manual');
              }} />
            <label htmlFor="newWaitForCi" className="text-sm text-gray-300">Wait for exact-head CI</label>
          </div>
          <div>
            <label className="block text-xs text-gray-400 mb-1">Provider</label>
            <select
              className="w-full bg-gray-700 text-foreground text-sm rounded px-3 py-2 outline-none focus:ring-1 focus:ring-indigo-500"
              value={provider} onChange={(e) => { setProvider(e.target.value); setReviewModel(''); setReviewEffort(''); }}
            >
              {providers.map((p) => <option key={p} value={p}>{p === 'codex' ? 'Codex' : 'Claude Code'}</option>)}
            </select>
          </div>
          <div>
            <label className="block text-xs text-gray-400 mb-1">Review Model (optional)</label>
            <select
              className="w-full bg-gray-700 text-foreground text-sm rounded px-3 py-2 outline-none focus:ring-1 focus:ring-indigo-500"
              value={reviewModel} onChange={(e) => { setReviewModel(e.target.value); setReviewEffort(''); }}
            >
              <option value="">default</option>
              {modelOptions.map((m) => <option key={m} value={m}>{m}</option>)}
            </select>
          </div>
          <div>
            <label className="block text-xs text-gray-400 mb-1">Review Effort (optional)</label>
            <select
              className="w-full bg-gray-700 text-foreground text-sm rounded px-3 py-2 outline-none focus:ring-1 focus:ring-indigo-500"
              value={reviewEffort} onChange={(e) => setReviewEffort(e.target.value)}
            >
              <option value="">default</option>
              {effortOptions.map((effort) => <option key={effort} value={effort}>{effort}</option>)}
            </select>
          </div>
          <div>
            <label className="block text-xs text-gray-400 mb-1">Default Branch</label>
            <input
              className="w-full bg-gray-700 text-foreground text-sm rounded px-3 py-2 outline-none focus:ring-1 focus:ring-indigo-500"
              value={defaultBranch} onChange={(e) => setDefaultBranch(e.target.value)}
            />
          </div>
          <div>
            <label className="block text-xs text-gray-400 mb-1">Run on</label>
            <select
              className="w-full bg-gray-700 text-foreground text-sm rounded px-3 py-2 outline-none focus:ring-1 focus:ring-indigo-500"
              value={workerId} onChange={(e) => setWorkerId(e.target.value)}
            >
              {isAdmin && <option value="">本机</option>}
              {workers.map(w => <option key={w.id} value={w.id}>{w.name}</option>)}
            </select>
          </div>
          <div>
            <label className="block text-xs text-gray-400 mb-1">Allowed Authors (comma-separated, empty = all)</label>
            <input
              className="w-full bg-gray-700 text-foreground text-sm rounded px-3 py-2 outline-none focus:ring-1 focus:ring-indigo-500"
              value={allowedAuthors} onChange={(e) => setAllowedAuthors(e.target.value)}
              placeholder="user1, user2"
            />
          </div>
          <div className="flex justify-end gap-2 pt-1">
            <button type="button" onClick={onClose} className="px-4 py-2 text-sm text-gray-300 hover:text-foreground">Cancel</button>
            <button type="submit" disabled={submitting || !repoName.trim()}
              className="px-4 py-2 text-sm bg-indigo-600 text-white rounded hover:bg-indigo-500 disabled:opacity-50">
              {submitting ? 'Adding...' : 'Add'}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}

function RepoDetail({ repo, onBack, onRefresh }: { repo: MonitoredRepo; onBack: () => void; onRefresh: () => void }) {
  const initialReviewMode = repo.review_mode || 'single';
  const initialWaitForCi = initialReviewMode === 'panel' && Boolean(repo.wait_for_ci);
  const [detail, setDetail] = useState<MonitoredRepo>(repo);
  const [reviews, setReviews] = useState<PRReview[]>([]);
  const [page, setPage] = useState(1);
  const [autoMerge, setAutoMerge] = useState(initialReviewMode === 'single' && repo.auto_merge);
  const [autoRepair, setAutoRepair] = useState(initialReviewMode === 'panel' && Boolean(repo.auto_repair));
  const [maxRepairAttempts, setMaxRepairAttempts] = useState(repo.max_repair_attempts || 3);
  const [mergeQueueMode, setMergeQueueMode] = useState<'manual' | 'shadow' | 'auto'>(
    initialWaitForCi ? (repo.merge_queue_mode || 'manual') : 'manual',
  );
  const [provider, setProvider] = useState(repo.provider || 'claude');
  const [reviewModel, setReviewModel] = useState(repo.review_model || '');
  const [reviewEffort, setReviewEffort] = useState(repo.review_effort || '');
  const [reviewMode, setReviewMode] = useState<'single' | 'panel'>(initialReviewMode);
  const [waitForCi, setWaitForCi] = useState(initialWaitForCi);
  const [requiredChecks, setRequiredChecks] = useState(renderRequiredChecks(repo));
  const [selectedReview, setSelectedReview] = useState<PRReview | null>(null);
  const [monitorRun, setMonitorRun] = useState<PRMonitorRun | null>(null);
  const [developerTaskId, setDeveloperTaskId] = useState('');
  const { providers, modelsFor, effortsFor } = useProviderModels();
  const modelOptions = modelsFor(provider);
  const effortOptions = effortsFor(provider, reviewModel);
  const [defaultBranch, setDefaultBranch] = useState(repo.default_branch);
  const [authorsInput, setAuthorsInput] = useState((repo.allowed_authors || []).join(', '));
  const [saving, setSaving] = useState(false);
  const [copied, setCopied] = useState<string | null>(null);
  const [webhookUrl, setWebhookUrl] = useState(DEFAULT_WEBHOOK_URL);
  const [saveError, setSaveError] = useState<string | null>(null);
  const [runActionError, setRunActionError] = useState<string | null>(null);
  const [runActionPending, setRunActionPending] = useState<string | null>(null);

  useEffect(() => {
    api.getWebhookInfo()
      .then(info => setWebhookUrl(info.webhook_url || DEFAULT_WEBHOOK_URL))
      .catch(() => setWebhookUrl(DEFAULT_WEBHOOK_URL));
  }, []);

  const loadDetail = useCallback(async () => {
    try {
      const d = await api.updateMonitoredRepo(repo.id, {});
      setDetail(d);
    } catch (error) { setSaveError(String(error)); }
  }, [repo.id]);

  const loadReviews = useCallback(async () => {
    try {
      const r = await api.getRepoReviews(repo.id, page);
      setReviews(r);
    } catch (error) { setSaveError(String(error)); }
  }, [repo.id, page]);

  useEffect(() => { loadDetail(); loadReviews(); }, [loadDetail, loadReviews]);

  const handleSave = async () => {
    setSaving(true);
    setSaveError(null);
    try {
      const authors = authorsInput.trim() ? authorsInput.split(',').map(a => a.trim()).filter(Boolean) : [];
      const checks = reviewMode === 'panel' ? parseRequiredChecks(requiredChecks) : [];
      if (reviewMode === 'panel' && waitForCi && checks.length === 0) {
        throw new Error('启用 CI Gate 时至少配置一个 required check');
      }
      const updated = await api.updateMonitoredRepo(repo.id, {
        auto_merge: reviewMode === 'single' && autoMerge,
        auto_repair: reviewMode === 'panel' && autoRepair,
        max_repair_attempts: maxRepairAttempts,
        merge_queue_mode: reviewMode === 'panel' && waitForCi ? mergeQueueMode : 'manual',
        provider,
        // 显式 null 才能清空（undefined 会被后端 exclude_unset 丢弃，
        // 换 provider 后旧模型残留会让 CLI 拿到错家族的 --model）
        review_model: reviewModel.trim() ? reviewModel.trim() : null,
        review_effort: reviewEffort || null,
        review_mode: reviewMode,
        wait_for_ci: reviewMode === 'panel' && waitForCi,
        required_checks: reviewMode === 'panel' ? checks : [],
        default_branch: defaultBranch.trim() || 'main',
        allowed_authors: authors,
      });
      setDetail(updated);
      setRequiredChecks(renderRequiredChecks(updated));
      onRefresh();
    } catch (error) { setSaveError(String(error)); }
    setSaving(false);
  };

  const handleRegenerate = async () => {
    if (!confirm('Regenerate webhook secret? You will need to update the GitHub webhook config.')) return;
    try {
      const updated = await api.regenerateSecret(repo.id);
      setDetail(updated);
      setSaveError(null);
    } catch (error) { setSaveError(String(error)); }
  };

  const performRunAction = async (
    action: string,
    operation: () => Promise<PRMonitorRun>,
  ) => {
    setRunActionPending(action);
    setRunActionError(null);
    try {
      setMonitorRun(await operation());
    } catch (error) {
      setRunActionError(String(error));
    } finally {
      setRunActionPending(null);
    }
  };

  const reviewerRuns = selectedReview?.reviewer_runs ?? [];
  const terminalRun = monitorRun ? TERMINAL_RUN_STATUSES.has(monitorRun.status) : false;
  const readyRun = monitorRun ? READY_RUN_STATUSES.has(monitorRun.status) : false;
  const busyRun = monitorRun ? BUSY_RUN_STATUSES.has(monitorRun.status) : false;
  const activeReview = Boolean(
    (selectedReview && ACTIVE_REVIEW_STATUSES.has(selectedReview.status))
    || (monitorRun && ACTIVE_REVIEW_STATUSES.has(monitorRun.status)),
  );
  const activePublication = Boolean(
    (selectedReview && ACTIVE_PUBLICATION_STATUSES.has(selectedReview.status))
    || (monitorRun && ACTIVE_PUBLICATION_STATUSES.has(monitorRun.status)),
  );
  const activeRepair = Boolean(monitorRun?.wakes.some((wake) => STARTED_REPAIR_STATUSES.has(wake.status)));
  const activeMerge = Boolean(monitorRun?.merge_actions?.some((action) => STARTED_MERGE_STATUSES.has(action.status)));
  const activeAdjudication = reviewerRuns.some((reviewerRun) => reviewerRun.findings.some(
    (finding) => finding.rebuttals?.some((rebuttal) => ACTIVE_ADJUDICATION_STATUSES.has(rebuttal.status)),
  ));
  const protectedRun = terminalRun || readyRun || busyRun || activeRepair || activeMerge || activeAdjudication;
  const canChangeBinding = Boolean(monitorRun && !protectedRun && !activePublication);
  const canBindDeveloper = canChangeBinding && detail.enabled;
  const canPause = Boolean(
    monitorRun
    && monitorRun.status !== 'paused'
    && !protectedRun
    && !activeReview,
  );
  const canResume = Boolean(
    monitorRun
    && monitorRun.status === 'paused'
    && detail.enabled
    && !protectedRun
    && !activeReview
    && !activePublication,
  );
  const canEnqueue = Boolean(
    monitorRun?.status === 'ready_to_merge'
    && !busyRun
    && !activeRepair
    && !activeMerge
    && !activeAdjudication
    && !activeReview
    && !activePublication,
  );
  const developerTaskNumber = Number(developerTaskId);
  const validDeveloperTaskId = Number.isInteger(developerTaskNumber) && developerTaskNumber > 0;

  const handleCopy = (text: string, label: string) => {
    copyToClipboard(text);
    setCopied(label);
    setTimeout(() => setCopied(null), 2000);
  };

  return (
    <div className="space-y-6">
      <button onClick={onBack} className="flex items-center gap-1 text-sm text-gray-400 hover:text-foreground">
        <ArrowLeft size={16} /> Back to repositories
      </button>

      <div className="bg-gray-800 rounded-lg p-5 space-y-4">
        <h3 className="text-foreground font-semibold text-lg">{detail.repo_full_name}</h3>
        {saveError && <p className="text-sm text-red-400">{saveError}</p>}

        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <div className="flex items-center gap-2">
            <input type="checkbox" id="detailAutoMerge" checked={reviewMode === 'single' && autoMerge}
              disabled={reviewMode !== 'single'} onChange={(e) => { setAutoMerge(e.target.checked); if (e.target.checked) setMergeQueueMode('manual'); }}
              className="rounded bg-gray-700 border-gray-600" />
            <label htmlFor="detailAutoMerge" className="text-sm text-gray-300">Legacy auto-merge (single reviewer only)</label>
          </div>
          <div className="flex items-center gap-2">
            <input type="checkbox" id="detailAutoRepair" checked={autoRepair}
              disabled={reviewMode !== 'panel'} onChange={(e) => setAutoRepair(e.target.checked)} />
            <label htmlFor="detailAutoRepair" className="text-sm text-gray-300">Auto-resume bound Developer Task</label>
          </div>
          <div>
            <label className="block text-xs text-gray-400 mb-1">Max automatic repair heads</label>
            <input type="number" min={1} max={20} value={maxRepairAttempts}
              onChange={(e) => setMaxRepairAttempts(Number(e.target.value))}
              className="w-full bg-gray-700 text-foreground text-sm rounded px-3 py-2" />
          </div>
          <div>
            <label className="block text-xs text-gray-400 mb-1">Merge Queue mode</label>
            <select value={mergeQueueMode} onChange={(e) => { const value = e.target.value as 'manual' | 'shadow' | 'auto'; setMergeQueueMode(value); if (value !== 'manual') setAutoMerge(false); }}
              disabled={reviewMode !== 'panel' || !waitForCi}
              className="w-full bg-gray-700 text-foreground text-sm rounded px-3 py-2">
              <option value="manual">Manual</option><option value="shadow">Shadow</option><option value="auto">Automatic</option>
            </select>
          </div>
          {reviewMode === 'panel' && waitForCi && (
            <div className="md:col-span-2">
              <label className="block text-xs text-gray-400 mb-1">Required CI identities（每行一个）</label>
              <textarea className="w-full bg-gray-700 text-foreground text-xs rounded px-3 py-2 font-mono"
                rows={3} value={requiredChecks} onChange={(e) => setRequiredChecks(e.target.value)}
                placeholder={'check_run,tests,github-actions\nstatus,lint,ci-bot'} />
            </div>
          )}
          <div>
            <label className="block text-xs text-gray-400 mb-1">Provider</label>
            <select className="w-full bg-gray-700 text-foreground text-sm rounded px-3 py-2 outline-none focus:ring-1 focus:ring-indigo-500"
              value={provider} onChange={(e) => { setProvider(e.target.value); setReviewModel(''); setReviewEffort(''); }}>
              {providers.map((p) => <option key={p} value={p}>{p === 'codex' ? 'Codex' : 'Claude Code'}</option>)}
            </select>
          </div>
          <div>
            <label className="block text-xs text-gray-400 mb-1">Review Model</label>
            <select className="w-full bg-gray-700 text-foreground text-sm rounded px-3 py-2 outline-none focus:ring-1 focus:ring-indigo-500"
              value={reviewModel} onChange={(e) => { setReviewModel(e.target.value); setReviewEffort(''); }}>
              <option value="">default</option>
              {reviewModel && !modelOptions.includes(reviewModel) && (
                <option value={reviewModel}>{reviewModel}</option>
              )}
              {modelOptions.map((m) => <option key={m} value={m}>{m}</option>)}
            </select>
          </div>
          <div>
            <label className="block text-xs text-gray-400 mb-1">Review Effort</label>
            <select className="w-full bg-gray-700 text-foreground text-sm rounded px-3 py-2 outline-none focus:ring-1 focus:ring-indigo-500"
              value={reviewEffort} onChange={(e) => setReviewEffort(e.target.value)}>
              <option value="">default</option>
              {reviewEffort && !effortOptions.includes(reviewEffort) && (
                <option value={reviewEffort}>{reviewEffort}</option>
              )}
              {effortOptions.map((effort) => <option key={effort} value={effort}>{effort}</option>)}
            </select>
          </div>
          <div>
            <label className="block text-xs text-gray-400 mb-1">Default Branch</label>
            <input className="w-full bg-gray-700 text-foreground text-sm rounded px-3 py-2 outline-none focus:ring-1 focus:ring-indigo-500"
              value={defaultBranch} onChange={(e) => setDefaultBranch(e.target.value)} />
          </div>
          <div>
            <label className="block text-xs text-gray-400 mb-1">Review Harness</label>
            <select className="w-full bg-gray-700 text-foreground text-sm rounded px-3 py-2"
              value={reviewMode} onChange={(e) => {
                const value = e.target.value as 'single' | 'panel';
                setReviewMode(value);
                if (value === 'panel') setAutoMerge(false);
                else { setAutoRepair(false); setWaitForCi(false); setMergeQueueMode('manual'); }
              }}>
              <option value="panel">Independent Principal / Senior / QA panel</option>
              <option value="single">Legacy single reviewer</option>
            </select>
          </div>
          <div className="flex items-center gap-2">
            <input type="checkbox" id="waitForCi" checked={reviewMode === 'panel' && waitForCi}
              disabled={reviewMode !== 'panel'} onChange={(e) => {
                setWaitForCi(e.target.checked);
                if (!e.target.checked) setMergeQueueMode('manual');
              }} />
            <label htmlFor="waitForCi" className="text-sm text-gray-300">Wait for exact-head CI before review</label>
          </div>
          <div>
            <label className="block text-xs text-gray-400 mb-1">Allowed Authors (comma-separated)</label>
            <input className="w-full bg-gray-700 text-foreground text-sm rounded px-3 py-2 outline-none focus:ring-1 focus:ring-indigo-500"
              value={authorsInput} onChange={(e) => setAuthorsInput(e.target.value)} placeholder="All authors" />
          </div>
        </div>

        <button onClick={handleSave} disabled={saving}
          className="px-4 py-2 text-sm bg-indigo-600 text-white rounded hover:bg-indigo-500 disabled:opacity-50">
          {saving ? 'Saving...' : 'Save Changes'}
        </button>
      </div>

      <div className="bg-gray-800 rounded-lg p-5 space-y-3">
        <h4 className="text-foreground font-semibold">Webhook Configuration</h4>
        <div className="space-y-2">
          <div>
            <label className="block text-xs text-gray-400 mb-1">Payload URL</label>
            <div className="flex items-center gap-2">
              <code className="flex-1 bg-gray-700 text-foreground text-xs rounded px-3 py-2 overflow-x-auto">{webhookUrl}</code>
              <button onClick={() => handleCopy(webhookUrl, 'url')}
                className="p-2 text-gray-400 hover:text-foreground">
                {copied === 'url' ? <Check size={16} className="text-green-400" /> : <Copy size={16} />}
              </button>
            </div>
          </div>
          <div>
            <label className="block text-xs text-gray-400 mb-1">Secret</label>
            <div className="flex items-center gap-2">
              <code className="flex-1 bg-gray-700 text-foreground text-xs rounded px-3 py-2 overflow-x-auto">{detail.webhook_secret}</code>
              <button onClick={() => handleCopy(detail.webhook_secret, 'secret')}
                className="p-2 text-gray-400 hover:text-foreground">
                {copied === 'secret' ? <Check size={16} className="text-green-400" /> : <Copy size={16} />}
              </button>
              <button onClick={handleRegenerate} className="p-2 text-gray-400 hover:text-foreground" title="Regenerate secret">
                <RefreshCw size={16} />
              </button>
            </div>
          </div>
          <p className="text-xs text-gray-500">
            Content type: application/json. Events: Pull requests only.
          </p>
        </div>
      </div>

      <div className="bg-gray-800 rounded-lg p-5 space-y-3">
        <h4 className="text-foreground font-semibold">Review History</h4>
        {runActionError && <p className="text-sm text-red-400" role="alert">{runActionError}</p>}
        {reviews.length === 0 ? (
          <p className="text-gray-500 text-sm">No reviews yet</p>
        ) : (
          <>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="text-gray-400 text-left border-b border-gray-700">
                    <th className="pb-2 pr-4">PR</th>
                    <th className="pb-2 pr-4">Title</th>
                    <th className="pb-2 pr-4">Author</th>
                    <th className="pb-2 pr-4">Status</th>
                    <th className="pb-2 pr-4">Action</th>
                    <th className="pb-2 pr-4">Task</th>
                    <th className="pb-2">Time</th>
                  </tr>
                </thead>
                <tbody>
                  {reviews.map(r => (
                    <tr key={r.id} className="border-b border-gray-700/50 text-gray-300 cursor-pointer hover:bg-gray-700/30"
                      onClick={async () => {
                        setRunActionError(null);
                        setDeveloperTaskId('');
                        try {
                          const reviewDetail = await api.getReviewDetail(r.id);
                          setSelectedReview(reviewDetail);
                          setMonitorRun(null);
                          if (reviewDetail.monitor_run_id) {
                            try {
                              setMonitorRun(await api.getPRMonitorRun(reviewDetail.monitor_run_id));
                            } catch (error) {
                              setRunActionError(String(error));
                            }
                          }
                        } catch (error) {
                          setSelectedReview(null);
                          setMonitorRun(null);
                          setRunActionError(String(error));
                        }
                      }}>
                      <td className="py-2 pr-4">
                        <a href={r.pr_url} target="_blank" rel="noopener noreferrer"
                          className="text-indigo-400 hover:text-indigo-300">#{r.pr_number}</a>
                      </td>
                      <td className="py-2 pr-4 max-w-xs truncate">{r.pr_title}</td>
                      <td className="py-2 pr-4">{r.pr_author}</td>
                      <td className="py-2 pr-4">
                        <span className={`px-2 py-0.5 rounded text-xs ${STATUS_COLORS[r.status] || 'bg-gray-600 text-gray-300'}`}>
                          {r.status}
                        </span>
                      </td>
                      <td className="py-2 pr-4 text-xs">{r.action_taken || '-'}</td>
                      <td className="py-2 pr-4">
                        {r.task_id ? <span className="text-indigo-400">#{r.task_id}</span> : '-'}
                      </td>
                      <td className="py-2 text-xs text-gray-500">{new Date(r.created_at).toLocaleString()}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            {selectedReview && (
              <>
                <div className="mt-4 border-t border-gray-700 pt-4 space-y-3">
                <div className="flex justify-between">
                  <h5 className="font-medium text-foreground">Review Detail · PR #{selectedReview.pr_number}</h5>
                  <button className="text-xs text-gray-400" onClick={() => {
                    setSelectedReview(null);
                    setMonitorRun(null);
                    setRunActionError(null);
                  }}>Close</button>
                </div>
                <p className="text-xs text-gray-400">Review: {selectedReview.status}</p>
                <div className="rounded bg-gray-900/40 p-3">
                  <h6 className="mb-2 text-sm font-medium text-foreground">1. 总体评价</h6>
                  <p className="text-xs text-gray-300">{selectedReview.review_summary || '评审仍在执行，暂无总体结论。'}</p>
                </div>
                {selectedReview.ci_summary && <p className="text-xs text-gray-400">CI: {selectedReview.ci_summary}</p>}
                {monitorRun && (
                  <div className="rounded bg-gray-900/50 p-3 text-xs space-y-2">
                    <p>Loop: {monitorRun.status} · repair {monitorRun.repair_attempts}/{monitorRun.max_repair_attempts}</p>
                    <p>Developer Task: {monitorRun.developer_task_id ? `#${monitorRun.developer_task_id}` : 'not bound'}</p>
                    {canBindDeveloper && !monitorRun.developer_task_id && (
                      <div className="flex gap-2">
                        <input value={developerTaskId} onChange={(e) => setDeveloperTaskId(e.target.value)}
                          disabled={runActionPending !== null} placeholder="Developer Task ID"
                          className="bg-gray-700 rounded px-2 py-1" />
                        <button className="bg-indigo-600 text-white rounded px-2 py-1 disabled:opacity-50"
                          disabled={runActionPending !== null || !validDeveloperTaskId}
                          onClick={() => performRunAction('bind', () => (
                            api.bindPRMonitorDeveloper(monitorRun.id, developerTaskNumber)
                          ))}>{runActionPending === 'bind' ? 'Binding…' : 'Bind'}</button>
                      </div>
                    )}
                    {monitorRun.pause_reason && <p className="text-yellow-500">Paused: {monitorRun.pause_reason}</p>}
                    {monitorRun.wakes.map(wake => <p key={wake.id}>Wake #{wake.id}: {wake.status} · {wake.reason_kind}{wake.last_error ? ` · ${wake.last_error}` : ''}</p>)}
                    {monitorRun.merge_actions?.map(action => <p key={action.id}>Merge #{action.id}: {action.status}{action.ci_status ? ` · CI ${action.ci_status}` : ''}{action.last_error ? ` · ${action.last_error}` : ''}</p>)}
                    <div className="flex gap-2">
                      {canResume && (
                        <button className="bg-indigo-600 text-white rounded px-2 py-1 disabled:opacity-50"
                          disabled={runActionPending !== null}
                          onClick={() => performRunAction('resume', () => api.resumePRMonitorRun(monitorRun.id))}>
                          {runActionPending === 'resume' ? 'Resuming…' : 'Resume loop'}
                        </button>
                      )}
                      {canPause && (
                        <button className="bg-gray-700 rounded px-2 py-1 disabled:opacity-50"
                          disabled={runActionPending !== null}
                          onClick={() => performRunAction('pause', () => api.pausePRMonitorRun(monitorRun.id))}>
                          {runActionPending === 'pause' ? 'Pausing…' : 'Pause loop'}
                        </button>
                      )}
                      {canChangeBinding && monitorRun.developer_task_id && (
                        <button className="bg-gray-700 rounded px-2 py-1 disabled:opacity-50"
                          disabled={runActionPending !== null}
                          onClick={() => performRunAction('unbind', () => api.unbindPRMonitorDeveloper(monitorRun.id))}>
                          {runActionPending === 'unbind' ? 'Unbinding…' : 'Unbind Developer'}
                        </button>
                      )}
                      {canEnqueue && (
                        <button className="bg-green-700 text-white rounded px-2 py-1 disabled:opacity-50"
                          disabled={runActionPending !== null}
                          onClick={() => performRunAction('enqueue', () => api.enqueuePRMonitorMerge(monitorRun.id))}>
                          {runActionPending === 'enqueue' ? 'Enqueuing…' : 'Enqueue merge'}
                        </button>
                      )}
                    </div>
                  </div>
                )}
                {selectedReview.ci_details?.observed.map((item) => (
                  <p key={`${item.kind}:${item.name}:${item.app_slug}`} className="text-xs text-gray-500">
                    {item.state} · {item.name} · {item.app_slug}
                  </p>
                ))}
                {reviewerRuns.length === 0 && (
                  <p className="text-xs text-gray-500">Reviewer panel has not started yet.</p>
                )}
                </div>
                {reviewerRuns.length > 0 && <h6 className="text-sm font-medium text-foreground">2. 问题清单（按风险等级排序）</h6>}
                {[...reviewerRuns].map(run => (
                  <div key={run.id} className="rounded bg-gray-900/40 p-3">
                    <div className="flex justify-between text-sm">
                      <span className="font-medium text-gray-200">{run.role}</span>
                      <span className={STATUS_COLORS[run.status] || 'text-gray-400'}>{run.status}</span>
                    </div>
                    {run.error_message && <p className="text-xs text-red-400 mt-1">{run.error_message}</p>}
                    {[...run.findings].sort((a, b) => (
                      (FINDING_SEVERITY_ORDER[a.severity] ?? 9) - (FINDING_SEVERITY_ORDER[b.severity] ?? 9)
                    )).map(finding => (
                      <div key={finding.id} className="mt-3 border-l-2 border-orange-500 pl-3 text-xs space-y-1">
                        <p className="text-orange-300">[{finding.severity}] {finding.path}{finding.line ? `:${finding.line}` : ''} — {finding.title}</p>
                        <p className="text-gray-300">Evidence: {finding.evidence}</p>
                        <p className="text-gray-400">Impact: {finding.impact}</p>
                        <p className="text-gray-400">Required fix: {finding.required_fix}</p>
                        <p className="text-gray-400">Test: {finding.test}</p>
                        <p className="text-gray-500">
                          Thread: {finding.github_comment_url ? (
                            <a className="text-indigo-400 hover:text-indigo-300" href={finding.github_comment_url}
                              target="_blank" rel="noopener noreferrer">{finding.thread_status}</a>
                          ) : finding.thread_status}
                        </p>
                        {finding.thread_error && <p className="text-yellow-500">{finding.thread_error}</p>}
                        <FindingActions
                          finding={finding}
                          currentSnapshot={selectedReview.is_current_snapshot !== false}
                          onChanged={async () => {
                            setSelectedReview(await api.getReviewDetail(selectedReview.id));
                          }}
                        />
                        <FindingRebuttalForm finding={finding} onSubmitted={async () => {
                          const refreshed = await api.getReviewDetail(selectedReview.id);
                          setSelectedReview(refreshed);
                        }} />
                      </div>
                    ))}
                  </div>
                ))}
                {reviewerRuns.length > 0 && (
                  <div className="grid gap-3 md:grid-cols-2">
                    <div className="rounded bg-gray-900/40 p-3">
                      <h6 className="mb-2 text-sm font-medium text-foreground">3. 优化总结</h6>
                      <p className="text-xs text-gray-300">优先处理高风险和中风险问题，并按每条 Finding 的 Required fix 验证修复。</p>
                    </div>
                    <div className="rounded bg-gray-900/40 p-3">
                      <h6 className="mb-2 text-sm font-medium text-foreground">4. 额外建议</h6>
                      <p className="text-xs text-gray-300">应用修复前下载并检查 Diff；推送后继续以 exact-head CI 和 Finding Gate 为准。</p>
                    </div>
                  </div>
                )}
              </>
            )}
            <div className="flex gap-2 pt-2">
              <button onClick={() => setPage(p => Math.max(1, p - 1))} disabled={page === 1}
                className="px-3 py-1 text-xs bg-gray-700 text-gray-300 rounded disabled:opacity-50">Prev</button>
              <span className="text-xs text-gray-400 py-1">Page {page}</span>
              <button onClick={() => setPage(p => p + 1)} disabled={reviews.length < 20}
                className="px-3 py-1 text-xs bg-gray-700 text-gray-300 rounded disabled:opacity-50">Next</button>
            </div>
          </>
        )}
      </div>
    </div>
  );
}

export function PRMonitorPage() {
  const [repos, setRepos] = useState<MonitoredRepo[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [showModal, setShowModal] = useState(false);
  const [selectedRepo, setSelectedRepo] = useState<MonitoredRepo | null>(null);

  const refresh = useCallback(async () => {
    try {
      const data = await api.getMonitoredRepos();
      setRepos(data);
      setError(null);
    } catch (e) {
      setError(String(e));
    }
  }, []);

  useEffect(() => {
    let active = true;
    api.getMonitoredRepos()
      .then((data) => {
        if (!active) return;
        setRepos(data);
        setError(null);
      })
      .catch((caught) => {
        if (active) setError(String(caught));
      });
    return () => { active = false; };
  }, []);

  const handleToggle = async (repo: MonitoredRepo) => {
    try {
      await api.toggleMonitoredRepo(repo.id);
      await refresh();
    } catch (caught) { setError(String(caught)); }
  };

  const handleDelete = async (repo: MonitoredRepo) => {
    if (!confirm(`Delete monitoring for ${repo.repo_full_name}? This will also delete all review history.`)) return;
    try {
      await api.deleteMonitoredRepo(repo.id);
      await refresh();
    } catch (caught) { setError(String(caught)); }
  };

  if (selectedRepo) {
    return (
      <div className="p-4 md:p-6 max-w-6xl mx-auto">
        <RepoDetail repo={selectedRepo} onBack={() => { setSelectedRepo(null); refresh(); }} onRefresh={refresh} />
      </div>
    );
  }

  return (
    <div className="p-4 md:p-6 max-w-6xl mx-auto">
      <div className="flex items-center justify-between mb-6">
        <div className="flex items-center gap-2">
          <GitPullRequest size={22} className="text-indigo-400" />
          <h2 className="text-xl font-bold text-foreground">PR Monitor</h2>
        </div>
        <button onClick={() => setShowModal(true)}
          className="flex items-center gap-1 px-4 py-2 text-sm bg-indigo-600 text-white rounded hover:bg-indigo-500">
          <Plus size={16} /> Add Repository
        </button>
      </div>

      {error && <p className="text-red-400 text-sm mb-4">{error}</p>}

      {repos.length === 0 ? (
        <div className="text-center py-16 text-gray-500">
          <GitPullRequest size={48} className="mx-auto mb-4 opacity-30" />
          <p>No repositories monitored yet</p>
          <p className="text-sm mt-1">Add a repository to start auto-reviewing PRs</p>
        </div>
      ) : (
        <div className="bg-gray-800 rounded-lg overflow-hidden">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-gray-400 text-left border-b border-gray-700">
                <th className="px-4 py-3">Repository</th>
                <th className="px-4 py-3">Status</th>
                <th className="px-4 py-3">Auto Merge</th>
                <th className="px-4 py-3">Enabled</th>
                <th className="px-4 py-3">Created</th>
                <th className="px-4 py-3"></th>
              </tr>
            </thead>
            <tbody>
              {repos.map(repo => (
                <tr key={repo.id} className="border-b border-gray-700/50 hover:bg-gray-700/30 cursor-pointer text-gray-300"
                  onClick={() => setSelectedRepo(repo)}>
                  <td className="px-4 py-3 font-medium text-foreground">{repo.repo_full_name}</td>
                  <td className="px-4 py-3">
                    <span className={`inline-block w-2 h-2 rounded-full mr-2 ${repo.status === 'active' ? 'bg-green-400' : 'bg-red-400'}`} />
                    {repo.status}
                  </td>
                  <td className="px-4 py-3">
                    {repo.auto_merge ? (
                      <span className="px-2 py-0.5 bg-green-500/20 text-green-400 rounded text-xs">ON</span>
                    ) : (
                      <span className="px-2 py-0.5 bg-gray-600/50 text-gray-400 rounded text-xs">OFF</span>
                    )}
                  </td>
                  <td className="px-4 py-3" onClick={(e) => e.stopPropagation()}>
                    <button onClick={() => handleToggle(repo)} className="text-gray-400 hover:text-foreground">
                      {repo.enabled ? <ToggleRight size={22} className="text-green-400" /> : <ToggleLeft size={22} />}
                    </button>
                  </td>
                  <td className="px-4 py-3 text-xs text-gray-500">{new Date(repo.created_at).toLocaleDateString()}</td>
                  <td className="px-4 py-3" onClick={(e) => e.stopPropagation()}>
                    <button onClick={() => handleDelete(repo)} className="text-gray-400 hover:text-red-400">
                      <Trash2 size={16} />
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {showModal && <AddRepoModal onClose={() => setShowModal(false)} onSaved={refresh} />}
    </div>
  );
}
