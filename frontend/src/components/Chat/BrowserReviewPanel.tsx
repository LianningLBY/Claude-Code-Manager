import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import type { PointerEvent as ReactPointerEvent } from 'react';
import { createPortal } from 'react-dom';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';

import { api } from '../../api/client';
import type { BrowserReviewJob, TestHarnessRun, WorkspaceReviewRun } from '../../api/client';
import {
  Activity,
  AlertCircle,
  CheckCircle2,
  Clock,
  Download,
  Eye,
  FileText,
  GripVertical,
  Image,
  Loader2,
  PanelLeftOpen,
  RefreshCw,
  Square,
  ChevronDown,
  ChevronUp,
  X,
} from '../icons';

const TERMINAL = new Set(['completed', 'failed', 'cancelled', 'stale']);
const STAGE_LABELS: Record<string, string> = {
  validating_workspace: '正在校验本地仓库',
  fingerprinted: '已锁定当前工作区版本',
  starting_preview: '正在启动隔离预览',
  waiting_for_preview: '等待预览服务就绪',
  creating_agent: '正在创建黑盒审查 Agent',
  preview_ready: '隔离预览已就绪',
  browser_agent_queued: '黑盒浏览器 Agent 已排队',
  reviewing: '浏览器 Agent 正在审查',
  checking_fingerprint: '正在核对工作区版本',
  publishing_report: '正在回传 Task 报告',
  cleaning_up: '正在清理预览进程',
  queued: '等待工具启动',
  waiting_for_browser: '正在准备浏览器',
  browser_ready: '页面已打开',
  executing_actions: '正在验证页面状态',
  agent_reported: '正在保存报告',
  completed: '审查完成',
  browser_closed: '浏览器已关闭',
  cancelling: '正在停止',
  cancelled: '已停止',
  failed: '审查失败',
  stale: '结果已过期',
  interrupted: '服务重启中断',
  resolving_git_target: '正在解析 Git 目标',
  detached_worktree_ready: '隔离 Git worktree 已就绪',
  preparing_environment: '正在准备测试环境',
  collecting_evidence: '正在归档测试证据',
  evaluating: '正在生成结构化结论',
};

interface BrowserReviewPanelProps {
  taskId: number;
  taskActive: boolean;
  open: boolean;
  displayMode: BrowserReviewDisplayMode;
  onAvailableChange: (available: boolean) => void;
  onClose: () => void;
  onDisplayModeChange: (mode: BrowserReviewDisplayMode) => void;
  onNewReview: () => void;
  startedWorkspaceRun?: TestHarnessRun | null;
  expectedWorkspaceReviewBaseline?: string | null;
  onExpectedWorkspaceReviewFound?: () => void;
  goalProgress?: BrowserReviewGoalProgress;
}

export type BrowserReviewDisplayMode = 'docked' | 'floating';

export interface BrowserReviewGoalProgress {
  turn: number;
  maxTurns: number;
  lastReason: string | null;
  active: boolean;
}

interface FloatingPosition {
  x: number;
  y: number;
}

const FLOATING_POSITION_KEY = 'ccm-browser-review-floating-position';
const FLOATING_WIDTH = 430;
const FLOATING_MARGIN = 12;
const FLOATING_HEADER_HEIGHT = 54;

function clampFloatingPosition(position: FloatingPosition): FloatingPosition {
  const width = Math.min(FLOATING_WIDTH, Math.max(280, window.innerWidth - FLOATING_MARGIN * 2));
  return {
    x: Math.max(FLOATING_MARGIN, Math.min(position.x, window.innerWidth - width - FLOATING_MARGIN)),
    y: Math.max(FLOATING_MARGIN, Math.min(position.y, window.innerHeight - FLOATING_HEADER_HEIGHT - FLOATING_MARGIN)),
  };
}

function loadFloatingPosition(): FloatingPosition {
  try {
    const parsed = JSON.parse(localStorage.getItem(FLOATING_POSITION_KEY) || 'null') as Partial<FloatingPosition> | null;
    if (typeof parsed?.x === 'number' && typeof parsed?.y === 'number') {
      return clampFloatingPosition({ x: parsed.x, y: parsed.y });
    }
  } catch { /* storage may be unavailable */ }
  return clampFloatingPosition({
    x: window.innerWidth - FLOATING_WIDTH - 24,
    y: Math.max(72, window.innerHeight - 690),
  });
}

function errorText(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function statusClass(status: string): string {
  if (status === 'completed') return 'border-emerald-500/30 bg-emerald-500/10 text-emerald-300';
  if (status === 'failed') return 'border-red-500/30 bg-red-500/10 text-red-300';
  if (status === 'cancelled') return 'border-gray-600 bg-gray-700/50 text-gray-300';
  return 'border-blue-500/30 bg-blue-500/10 text-blue-300';
}

export function BrowserReviewPanel({
  taskId,
  taskActive,
  open,
  displayMode,
  onAvailableChange,
  onClose,
  onDisplayModeChange,
  onNewReview,
  startedWorkspaceRun,
  expectedWorkspaceReviewBaseline,
  onExpectedWorkspaceReviewFound,
  goalProgress,
}: BrowserReviewPanelProps) {
  const [runs, setRuns] = useState<TestHarnessRun[]>([]);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [waitingForWorkspaceReview, setWaitingForWorkspaceReview] = useState(
    expectedWorkspaceReviewBaseline !== undefined,
  );
  const [error, setError] = useState<string | null>(null);
  const [screenshotUrl, setScreenshotUrl] = useState<string | null>(null);
  const screenshotObjectUrlRef = useRef<string | null>(null);
  const [minimized, setMinimized] = useState(false);
  const [cancellingJobId, setCancellingJobId] = useState<string | null>(null);
  const [repeatingRunId, setRepeatingRunId] = useState<string | null>(null);
  const [floatingPosition, setFloatingPosition] = useState<FloatingPosition>(loadFloatingPosition);
  const floatingPanelRef = useRef<HTMLElement | null>(null);
  const dragRef = useRef<{ offsetX: number; offsetY: number } | null>(null);
  const latestReviewIdRef = useRef<string | null>(null);
  const startedWorkspaceRunRef = useRef<TestHarnessRun | null>(null);
  const expectedWorkspaceReviewBaselineRef = useRef<string | null | undefined>(undefined);

  const refresh = useCallback(async () => {
    try {
      const nextRuns = await api.listTestRuns(taskId);
      const seededRun = startedWorkspaceRunRef.current;
      const visibleRuns = seededRun && !nextRuns.some((run) => run.id === seededRun.id)
        ? [seededRun, ...nextRuns]
        : nextRuns;
      const nextLatestId = visibleRuns[0]?.id ?? null;
      const expectedBaseline = expectedWorkspaceReviewBaselineRef.current;
      const expectedWorkspaceRun = expectedBaseline !== undefined
        && visibleRuns[0]
        && visibleRuns[0].id !== expectedBaseline
        ? visibleRuns[0]
        : null;
      const previousLatestId = latestReviewIdRef.current;
      const hasNewReview = Boolean(
        previousLatestId
        && nextLatestId
        && previousLatestId !== nextLatestId,
      );
      latestReviewIdRef.current = nextLatestId;
      setRuns(visibleRuns);
      setSelectedId((current) => (
        expectedWorkspaceRun
          ? expectedWorkspaceRun.id
          : !hasNewReview && current && (
          visibleRuns.some((run) => run.id === current)
        )
          ? current
          : nextLatestId
      ));
      if (expectedWorkspaceRun) {
        expectedWorkspaceReviewBaselineRef.current = undefined;
        setWaitingForWorkspaceReview(false);
        setMinimized(false);
        onExpectedWorkspaceReviewFound?.();
        onNewReview();
      } else if (hasNewReview) {
        setMinimized(false);
        onNewReview();
      }
      setError(null);
      onAvailableChange(visibleRuns.length > 0);
    } catch (nextError) {
      setError(errorText(nextError));
    } finally {
      setLoading(false);
    }
  }, [onAvailableChange, onExpectedWorkspaceReviewFound, onNewReview, taskId]);

  useEffect(() => {
    startedWorkspaceRunRef.current = null;
    expectedWorkspaceReviewBaselineRef.current = undefined;
    setRuns([]);
    setSelectedId(null);
    setLoading(true);
    setWaitingForWorkspaceReview(false);
    latestReviewIdRef.current = null;
    onAvailableChange(false);
    void refresh();
  }, [onAvailableChange, refresh, taskId]);

  useEffect(() => {
    if (!startedWorkspaceRun || startedWorkspaceRun.task_id !== taskId) return;
    startedWorkspaceRunRef.current = startedWorkspaceRun;
    latestReviewIdRef.current = startedWorkspaceRun.id;
    setRuns((current) => [
      startedWorkspaceRun,
      ...current.filter((run) => run.id !== startedWorkspaceRun.id),
    ]);
    setSelectedId(startedWorkspaceRun.id);
    setLoading(false);
    setError(null);
    setMinimized(false);
    onAvailableChange(true);
  }, [onAvailableChange, startedWorkspaceRun, taskId]);

  useEffect(() => {
    expectedWorkspaceReviewBaselineRef.current = expectedWorkspaceReviewBaseline;
    setWaitingForWorkspaceReview(expectedWorkspaceReviewBaseline !== undefined);
    if (expectedWorkspaceReviewBaseline === undefined) return;
    setLoading(true);
    setError(null);
    setMinimized(false);
    void refresh();
  }, [expectedWorkspaceReviewBaseline, refresh]);

  const hasActiveReview = runs.some((run) => !TERMINAL.has(run.status));
  useEffect(() => {
    if (!taskActive && !hasActiveReview && !waitingForWorkspaceReview) return;
    const timer = window.setInterval(() => { void refresh(); }, 1000);
    return () => window.clearInterval(timer);
  }, [hasActiveReview, refresh, taskActive, waitingForWorkspaceReview]);

  const harnessRun = runs.find((item) => item.id === selectedId)
    ?? runs[0]
    ?? null;
  const displayedRun = waitingForWorkspaceReview ? null : harnessRun;
  const selectedRunIndex = harnessRun
    ? runs.findIndex((item) => item.id === harnessRun.id)
    : -1;
  const selectAdjacentRun = (offset: number) => {
    if (selectedRunIndex < 0) return;
    const nextRun = runs[selectedRunIndex + offset];
    if (nextRun) setSelectedId(nextRun.id);
  };
  const workspaceRun: WorkspaceReviewRun | null = displayedRun?.workspace_review ?? null;
  const job: BrowserReviewJob | null = displayedRun?.browser_review ?? null;
  const harnessRunId = displayedRun?.id ?? null;
  const latestScreenshot = job?.latest_screenshot ?? null;

  useEffect(() => {
    if (!latestScreenshot) {
      if (screenshotObjectUrlRef.current) URL.revokeObjectURL(screenshotObjectUrlRef.current);
      screenshotObjectUrlRef.current = null;
      setScreenshotUrl(null);
      return;
    }
    let active = true;
    if (!harnessRunId) return;
    api.getTestRunEvidence(taskId, harnessRunId, latestScreenshot)
      .then((blob) => {
        const nextObjectUrl = URL.createObjectURL(blob);
        if (!active) {
          URL.revokeObjectURL(nextObjectUrl);
          return;
        }
        const previousObjectUrl = screenshotObjectUrlRef.current;
        screenshotObjectUrlRef.current = nextObjectUrl;
        setScreenshotUrl(nextObjectUrl);
        if (previousObjectUrl) URL.revokeObjectURL(previousObjectUrl);
      })
      .catch((nextError) => {
        if (active) setError(errorText(nextError));
      });
    return () => {
      active = false;
    };
  }, [harnessRunId, latestScreenshot, taskId]);

  useEffect(() => () => {
    if (screenshotObjectUrlRef.current) URL.revokeObjectURL(screenshotObjectUrlRef.current);
  }, []);

  const telemetry = useMemo(() => Object.entries(job?.telemetry || {})
    .filter((entry): entry is [string, Record<string, unknown>[]] => Array.isArray(entry[1]))
    .filter(([, entries]) => entries.length > 0), [job?.telemetry]);

  const download = async (name: string) => {
    if (!harnessRun) return;
    try {
      const blob = await api.getTestRunEvidence(taskId, harnessRun.id, name);
      const objectUrl = URL.createObjectURL(blob);
      const anchor = document.createElement('a');
      anchor.href = objectUrl;
      anchor.download = name;
      anchor.click();
      URL.revokeObjectURL(objectUrl);
    } catch (nextError) {
      setError(errorText(nextError));
    }
  };
  const stopReview = async () => {
    if (!harnessRun || TERMINAL.has(harnessRun.status) || cancellingJobId === harnessRun.id) return;
    setCancellingJobId(harnessRun.id);
    setError(null);
    try {
      const cancelled = await api.cancelTestRun(taskId, harnessRun.id);
      setRuns((current) => current.map((item) => (
        item.id === cancelled.id ? cancelled : item
      )));
    } catch (nextError) {
      setError(`停止审查失败，Task 可能仍在运行：${errorText(nextError)}`);
    } finally {
      setCancellingJobId(null);
    }
  };
  const repeatReview = async () => {
    if (
      !harnessRun
      || !TERMINAL.has(harnessRun.status)
      || taskActive
      || repeatingRunId === harnessRun.id
    ) return;
    setRepeatingRunId(harnessRun.id);
    setError(null);
    try {
      const repeated = await api.repeatTestRun(taskId, harnessRun.id);
      startedWorkspaceRunRef.current = repeated;
      latestReviewIdRef.current = repeated.id;
      setRuns((current) => [
        repeated,
        ...current.filter((item) => item.id !== repeated.id),
      ]);
      setSelectedId(repeated.id);
      setMinimized(false);
      onAvailableChange(true);
      onNewReview();
    } catch (nextError) {
      setError(`重新测试失败：${errorText(nextError)}`);
    } finally {
      setRepeatingRunId(null);
    }
  };
  const displayedGoalRound = goalProgress
    ? Math.min(
      Math.max(1, goalProgress.maxTurns),
      Math.max(1, goalProgress.turn + (goalProgress.active ? 1 : 0)),
    )
    : null;

  const startDragging = useCallback((event: ReactPointerEvent<HTMLDivElement>) => {
    if (displayMode !== 'floating' || event.button !== 0) return;
    if ((event.target as HTMLElement).closest('button, select, a')) return;
    const bounds = floatingPanelRef.current?.getBoundingClientRect();
    dragRef.current = {
      offsetX: event.clientX - (bounds?.left ?? floatingPosition.x),
      offsetY: event.clientY - (bounds?.top ?? floatingPosition.y),
    };
    document.body.style.userSelect = 'none';
    event.preventDefault();
  }, [displayMode, floatingPosition]);

  useEffect(() => {
    if (displayMode !== 'floating') return;

    const onPointerMove = (event: PointerEvent) => {
      if (!dragRef.current) return;
      setFloatingPosition(clampFloatingPosition({
        x: event.clientX - dragRef.current.offsetX,
        y: event.clientY - dragRef.current.offsetY,
      }));
    };
    const onPointerUp = () => {
      if (!dragRef.current) return;
      dragRef.current = null;
      document.body.style.userSelect = '';
      setFloatingPosition((current) => {
        try { localStorage.setItem(FLOATING_POSITION_KEY, JSON.stringify(current)); } catch { /* storage may be unavailable */ }
        return current;
      });
    };
    const onResize = () => setFloatingPosition((current) => clampFloatingPosition(current));

    window.addEventListener('pointermove', onPointerMove);
    window.addEventListener('pointerup', onPointerUp);
    window.addEventListener('resize', onResize);
    return () => {
      window.removeEventListener('pointermove', onPointerMove);
      window.removeEventListener('pointerup', onPointerUp);
      window.removeEventListener('resize', onResize);
      dragRef.current = null;
      document.body.style.userSelect = '';
    };
  }, [displayMode]);

  useEffect(() => {
    if (displayMode === 'docked') setMinimized(false);
  }, [displayMode]);

  if (
    !open
    || (!waitingForWorkspaceReview && !loading && runs.length === 0)
  ) return null;

  const panel = (
    <aside
      ref={floatingPanelRef}
      aria-label="Frontend Review progress"
      data-display-mode={displayMode}
      className={displayMode === 'floating'
        ? `fixed z-[70] flex w-[min(430px,calc(100vw-24px))] flex-col overflow-hidden rounded-xl border border-gray-700 bg-gray-950/98 shadow-2xl shadow-black/60 backdrop-blur ${minimized ? '' : 'max-h-[min(720px,calc(100vh-24px))]'}`
        : 'flex max-h-[46vh] w-full shrink-0 flex-col border-t border-gray-800 bg-gray-950/95 lg:max-h-none lg:w-[430px] lg:border-l lg:border-t-0'}
      style={displayMode === 'floating' ? { left: floatingPosition.x, top: floatingPosition.y } : undefined}
    >
      <div
        data-floating-drag-handle={displayMode === 'floating' ? 'true' : undefined}
        onPointerDown={startDragging}
        className={`flex items-center gap-2 border-b border-gray-800 px-3 py-2.5 ${displayMode === 'floating' ? 'cursor-move touch-none' : ''}`}
      >
        {displayMode === 'floating' && <GripVertical size={14} className="shrink-0 text-gray-600" />}
        <Eye size={16} className="text-indigo-400" />
        <div className="min-w-0 flex-1">
          <div className="text-sm font-medium text-gray-100">前端运行审查</div>
          <div className="truncate text-[10px] text-gray-500">
            Task #{taskId}{displayedGoalRound ? ` · Goal 第 ${displayedGoalRound} 轮` : ''} · {waitingForWorkspaceReview
              ? '等待 Agent 创建新的浏览器审查'
              : displayedRun
              ? STAGE_LABELS[displayedRun.stage] || displayedRun.stage
              : '加载审查进度'}
          </div>
        </div>
        <button
          type="button"
          onClick={() => void refresh()}
          className="rounded p-1.5 text-gray-500 hover:bg-gray-800 hover:text-gray-300"
          title="刷新审查进度"
        >
          <RefreshCw size={14} />
        </button>
        {displayMode === 'docked' ? (
          <button
            type="button"
            onClick={() => onDisplayModeChange('floating')}
            className="rounded p-1.5 text-gray-500 hover:bg-gray-800 hover:text-indigo-300"
            title="切换为浮窗"
            aria-label="Open Frontend Review as floating window"
          >
            <Square size={13} />
          </button>
        ) : (
          <>
            <button
              type="button"
              onClick={() => onDisplayModeChange('docked')}
              className="rounded p-1.5 text-gray-500 hover:bg-gray-800 hover:text-indigo-300"
              title="停靠到右侧"
              aria-label="Dock Frontend Review panel"
            >
              <PanelLeftOpen size={14} />
            </button>
            <button
              type="button"
              onClick={() => setMinimized((value) => !value)}
              className="rounded p-1.5 text-gray-500 hover:bg-gray-800 hover:text-gray-300"
              title={minimized ? '展开浮窗' : '最小化浮窗'}
              aria-label={minimized ? 'Restore Frontend Review window' : 'Minimize Frontend Review window'}
            >
              {minimized ? <ChevronUp size={14} /> : <ChevronDown size={14} />}
            </button>
          </>
        )}
        <button
          type="button"
          onClick={onClose}
          className="rounded p-1.5 text-gray-500 hover:bg-gray-800 hover:text-gray-300"
          aria-label="Close Frontend Review panel"
        >
          <X size={14} />
        </button>
      </div>

      {!minimized && !waitingForWorkspaceReview && runs.length > 1 && (
        <div className="border-b border-gray-800 px-3 py-2">
          <div className="flex items-center gap-1.5">
            <select
              value={harnessRun?.id || ''}
              onChange={(event) => setSelectedId(event.target.value)}
              aria-label="Select test run"
              className="min-w-0 flex-1 rounded border border-gray-700 bg-gray-900 px-2 py-1.5 text-xs text-gray-200 outline-none focus:border-indigo-500"
            >
              {runs.map((item, index) => (
                <option key={item.id} value={item.id}>
                  #{runs.length - index} · {item.target_kind} · {STAGE_LABELS[item.stage] || item.stage} · {String(item.test_plan.objective || '')}
                </option>
              ))}
            </select>
            <button
              type="button"
              onClick={() => selectAdjacentRun(1)}
              disabled={selectedRunIndex < 0 || selectedRunIndex >= runs.length - 1}
              className="rounded border border-gray-700 bg-gray-900 p-1.5 text-gray-400 hover:border-indigo-500/60 hover:text-indigo-300 disabled:cursor-not-allowed disabled:opacity-35"
              title="切换到更早的测试"
              aria-label="Select older test run"
            >
              <ChevronDown size={14} />
            </button>
            <button
              type="button"
              onClick={() => selectAdjacentRun(-1)}
              disabled={selectedRunIndex <= 0}
              className="rounded border border-gray-700 bg-gray-900 p-1.5 text-gray-400 hover:border-indigo-500/60 hover:text-indigo-300 disabled:cursor-not-allowed disabled:opacity-35"
              title="切换到更新的测试"
              aria-label="Select newer test run"
            >
              <ChevronUp size={14} />
            </button>
          </div>
        </div>
      )}

      {!minimized && <div className="min-h-0 flex-1 space-y-3 overflow-y-auto p-3">
        {waitingForWorkspaceReview && (
          <section data-testid="workspace-review-expected" className="flex min-h-64 flex-col items-center justify-center rounded-lg border border-cyan-500/25 bg-cyan-500/8 px-5 py-8 text-center">
            <div className="flex h-10 w-10 items-center justify-center rounded-full border border-cyan-400/25 bg-cyan-400/10">
              <Loader2 size={18} className="animate-spin text-cyan-300" />
            </div>
            <div className="mt-3 text-sm font-medium text-cyan-100">正在创建新的前端测试</div>
            <div className="mt-1 max-w-72 text-[11px] leading-relaxed text-gray-400">
              已收到本次测试请求，正在等待 Agent 创建独立的 Harness Run。新 Run 就绪后，右栏会自动切换到它的实时截图和操作轨迹。
            </div>
            <div className="mt-4 grid w-full max-w-72 gap-1.5 text-left text-[10px] text-gray-500">
              <div className="rounded bg-gray-950/45 px-2.5 py-1.5">1 · 识别本次测试目标</div>
              <div className="rounded bg-gray-950/45 px-2.5 py-1.5">2 · 创建独立 Harness Run</div>
              <div className="rounded bg-gray-950/45 px-2.5 py-1.5">3 · 绑定浏览器 Agent</div>
            </div>
            <div className="mt-3 text-[10px] text-gray-600">上一轮测试仍保留在历史记录中，本页不会继续展示其内容。</div>
          </section>
        )}
        {loading && !waitingForWorkspaceReview && !displayedRun && (
          <div className="flex items-center justify-center gap-2 py-10 text-sm text-gray-500">
            <Loader2 size={15} className="animate-spin" />
            加载审查运行…
          </div>
        )}
        {error && (
          <div role="alert" className="flex items-start gap-2 rounded border border-red-500/30 bg-red-500/10 px-2.5 py-2 text-xs text-red-300">
            <AlertCircle size={14} className="mt-0.5 shrink-0" />
            <span className="break-words">{error}</span>
          </div>
        )}
        {displayedRun && (
          <section data-testid="test-harness-progress" className="rounded-lg border border-indigo-500/25 bg-indigo-500/8 p-3">
            <div className="flex items-start justify-between gap-2">
              <div className="min-w-0">
                <div className="text-xs font-medium text-indigo-200">Test Harness · {displayedRun.target_kind}</div>
                <div className="mt-0.5 line-clamp-2 text-[10px] text-gray-500">
                  {String(displayedRun.test_plan.objective || '前端黑盒测试')}
                </div>
              </div>
              <div className="flex shrink-0 items-center gap-1.5">
                {!TERMINAL.has(displayedRun.status) && (
                  <button
                    type="button"
                    onClick={() => void stopReview()}
                    disabled={cancellingJobId === displayedRun.id}
                    className="inline-flex items-center gap-1 rounded border border-red-500/30 px-1.5 py-0.5 text-[10px] text-red-300 hover:bg-red-500/10 disabled:cursor-wait disabled:opacity-60"
                    aria-label="Stop test run"
                  >
                    {cancellingJobId === displayedRun.id
                      ? <Loader2 size={10} className="animate-spin" />
                      : <Square size={9} />}
                    {cancellingJobId === displayedRun.id ? '停止中' : '停止'}
                  </button>
                )}
                {TERMINAL.has(displayedRun.status) && !taskActive && (
                  <button
                    type="button"
                    onClick={() => void repeatReview()}
                    disabled={repeatingRunId === displayedRun.id}
                    className="inline-flex items-center gap-1 rounded border border-indigo-500/30 px-1.5 py-0.5 text-[10px] text-indigo-300 hover:bg-indigo-500/10 disabled:cursor-wait disabled:opacity-60"
                    aria-label="Repeat test run"
                  >
                    <RefreshCw size={10} className={repeatingRunId === displayedRun.id ? 'animate-spin' : ''} />
                    {repeatingRunId === displayedRun.id ? '创建中' : '重新测试'}
                  </button>
                )}
                <span className={`rounded-full border px-2 py-0.5 text-[10px] ${statusClass(displayedRun.status)}`}>
                  {STAGE_LABELS[displayedRun.stage] || displayedRun.stage}
                </span>
              </div>
            </div>
            <div className="mt-2 grid grid-cols-2 gap-2 text-[10px] text-gray-500">
              <div className="truncate rounded bg-gray-950/60 px-2 py-1.5" title={displayedRun.source_git_head || ''}>
                {displayedRun.source_git_head ? `HEAD ${displayedRun.source_git_head.slice(0, 10)}` : `Run ${displayedRun.id.slice(0, 10)}`}
              </div>
              <div className={`rounded px-2 py-1.5 ${displayedRun.stale ? 'bg-amber-500/10 text-amber-300' : 'bg-gray-950/60'}`}>
                {displayedRun.stale ? '代码已变化 · 结果过期' : `结论 ${displayedRun.verdict || '待定'}`}
              </div>
            </div>
            {(displayedRun.error || displayedRun.cleanup_error) && (
              <div className="mt-2 whitespace-pre-wrap break-words text-[10px] text-red-300">
                {displayedRun.error || displayedRun.cleanup_error}
              </div>
            )}
          </section>
        )}
        {workspaceRun && (
          <section data-testid="workspace-review-progress" className="rounded-lg border border-blue-500/25 bg-blue-500/8 p-3">
            <div className="flex items-start justify-between gap-2">
              <div className="min-w-0">
                <div className="text-xs font-medium text-blue-200">当前分支黑盒测试</div>
                <div className="mt-0.5 line-clamp-2 text-[10px] text-gray-500">{workspaceRun.goal}</div>
              </div>
              <div className="flex shrink-0 items-center gap-1.5">
                <span className={`rounded-full border px-2 py-0.5 text-[10px] ${statusClass(workspaceRun.status)}`}>
                  {STAGE_LABELS[workspaceRun.stage] || workspaceRun.stage}
                </span>
              </div>
            </div>
            <div className="mt-2 grid grid-cols-2 gap-2 text-[10px] text-gray-500">
              <div className="truncate rounded bg-gray-950/60 px-2 py-1.5" title={workspaceRun.git_head}>
                HEAD {workspaceRun.git_head.slice(0, 10)}
              </div>
              <div className={`rounded px-2 py-1.5 ${workspaceRun.stale ? 'bg-amber-500/10 text-amber-300' : 'bg-gray-950/60'}`}>
                {workspaceRun.stale ? '工作区已变化 · 结果过期' : `指纹 ${workspaceRun.workspace_fingerprint.slice(0, 10)}`}
              </div>
            </div>
            {workspaceRun.preview_url && (
              <div className="mt-2 truncate text-[10px] text-gray-600" title={workspaceRun.preview_url}>
                隔离预览：{workspaceRun.preview_url}
              </div>
            )}
            {(workspaceRun.error || workspaceRun.cleanup_error) && (
              <div className="mt-2 whitespace-pre-wrap break-words text-[10px] text-red-300">
                {workspaceRun.error || workspaceRun.cleanup_error}
              </div>
            )}
          </section>
        )}
        {job && (
          <section className="overflow-hidden rounded-lg border border-gray-800 bg-black">
            <div className="flex items-center gap-1.5 border-b border-gray-800 bg-gray-900 px-2.5 py-2 text-[11px] text-gray-400">
              <Image size={13} />
              最新浏览器画面
            </div>
            {screenshotUrl ? (
              <img src={screenshotUrl} alt="Latest frontend review screenshot" className="block h-auto w-full" />
            ) : (
              <div className="flex aspect-video items-center justify-center text-xs text-gray-600">
                {TERMINAL.has(job.status) ? '没有可用截图' : '等待浏览器截图…'}
              </div>
            )}
          </section>
        )}
        {displayedRun && (
          <section className="rounded-lg border border-gray-800 bg-gray-900/55">
            <div className="flex items-center gap-1.5 border-b border-gray-800 px-3 py-2 text-xs font-medium text-gray-200">
              <Activity size={13} className="text-indigo-400" />
              模型观察与操作轨迹
            </div>
            <div className="max-h-72 space-y-0 overflow-y-auto px-3 py-1">
              {displayedRun.events.length === 0 && (
                <div className="py-5 text-center text-[11px] text-gray-600">等待测试 Harness 开始…</div>
              )}
              {displayedRun.events.map((event, index) => (
                <div key={event.id} className="relative border-l border-gray-700 py-2 pl-4">
                  <span className={`absolute -left-1 top-3 h-2 w-2 rounded-full ${event.event_type === 'decision' ? 'bg-indigo-400' : 'bg-cyan-400'}`} />
                  <div className="flex items-center justify-between gap-2">
                    <span className="text-[11px] font-medium text-gray-300">{event.title}</span>
                    <span className="text-[9px] text-gray-600">{index + 1}</span>
                  </div>
                  {event.detail && <div className="mt-0.5 whitespace-pre-wrap break-words text-[10px] leading-relaxed text-gray-500">{event.detail}</div>}
                </div>
              ))}
            </div>
          </section>
        )}
        {job && (
          <>
            {goalProgress && (
              <section data-testid="frontend-review-goal-progress" className="rounded-lg border border-indigo-500/25 bg-indigo-500/8 p-3">
                <div className="flex items-center justify-between gap-3">
                  <div>
                    <div className="text-xs font-medium text-indigo-200">Goal 循环审查 · 模型自动判断</div>
                    <div className="mt-0.5 text-[10px] text-gray-500">
                      第 {displayedGoalRound ?? 1} 轮 · 安全上限 {goalProgress.maxTurns} 轮
                    </div>
                  </div>
                  <span className={`h-2 w-2 shrink-0 rounded-full ${goalProgress.active ? 'animate-pulse bg-blue-400' : 'bg-emerald-400'}`} />
                </div>
                <div className="mt-2 h-1 overflow-hidden rounded-full bg-gray-800">
                  <div
                    className="h-full rounded-full bg-indigo-500 transition-all"
                    style={{ width: `${Math.min(100, ((displayedGoalRound ?? 1) / Math.max(1, goalProgress.maxTurns)) * 100)}%` }}
                  />
                </div>
                {goalProgress.lastReason && (
                  <div className="mt-2 line-clamp-3 text-[10px] leading-relaxed text-gray-400">
                    评估器：{goalProgress.lastReason}
                  </div>
                )}
              </section>
            )}
            <section className="rounded-lg border border-gray-800 bg-gray-900/55 p-3">
              <div className="flex items-start justify-between gap-2">
                <div className="min-w-0">
                  <div className="truncate text-xs font-medium text-gray-100" title={job.url}>{job.url}</div>
                  <div className="mt-1 line-clamp-2 text-[11px] text-gray-500">{job.goal}</div>
                </div>
                <div className="flex shrink-0 items-center gap-1.5">
                  <span className={`rounded-full border px-2 py-0.5 text-[10px] ${statusClass(job.status)}`}>
                    {STAGE_LABELS[job.stage] || job.stage}
                  </span>
                </div>
              </div>
              <div className="mt-2 grid grid-cols-3 gap-2 text-center text-[10px]">
                <div className="rounded bg-gray-950/70 px-2 py-1.5 text-gray-400">
                  <Activity size={12} className="mx-auto mb-0.5 text-blue-400" />
                  {job.steps}/{job.max_steps} 步
                </div>
                <div className="rounded bg-gray-950/70 px-2 py-1.5 text-gray-400">
                  <Clock size={12} className="mx-auto mb-0.5 text-amber-400" />
                  {TERMINAL.has(job.status) ? '已结束' : '运行中'}
                </div>
                <div className="rounded bg-gray-950/70 px-2 py-1.5 text-gray-400">
                  <CheckCircle2 size={12} className="mx-auto mb-0.5 text-emerald-400" />
                  {job.actions} 动作
                </div>
              </div>
            </section>

            {telemetry.length > 0 && (
              <section className="rounded-lg border border-amber-500/20 bg-amber-500/5 p-3">
                <div className="mb-2 flex items-center gap-1.5 text-xs font-medium text-amber-300">
                  <AlertCircle size={13} />运行时信号
                </div>
                <div className="flex flex-wrap gap-1.5">
                  {telemetry.map(([name, entries]) => (
                    <span key={name} className="rounded border border-amber-500/20 bg-gray-950/60 px-2 py-1 text-[10px] text-gray-400">
                      {name.replaceAll('_', ' ')}: {entries.length}
                    </span>
                  ))}
                </div>
              </section>
            )}

            {(displayedRun?.report || job.report) && (
              <section className="rounded-lg border border-emerald-500/20 bg-emerald-500/5">
                <div className="flex items-center gap-1.5 border-b border-emerald-500/15 px-3 py-2 text-xs font-medium text-emerald-300">
                  <FileText size={13} />审查报告
                </div>
                <div className="prose prose-invert prose-sm max-w-none px-3 py-2 text-xs text-gray-300">
                  <ReactMarkdown remarkPlugins={[remarkGfm]}>{displayedRun?.report || job.report}</ReactMarkdown>
                </div>
              </section>
            )}

            {(displayedRun?.findings.length ?? 0) > 0 && (
              <section className="rounded-lg border border-amber-500/20 bg-amber-500/5 p-3">
                <div className="mb-2 flex items-center gap-1.5 text-xs font-medium text-amber-300">
                  <AlertCircle size={13} />结构化发现
                </div>
                <div className="space-y-2">
                  {displayedRun!.findings.map((finding) => (
                    <div key={finding.fingerprint || `${finding.scenario_id}-${finding.title}`} className="rounded border border-gray-800 bg-gray-950/55 p-2">
                      <div className="flex items-center gap-2">
                        <span className="rounded bg-gray-800 px-1.5 py-0.5 text-[9px] uppercase text-gray-300">{finding.severity}</span>
                        <span className="text-[11px] font-medium text-gray-200">{finding.title}</span>
                      </div>
                      {finding.actual && <div className="mt-1 text-[10px] leading-relaxed text-gray-500">{finding.actual}</div>}
                    </div>
                  ))}
                </div>
              </section>
            )}

            {(displayedRun?.evidence.length ?? 0) > 0 && (
              <section className="flex flex-wrap gap-1.5">
                {displayedRun!.evidence.map((item) => (
                  <button
                    key={item.id}
                    type="button"
                    onClick={() => void download(item.name)}
                    className="inline-flex items-center gap-1 rounded border border-gray-700 bg-gray-900 px-2 py-1 text-[10px] text-gray-400 hover:border-gray-600 hover:text-gray-200"
                  >
                    <Download size={10} />{item.name}
                  </button>
                ))}
              </section>
            )}
          </>
        )}
      </div>}
    </aside>
  );

  return displayMode === 'floating' ? createPortal(panel, document.body) : panel;
}
