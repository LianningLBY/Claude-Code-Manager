import { useState, useEffect, useCallback, useRef } from 'react';
import { api } from '../api/client';
import type { Task, Project, TagItem, PRReviewResult } from '../api/client';
import { useWebSocket } from '../hooks/useWebSocket';
import { TaskForm } from '../components/Tasks/TaskForm';
import { TaskList } from '../components/Tasks/TaskList';
import {
  canControlTask,
  canManageTaskShare,
  readStoredUserIdentity,
} from '../components/Tasks/taskSharePermissions';
import { ChatView } from '../components/Chat/ChatView';
import { LoopChatView } from '../components/Chat/LoopChatView';
import { ProjectSelect } from '../components/ProjectSelect';
import { resolveTagColor } from '../components/TagColors';
import { ChevronLeft, ChevronRight, ChevronDown, Filter, PanelLeftClose, PanelLeftOpen, Search, X, Star, Archive, ArchiveRestore, Share2, Pin } from '../components/icons';
import { PluginsBadge, SubAgentsBadge } from '../components/Tasks/TaskBadges';
import { TAG_COLOR_OPTIONS } from '../components/TagColors';
import { mergeVisibleTaskOrder, useTaskReorder } from '../hooks/useTaskReorder';
import { useTaskSearch } from '../hooks/useTaskSearch';
import { TeamShareModal } from '../components/TeamShareModal';
import { getTaskStatusLabel } from '../components/Tasks/taskStatus';
import { PRReviewResultCard } from '../components/PRReview/PRReviewResultCard';
import { canonicalGitHubPRUrl } from '../components/PRReview/githubUrls';

const PAGE_SIZE = 20;
const PR_RESULTS_PAGE_SIZE = 20;

function isDeliveryOwnedTask(task: Task): boolean {
  return task.mode === 'delivery_loop' || task.delivery_run_id != null;
}

function taskStatusColorKey(task: Task): string {
  if (task.background_active) return 'background';
  if (isDeliveryOwnedTask(task)) {
    if (task.delivery_activity === 'running') return 'executing';
    if (task.delivery_activity === 'waiting') return 'delivery_waiting';
    if (task.delivery_activity === 'paused') return 'delivery_paused';
  }
  return task.status;
}

interface TasksPageProps {
  chatTaskId: number | null;
  onChatTaskChange: (id: number | null) => void;
}

export function TasksPage({ chatTaskId, onChatTaskChange }: TasksPageProps) {
  const [tasks, setTasks] = useState<Task[]>([]);
  const [prReviewResults, setPRReviewResults] = useState<PRReviewResult[]>([]);
  const [prResultsError, setPRResultsError] = useState<string | null>(null);
  const [prResultsRefreshing, setPRResultsRefreshing] = useState(false);
  const [prResultsPage, setPRResultsPage] = useState(1);
  const [prResultsHasNext, setPRResultsHasNext] = useState(false);
  const [rerunPendingKey, setRerunPendingKey] = useState<string | null>(null);
  const [rerunErrors, setRerunErrors] = useState<Record<string, {
    sourceReviewId: number;
    sourceHeadSha: string;
    message: string;
  }>>({});
  const [rerunStarted, setRerunStarted] = useState<Record<string, {
    sourceReviewId: number;
    sourceHeadSha: string;
    reviewId: number;
    message: string;
  }>>({});
  const rerunIdempotencyRef = useRef(new Map<string, {
    sourceReviewId: number;
    headSha: string;
    key: string;
  }>());
  const [taskPrefill, setTaskPrefill] = useState<{ key: string; description: string } | null>(null);
  const [, setAllTasks] = useState<Task[]>([]);
  const [totalCount, setTotalCount] = useState(0);
  const [page, setPage] = useState(1);
  const [projects, setProjects] = useState<Project[]>([]);
  const [statusFilters, setStatusFilters] = useState<string[]>([]);
  const [tagFilters, setTagFilters] = useState<string[]>([]);
  const [projectFilter, setProjectFilter] = useState<number | undefined>(undefined);
  const [starredFilter, setStarredFilter] = useState(false);
  const [unreadFilter, setUnreadFilter] = useState(false);
  const [showArchived, setShowArchived] = useState(false);
  // Regex search over task titles (falls back to plain substring on invalid regex)
  const [showSearch, setShowSearch] = useState(false);
  const [searchQuery, setSearchQuery] = useState('');
  const [searchResults, setSearchResults] = useTaskSearch(searchQuery, showArchived);
  const searchInputRef = useRef<HTMLInputElement>(null);
  const [tagItems, setTagItems] = useState<TagItem[]>([]);
  const [chatTask, setChatTask] = useState<Task | null>(null);
  const [teamSharingTask, setTeamSharingTask] = useState<Task | null>(null);
  const chatTaskRef = useRef<Task | null>(null);
  chatTaskRef.current = chatTask;
  const chatTaskIdRef = useRef(chatTaskId);
  chatTaskIdRef.current = chatTaskId;
  const skipFreezeOnce = useRef(false);

  const setChatTaskWrapped = useCallback((t: Task | null) => {
    setChatTask(t);
    onChatTaskChange(t?.id ?? null);
  }, [onChatTaskChange]);

  const applyReturnedTaskUpdate = useCallback((updated: Task) => {
    // This callback currently handles the attention-tag editor. Patch only the
    // returned field so a PUT response cannot overwrite a newer WS status.
    const patchTask = (task: Task): Task => task.id === updated.id
      ? { ...task, attention_tag: updated.attention_tag }
      : task;
    const patchList = (list: Task[]): Task[] => list.map(patchTask);

    setTasks(patchList);
    setAllTasks(patchList);
    setSearchResults((current) => current ? patchList(current) : current);
    setChatTask((current) => current ? patchTask(current) : current);
  }, [setSearchResults]);

  useEffect(() => {
    const teamHandler = (e: Event) => {
      const task = (e as CustomEvent).detail?.task;
      if (task && canManageTaskShare(task, readStoredUserIdentity())) {
        setTeamSharingTask(task);
      }
    };
    window.addEventListener('ccm-team-share-task', teamHandler);
    return () => window.removeEventListener('ccm-team-share-task', teamHandler);
  }, []);

  const [autoSortOnAccess, setAutoSortOnAccess] = useState(true);
  useEffect(() => {
    api.getRuntimeSettings().then((s) => setAutoSortOnAccess(s.auto_sort_on_access)).catch(() => {});
  }, []);
  const refreshRef = useRef<() => void>(() => {});
  const handleGlobalWs = useCallback((raw: Record<string, unknown>) => {
    const msg = raw as { channel?: string; data?: Record<string, unknown> };
    if (msg.channel === 'system' && msg.data?.event === 'runtime_settings_changed') {
      setAutoSortOnAccess(Boolean(msg.data.auto_sort_on_access));
      return;
    }
    // Real-time status updates. This also keeps tasks that fell out of the
    // current page/filter fresh while the chat-open freeze preserves them
    // in the sidebar with last-known data (they'd otherwise show a stale
    // status until the chat is closed).
    const data = msg.data;
    const event = msg.channel === 'tasks' ? data?.event : undefined;
    if (
      data
      && (
        event === 'status_change'
        || event === 'background_activity'
        || event === 'sub_agent_count'
        || event === 'plan_stage_change'
        || event === 'plan_ready'
      )
    ) {
      const taskId = Number(data.task_id);
      if (!Number.isSafeInteger(taskId) || taskId <= 0) return;

      const newStatus = event === 'plan_ready'
        ? 'plan_review'
        : (
          event === 'status_change'
          && typeof data.new_status === 'string'
          && data.new_status
            ? data.new_status
            : undefined
        );
      const backgroundActive = typeof data.background_active === 'boolean'
        ? data.background_active
        : undefined;
      const activeSubAgents = event === 'sub_agent_count'
        && Number.isSafeInteger(Number(data.active_sub_agents))
        && Number(data.active_sub_agents) >= 0
        ? Number(data.active_sub_agents)
        : undefined;
      const planStage = event === 'plan_stage_change'
        && typeof data.plan_stage === 'string'
        ? data.plan_stage
        : undefined;
      const planStageRound = event === 'plan_stage_change'
        && Number.isSafeInteger(Number(data.plan_stage_round))
        ? Number(data.plan_stage_round)
        : undefined;
      const planStageProvider = event === 'plan_stage_change'
        && typeof data.plan_stage_provider === 'string'
        ? data.plan_stage_provider
        : undefined;
      const planStageModel = event === 'plan_stage_change'
        && (typeof data.plan_stage_model === 'string' || data.plan_stage_model === null)
        ? data.plan_stage_model
        : undefined;
      const planStageEffort = event === 'plan_stage_change'
        && (typeof data.plan_stage_effort === 'string' || data.plan_stage_effort === null)
        ? data.plan_stage_effort
        : undefined;
      const planStageRouteSlot = event === 'plan_stage_change'
        && (data.plan_stage_route_slot === 'primary'
          || data.plan_stage_route_slot === 'fallback'
          || data.plan_stage_route_slot === null)
        ? data.plan_stage_route_slot
        : undefined;
      if (
        newStatus === undefined
        && backgroundActive === undefined
        && activeSubAgents === undefined
        && planStage === undefined
        && planStageRound === undefined
        && planStageProvider === undefined
        && planStageModel === undefined
        && planStageEffort === undefined
        && planStageRouteSlot === undefined
      ) return;

      const patchTask = (task: Task): Task => {
        if (task.id !== taskId) return task;
        const statusChanged = newStatus !== undefined && task.status !== newStatus;
        const backgroundChanged = (
          backgroundActive !== undefined
          && task.background_active !== backgroundActive
        );
        const subAgentCountChanged = (
          activeSubAgents !== undefined
          && task.active_sub_agents !== activeSubAgents
        );
        const planStageChanged = (
          planStage !== undefined
          && task.plan_stage !== planStage
        );
        const planStageRoundChanged = (
          planStageRound !== undefined
          && task.plan_stage_round !== planStageRound
        );
        const planRouteChanged = (
          (planStageProvider !== undefined && task.plan_stage_provider !== planStageProvider)
          || (planStageModel !== undefined && task.plan_stage_model !== planStageModel)
          || (planStageEffort !== undefined && task.plan_stage_effort !== planStageEffort)
          || (planStageRouteSlot !== undefined && task.plan_stage_route_slot !== planStageRouteSlot)
        );
        if (
          !statusChanged
          && !backgroundChanged
          && !subAgentCountChanged
          && !planStageChanged
          && !planStageRoundChanged
          && !planRouteChanged
        ) return task;
        return {
          ...task,
          ...(newStatus !== undefined ? { status: newStatus } : {}),
          ...(backgroundActive !== undefined ? { background_active: backgroundActive } : {}),
          ...(activeSubAgents !== undefined ? { active_sub_agents: activeSubAgents } : {}),
          ...(planStage !== undefined ? { plan_stage: planStage } : {}),
          ...(planStageRound !== undefined ? { plan_stage_round: planStageRound } : {}),
          ...(planStageProvider !== undefined ? { plan_stage_provider: planStageProvider } : {}),
          ...(planStageModel !== undefined ? { plan_stage_model: planStageModel } : {}),
          ...(planStageEffort !== undefined ? { plan_stage_effort: planStageEffort } : {}),
          ...(planStageRouteSlot !== undefined ? { plan_stage_route_slot: planStageRouteSlot } : {}),
        };
      };
      const patchList = (list: Task[]) => {
        const index = list.findIndex((task) => task.id === taskId);
        if (index < 0) return list;
        const patched = patchTask(list[index]);
        if (patched === list[index]) return list;
        const next = [...list];
        next[index] = patched;
        return next;
      };

      setTasks(patchList);
      setAllTasks(patchList);
      setSearchResults((prev) => (prev ? patchList(prev) : prev));
      setChatTask((prev) => (prev ? patchTask(prev) : prev));
      if (event === 'status_change') {
        refreshRef.current();
      }
    }
  }, [setSearchResults]);
  useWebSocket(['system', 'tasks'], handleGlobalWs);

  const prResultsMountedRef = useRef(false);
  const prResultsMountGenerationRef = useRef(0);
  const prResultsPageRef = useRef(prResultsPage);
  prResultsPageRef.current = prResultsPage;
  const prResultIdentityRef = useRef(new Map<string, { reviewId: number | null; headSha: string | null }>());
  const prResultsCoordinatorRef = useRef<{
    running: boolean;
    trailing: boolean;
    nextSequence: number;
    appliedSequence: number;
    waiters: Array<(ok: boolean) => void>;
  }>({
    running: false,
    trailing: false,
    nextSequence: 0,
    appliedSequence: 0,
    waiters: [],
  });

  const refreshPRReviewResults = useCallback((): Promise<boolean> => new Promise((resolve) => {
    const coordinator = prResultsCoordinatorRef.current;
    coordinator.waiters.push(resolve);
    if (coordinator.running) {
      coordinator.trailing = true;
      return;
    }
    coordinator.running = true;
    void (async () => {
      let lastRequestOk = false;
      if (prResultsMountedRef.current) setPRResultsRefreshing(true);
      do {
        coordinator.trailing = false;
        const sequence = ++coordinator.nextSequence;
        const mountGeneration = prResultsMountGenerationRef.current;
        const requestedPage = prResultsPageRef.current;
        try {
          const results = await api.getPRReviewResults(requestedPage, PR_RESULTS_PAGE_SIZE);
          lastRequestOk = true;
          if (
            prResultsMountedRef.current
            && mountGeneration === prResultsMountGenerationRef.current
            && requestedPage === prResultsPageRef.current
            && sequence > coordinator.appliedSequence
          ) {
            coordinator.appliedSequence = sequence;
            const byResultKey = new Map<string, PRReviewResult>();
            results.forEach((result) => {
              if (!byResultKey.has(result.result_key)) {
                byResultKey.set(result.result_key, result);
              }
            });
            const nextResults = [...byResultKey.values()];
            prResultIdentityRef.current = new Map(nextResults.map((result) => [result.result_key, {
              reviewId: result.review_id,
              headSha: result.head_sha,
            }]));
            setPRReviewResults(nextResults);
            setPRResultsHasNext(results.length === PR_RESULTS_PAGE_SIZE);
            setRerunErrors((current) => Object.fromEntries(Object.entries(current).filter(([key, error]) => {
              const identity = prResultIdentityRef.current.get(key);
              return identity?.reviewId === error.sourceReviewId && identity.headSha === error.sourceHeadSha;
            })));
            setRerunStarted((current) => Object.fromEntries(Object.entries(current).filter(([key, started]) => {
              const identity = prResultIdentityRef.current.get(key);
              return identity?.reviewId === started.sourceReviewId && identity.headSha === started.sourceHeadSha;
            })));
            setPRResultsError(null);
          }
        } catch (error) {
          lastRequestOk = false;
          if (
            prResultsMountedRef.current
            && mountGeneration === prResultsMountGenerationRef.current
            && requestedPage === prResultsPageRef.current
            && sequence > coordinator.appliedSequence
          ) {
            coordinator.appliedSequence = sequence;
            setPRResultsError(error instanceof Error ? error.message : String(error));
          }
        }
      } while (coordinator.trailing && prResultsMountedRef.current);
      coordinator.running = false;
      if (prResultsMountedRef.current) setPRResultsRefreshing(false);
      const waiters = coordinator.waiters.splice(0);
      waiters.forEach((waiter) => waiter(lastRequestOk));
    })();
  }), []);

  useEffect(() => {
    prResultsMountedRef.current = true;
    prResultsMountGenerationRef.current += 1;
    void refreshPRReviewResults();
    const interval = window.setInterval(refreshPRReviewResults, 15000);
    return () => {
      window.clearInterval(interval);
      prResultsMountedRef.current = false;
      prResultsMountGenerationRef.current += 1;
    };
  }, [prResultsPage, refreshPRReviewResults]);

  const openPRReviewDetail = useCallback((result: PRReviewResult, reviewId?: number) => {
    const targetReviewId = reviewId ?? result.review_id;
    if (targetReviewId == null) return;
    window.location.hash = `#/pr-monitor?repo=${result.repo_id}&review=${targetReviewId}`;
  }, []);

  const createPRFollowUp = useCallback((result: PRReviewResult) => {
    const prUrl = canonicalGitHubPRUrl(result.pr_url, result.repo_full_name, result.pr_number);
    const verdict = result.aggregate_verdict === 'changes_required'
      ? 'Changes required'
      : result.aggregate_verdict === 'pass'
        ? 'Pass'
        : 'Unavailable';
    setTaskPrefill({
      key: `${result.result_key}:${Date.now()}`,
      description: [
        `Follow up PR ${result.repo_full_name}#${result.pr_number}: ${result.pr_title}`,
        prUrl ? `PR: ${prUrl}` : null,
        `Reviewed head: ${result.head_sha || 'unknown'}`,
        `Code verdict: ${verdict}`,
        result.display_summary ? `Review summary: ${result.display_summary}` : null,
      ].filter(Boolean).join('\n'),
    });
  }, []);

  const rerunPRReview = useCallback(async (result: PRReviewResult) => {
    if (result.review_id == null || !result.head_sha || rerunPendingKey != null) return;
    const resultKey = result.result_key;
    setRerunPendingKey(resultKey);
    setRerunErrors((current) => {
      const next = { ...current };
      delete next[resultKey];
      return next;
    });
    const priorAttempt = rerunIdempotencyRef.current.get(resultKey);
    const attempt = priorAttempt
      && priorAttempt.sourceReviewId === result.review_id
      && priorAttempt.headSha === result.head_sha
      ? priorAttempt
      : {
          sourceReviewId: result.review_id,
          headSha: result.head_sha,
          key: typeof crypto.randomUUID === 'function'
            ? crypto.randomUUID()
            : `pr-rerun-${result.review_id}-${Date.now()}`,
        };
    rerunIdempotencyRef.current.set(resultKey, attempt);
    try {
      const startedReview = await api.rerunPRReview(result.review_id, result.head_sha, attempt.key);
      rerunIdempotencyRef.current.delete(resultKey);
      const currentIdentity = prResultIdentityRef.current.get(resultKey);
      if (
        currentIdentity?.reviewId !== result.review_id
        || currentIdentity.headSha !== result.head_sha
      ) {
        setRerunPendingKey(null);
        return;
      }
      setRerunStarted((current) => ({
        ...current,
        [resultKey]: {
          sourceReviewId: result.review_id!,
          sourceHeadSha: result.head_sha!,
          reviewId: startedReview.id,
          message: 'Exact-head review started.',
        },
      }));
    } catch (error) {
      const currentIdentity = prResultIdentityRef.current.get(resultKey);
      if (
        currentIdentity?.reviewId === result.review_id
        && currentIdentity.headSha === result.head_sha
      ) {
        setRerunErrors((current) => ({
          ...current,
          [resultKey]: {
            sourceReviewId: result.review_id!,
            sourceHeadSha: result.head_sha!,
            message: error instanceof Error ? error.message : String(error),
          },
        }));
      }
      setRerunPendingKey(null);
      return;
    }
    const refreshed = await refreshPRReviewResults();
    if (!refreshed) {
      setRerunStarted((current) => ({
        ...current,
        [resultKey]: current[resultKey] ? {
          ...current[resultKey],
          message: 'Exact-head review started. The result refresh was delayed and will retry automatically.',
        } : current[resultKey],
      }));
    }
    setRerunPendingKey(null);
  }, [refreshPRReviewResults, rerunPendingKey]);

  const [isWide, setIsWide] = useState(() => window.innerWidth >= 1280);
  useEffect(() => {
    const mq = window.matchMedia('(min-width: 1280px)');
    const handler = (e: MediaQueryListEvent) => setIsWide(e.matches);
    mq.addEventListener('change', handler);
    return () => mq.removeEventListener('change', handler);
  }, []);

  const totalPages = Math.max(1, Math.ceil(totalCount / PAGE_SIZE));
  const statusFilterParam = statusFilters.length > 0 ? statusFilters.join(',') : undefined;

  const refresh = useCallback(async () => {
    try {
      const offset = (page - 1) * PAGE_SIZE;
      const [filtered, count, all, projs, tags] = await Promise.all([
        api.listTasks(statusFilterParam, false, projectFilter, starredFilter || undefined, PAGE_SIZE, offset, showArchived, unreadFilter || undefined),
        api.countTasks(statusFilterParam, false, projectFilter, starredFilter || undefined, showArchived, unreadFilter || undefined),
        api.listTasks(undefined, false, undefined, undefined, PAGE_SIZE, 0, showArchived),
        api.listProjects(),
        api.listTags(),
      ]);
      // When chat is open, freeze sidebar order: update task data in place and
      // append genuinely-new tasks at the end, but NEVER drop a task just because
      // it fell off page-1. With 20/page over hundreds of tasks, an active task
      // (loop/executing) bumping last_accessed_at constantly crosses the page-1
      // boundary; the old code dropped the task that fell off and re-appended the
      // returning one at the end, so the list churned/flickered every few seconds.
      // Keeping fallen-off tasks in place (with last-known data until they return)
      // is what "freeze" should mean — the full server order is restored on close.
      if (chatTaskRef.current && !skipFreezeOnce.current) {
        setTasks(prev => {
          const byId = new Map(filtered.map(t => [t.id, t]));
          const updated = prev.map(t => byId.get(t.id) ?? t);
          const prevIds = new Set(prev.map(t => t.id));
          const added = filtered.filter(t => !prevIds.has(t.id));
          return [...updated, ...added];
        });
      } else {
        skipFreezeOnce.current = false;
        setTasks(filtered);
      }
      setTotalCount(count.total);
      setAllTasks(all);
      setProjects(projs);
      setTagItems(tags);
      // Resolve chatTaskId from URL on first load, or update open chatTask.
      // chatTaskId comes from URL hash — null after onBack.
      // chatTaskRef tracks the currently open chat — used to refresh its data.
      const currentChatTaskId = chatTaskIdRef.current;
      if (currentChatTaskId) {
        const pool = [...filtered, ...all];
        let found = pool.find((t) => t.id === currentChatTaskId);
        if (!found) {
          try { found = await api.getTask(currentChatTaskId); } catch { /* task may not exist */ }
        }
        if (found) setChatTaskWrapped(found);
      } else if (chatTaskRef.current) {
        // Chat is open but not from URL (e.g. clicked from list) — refresh its data
        const found = [...filtered, ...all].find((t) => t.id === chatTaskRef.current!.id);
        if (found) setChatTask(found);
      }
    } catch (e) {
      console.error('Failed to load tasks:', e);
    }
  }, [statusFilterParam, showArchived, projectFilter, starredFilter, unreadFilter, page, setChatTaskWrapped]);

  refreshRef.current = refresh;

  useEffect(() => {
    refresh();
    const interval = setInterval(refresh, 5000);
    return () => clearInterval(interval);
  }, [refresh]);

  // Reset to page 1 when filters change
  const prevFilter = useRef({ statusFilterParam, showArchived, projectFilter, starredFilter, unreadFilter });
  useEffect(() => {
    const prev = prevFilter.current;
    if (prev.statusFilterParam !== statusFilterParam || prev.showArchived !== showArchived || prev.projectFilter !== projectFilter || prev.starredFilter !== starredFilter || prev.unreadFilter !== unreadFilter) {
      setPage(1);
      skipFreezeOnce.current = true;
      prevFilter.current = { statusFilterParam, showArchived, projectFilter, starredFilter, unreadFilter };
    }
  }, [statusFilterParam, showArchived, projectFilter, starredFilter, unreadFilter]);

  const statusOptions = ['pending', 'in_progress', 'executing', 'waiting_capability', 'delivery_waiting', 'plan_review', 'completed', 'superseded', 'failed'];
  const [showFilterDropdown, setShowFilterDropdown] = useState(false);
  const filterDropdownRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!showFilterDropdown) return;
    const handleClick = (e: MouseEvent) => {
      if (filterDropdownRef.current && !filterDropdownRef.current.contains(e.target as Node)) {
        setShowFilterDropdown(false);
      }
    };
    document.addEventListener('mousedown', handleClick);
    return () => document.removeEventListener('mousedown', handleClick);
  }, [showFilterDropdown]);

  const statusLabels: Record<string, string> = {
    pending: 'Pending',
    in_progress: 'In Progress',
    executing: 'Executing',
    waiting_capability: 'Waiting Capability',
    delivery_waiting: 'Delivery Waiting',
    plan_review: 'Plan Review',
    completed: 'Completed',
    superseded: 'Superseded',
    failed: 'Failed',
  };

  const statusDotColors: Record<string, string> = {
    pending: 'bg-yellow-500',
    in_progress: 'bg-blue-500',
    executing: 'bg-blue-400',
    waiting_capability: 'bg-violet-400',
    delivery_waiting: 'bg-indigo-500',
    plan_review: 'bg-purple-500',
    completed: 'bg-green-500',
    superseded: 'bg-gray-500',
    failed: 'bg-red-500',
  };

  const activeFilterCount = statusFilters.length + (starredFilter ? 1 : 0) + (unreadFilter ? 1 : 0) + (showArchived ? 1 : 0) + tagFilters.length;

  const visibleProjects = projects.filter((p) => p.show_in_selector);

  // Collect all unique tags from visible projects + tag registry
  const allProjectTags = Array.from(new Set([...visibleProjects.flatMap((p) => p.tags), ...tagItems.map((t) => t.name)])).sort();

  // Build tag color map
  const tagColorMap: Record<string, string> = {};
  for (const t of tagItems) tagColorMap[t.name] = t.color;

  // Projects filtered by tag (for the project dropdown)
  const tagFilteredProjects = tagFilters.length > 0
    ? visibleProjects.filter((p) => tagFilters.some((t) => p.tags.includes(t)))
    : visibleProjects;

  const splitMode = isWide && chatTask;

  const filteredTasks = tagFilters.length > 0
    ? tasks.filter((t) => {
        if (!t.project_id) return false;
        const proj = projects.find((p) => p.id === t.project_id);
        return proj ? tagFilters.some((tag) => proj.tags.includes(tag)) : false;
      })
    : tasks;
  const filteredSearchResults = searchResults;

  // 侧边栏拖拽排序（与主列表同一套逻辑）
  const sidebarTasks = filteredSearchResults ?? filteredTasks;
  const reorderRefresh = useCallback((optimistic?: Task[]) => {
    if (optimistic) {
      setTasks((current) => mergeVisibleTaskOrder(current, optimistic));
      setAllTasks((current) => mergeVisibleTaskOrder(current, optimistic));
      setSearchResults((current) => current ? optimistic : current);
      return;
    }
    skipFreezeOnce.current = true;
    void refresh();
  }, [refresh, setSearchResults]);
  const sidebarReorder = useTaskReorder(sidebarTasks, reorderRefresh, autoSortOnAccess);

  const handleOpenChat = useCallback((t: Task) => {
    setChatTaskWrapped(t);
    if (t.has_unread) {
      api.markTaskRead(t.id).catch(() => {});
    }
  }, [setChatTaskWrapped]);

  // Filter / Projects / Search controls — shared between the full task list
  // and the split-mode sidebar
  const filterControls = (
      <div className="flex gap-2 flex-wrap items-center">
        <div className="relative" ref={filterDropdownRef}>
          <button
            onClick={() => setShowFilterDropdown(!showFilterDropdown)}
            className={`flex items-center gap-1.5 px-3 py-1.5 rounded text-xs font-medium transition-colors ${
              activeFilterCount > 0
                ? 'bg-indigo-600 text-white'
                : 'bg-gray-800 text-gray-400 hover:bg-gray-700'
            }`}
          >
            <Filter size={12} />
            Filter
            {activeFilterCount > 0 && (
              <span className="bg-white/20 text-white px-1.5 rounded-full text-[10px]">{activeFilterCount}</span>
            )}
            <ChevronDown size={12} className={`transition-transform ${showFilterDropdown ? 'rotate-180' : ''}`} />
          </button>
          {showFilterDropdown && (
            <div className="absolute top-full mt-1 left-0 bg-gray-900 border border-gray-700 rounded-lg shadow-xl z-30 min-w-[180px] py-1 max-h-72 overflow-y-auto">
              {/* Status section */}
              <div className="px-3 py-1 text-[10px] text-gray-500 uppercase tracking-wider">Status</div>
              {statusOptions.map((f) => {
                const checked = statusFilters.includes(f);
                return (
                  <button
                    key={f}
                    onClick={() => setStatusFilters(checked ? statusFilters.filter(s => s !== f) : [...statusFilters, f])}
                    className={`w-full flex items-center gap-2 px-3 py-1.5 text-xs text-left transition-colors ${
                      checked ? 'bg-indigo-600/20 text-indigo-300' : 'text-gray-300 hover:bg-gray-800'
                    }`}
                  >
                    <span className={`w-3 h-3 rounded border flex items-center justify-center text-[8px] ${checked ? 'bg-indigo-500 border-indigo-500 text-white' : 'border-gray-600'}`}>
                      {checked && '✓'}
                    </span>
                    <span className={`w-2 h-2 rounded-full ${statusDotColors[f] || ''}`} />
                    {statusLabels[f]}
                  </button>
                );
              })}

              <div className="border-t border-gray-700 my-1" />

              {/* Toggle filters */}
              <div className="px-3 py-1 text-[10px] text-gray-500 uppercase tracking-wider">Filters</div>
              <button
                onClick={() => setStarredFilter(!starredFilter)}
                className={`w-full flex items-center gap-2 px-3 py-1.5 text-xs text-left transition-colors ${
                  starredFilter ? 'bg-yellow-600/20 text-yellow-300' : 'text-gray-300 hover:bg-gray-800'
                }`}
              >
                <span className={`w-3 h-3 rounded border flex items-center justify-center text-[8px] ${starredFilter ? 'bg-yellow-500 border-yellow-500 text-white' : 'border-gray-600'}`}>
                  {starredFilter && '✓'}
                </span>
                ★ Starred
              </button>
              <button
                onClick={() => setUnreadFilter(!unreadFilter)}
                className={`w-full flex items-center gap-2 px-3 py-1.5 text-xs text-left transition-colors ${
                  unreadFilter ? 'bg-indigo-600/20 text-indigo-300' : 'text-gray-300 hover:bg-gray-800'
                }`}
              >
                <span className={`w-3 h-3 rounded border flex items-center justify-center text-[8px] ${unreadFilter ? 'bg-indigo-500 border-indigo-500 text-white' : 'border-gray-600'}`}>
                  {unreadFilter && '✓'}
                </span>
                Unread
              </button>
              <button
                onClick={() => setShowArchived(!showArchived)}
                className={`w-full flex items-center gap-2 px-3 py-1.5 text-xs text-left transition-colors ${
                  showArchived ? 'bg-amber-600/20 text-amber-300' : 'text-gray-300 hover:bg-gray-800'
                }`}
              >
                <span className={`w-3 h-3 rounded border flex items-center justify-center text-[8px] ${showArchived ? 'bg-amber-500 border-amber-500 text-white' : 'border-gray-600'}`}>
                  {showArchived && '✓'}
                </span>
                Archived
              </button>

              {/* Tags section */}
              {allProjectTags.length > 0 && (
                <>
                  <div className="border-t border-gray-700 my-1" />
                  <div className="px-3 py-1 text-[10px] text-gray-500 uppercase tracking-wider">Tags</div>
                  {allProjectTags.map((tag) => {
                    const c = resolveTagColor(tag, tagColorMap[tag]);
                    const active = tagFilters.includes(tag);
                    return (
                      <button
                        key={tag}
                        onClick={() => {
                          const next = active ? tagFilters.filter((t) => t !== tag) : [...tagFilters, tag];
                          setTagFilters(next);
                          if (next.length > 0 && projectFilter !== undefined) {
                            const filtered = visibleProjects.filter((p) => next.some((t) => p.tags.includes(t)));
                            if (!filtered.some((p) => p.id === projectFilter)) {
                              setProjectFilter(undefined);
                            }
                          }
                        }}
                        className={`w-full flex items-center gap-2 px-3 py-1.5 text-xs text-left transition-colors ${
                          active ? `${c.bg} ${c.text}` : 'text-gray-300 hover:bg-gray-800'
                        }`}
                      >
                        <span className={`w-3 h-3 rounded border flex items-center justify-center text-[8px] ${active ? `${c.dot.replace('bg-', 'bg-')} border-current text-white` : 'border-gray-600'}`}>
                          {active && '✓'}
                        </span>
                        <span className={`w-2 h-2 rounded-full ${c.dot} ${active ? '' : 'opacity-60'}`} />
                        {tag}
                      </button>
                    );
                  })}
                </>
              )}

              {/* Clear all */}
              {activeFilterCount > 0 && (
                <>
                  <div className="border-t border-gray-700 my-1" />
                  <button
                    onClick={() => { setStatusFilters([]); setStarredFilter(false); setUnreadFilter(false); setShowArchived(false); setTagFilters([]); }}
                    className="w-full px-3 py-1.5 text-xs text-red-400 hover:bg-gray-800 text-left"
                  >
                    Clear all filters
                  </button>
                </>
              )}
            </div>
          )}
        </div>

        <button
          onClick={() => {
            setShowSearch((s) => {
              if (s) setSearchQuery('');
              return !s;
            });
            setTimeout(() => searchInputRef.current?.focus(), 0);
          }}
          className={`flex items-center gap-1 text-xs px-2 py-1.5 rounded border transition-colors ${
            showSearch
              ? 'bg-indigo-600/30 text-indigo-300 border-indigo-500/50 hover:bg-indigo-600/40'
              : 'bg-gray-700 text-gray-400 border-gray-600 hover:bg-gray-600 hover:text-gray-300'
          }`}
          title="Search task titles (regex)"
        >
          <Search size={13} />
        </button>
        {showSearch && (
          <div className="relative flex-1 max-w-xs">
            <input
              ref={searchInputRef}
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              onKeyDown={(e) => { if (e.key === 'Escape') { setSearchQuery(''); setShowSearch(false); } }}
              placeholder="Regex search titles..."
              className="w-full px-3 py-2 pr-8 rounded-lg bg-gray-900 border border-gray-700 text-sm text-gray-200 placeholder-gray-600 focus:border-indigo-500 focus:outline-none"
            />
            {searchQuery && (
              <button
                onClick={() => setSearchQuery('')}
                className="absolute right-2 top-1/2 -translate-y-1/2 text-gray-500 hover:text-gray-300"
              >
                <X size={14} />
              </button>
            )}
          </div>
        )}
        {filteredSearchResults !== null && (
          <span className="text-xs text-gray-500 whitespace-nowrap">{filteredSearchResults.length} match{filteredSearchResults.length === 1 ? '' : 'es'}</span>
        )}

        <ProjectSelect
          projects={tagFilteredProjects}
          value={projectFilter}
          onChange={(v) => setProjectFilter(v ? Number(v) : undefined)}
          placeholder="Projects"
          tagColorMap={tagColorMap}
        />
      </div>
  );

  const showPRReviewResults = prReviewResults.length > 0
    || Boolean(prResultsError)
    || prResultsRefreshing
    || prResultsPage > 1;
  const prReviewResultsContent = (
    <>
      {showPRReviewResults && (
        <section aria-label="PR review results" className="space-y-3">
          <div className="flex items-center justify-between gap-3">
            <div>
              <h2 className="text-sm font-semibold text-gray-200">PR Review Results</h2>
              <p className="text-xs text-gray-500">
                Read-only results in a separate feed; internal Reviewer Tasks stay hidden and Task filters do not apply.
              </p>
            </div>
            <button
              type="button"
              onClick={() => void refreshPRReviewResults()}
              disabled={prResultsRefreshing}
              className="rounded px-2 py-1 text-xs text-gray-400 hover:bg-gray-800 hover:text-gray-200 disabled:cursor-wait disabled:opacity-50"
            >
              {prResultsRefreshing ? 'Refreshing…' : 'Refresh'}
            </button>
          </div>
          {prResultsError && (
            <p role="alert" className="rounded border border-red-500/30 bg-red-500/10 px-3 py-2 text-xs text-red-300">
              Unable to load PR review results: {prResultsError}
            </p>
          )}
          <div className="grid gap-3">
            {prReviewResults.map((result) => {
              const startedReview = rerunStarted[result.result_key];
              const matchingStartedReview = startedReview?.sourceReviewId === result.review_id
                && startedReview.sourceHeadSha === result.head_sha
                ? startedReview
                : null;
              const rerunError = rerunErrors[result.result_key];
              const matchingRerunError = rerunError?.sourceReviewId === result.review_id
                && rerunError.sourceHeadSha === result.head_sha
                ? rerunError.message
                : null;
              return (
                <PRReviewResultCard
                  key={result.result_key}
                  result={result}
                  onOpenDetail={openPRReviewDetail}
                  onCreateFollowUp={createPRFollowUp}
                  onRerun={rerunPRReview}
                  rerunPending={rerunPendingKey === result.result_key}
                  rerunError={matchingRerunError}
                  rerunSuccess={matchingStartedReview ? {
                    reviewId: matchingStartedReview.reviewId,
                    message: matchingStartedReview.message,
                  } : null}
                />
              );
            })}
          </div>
          <div className="flex items-center justify-center gap-3 pt-1">
            <button
              type="button"
              onClick={() => setPRResultsPage((current) => Math.max(1, current - 1))}
              disabled={prResultsPage <= 1 || prResultsRefreshing}
              className="rounded px-2 py-1 text-xs text-gray-400 hover:bg-gray-800 hover:text-gray-200 disabled:opacity-30"
            >
              Previous results
            </button>
            <span className="text-xs text-gray-500">Results page {prResultsPage}</span>
            <button
              type="button"
              onClick={() => setPRResultsPage((current) => current + 1)}
              disabled={!prResultsHasNext || prResultsRefreshing}
              className="rounded px-2 py-1 text-xs text-gray-400 hover:bg-gray-800 hover:text-gray-200 disabled:opacity-30"
            >
              Older results
            </button>
          </div>
        </section>
      )}
    </>
  );

  const taskListContent = (
    <>
      <TaskForm onCreated={refresh} prefill={taskPrefill} />
      {prReviewResultsContent}
      {filterControls}

      <TaskList
        tasks={filteredSearchResults ?? filteredTasks}
        projects={projects}
        onRefresh={refresh}
        onTaskUpdated={applyReturnedTaskUpdate}
        onOpenChat={handleOpenChat}
        activeTaskId={chatTask?.id ?? null}
        autoSortOnAccess={autoSortOnAccess}
        onBeforeArchive={() => { skipFreezeOnce.current = true; }}
        onReorder={reorderRefresh}
      />

      {totalPages > 1 && searchResults === null && (
        <div className="flex items-center justify-center gap-3 py-2">
          <button
            onClick={() => setPage((p) => Math.max(1, p - 1))}
            disabled={page <= 1}
            className="p-1.5 rounded text-gray-400 hover:text-foreground disabled:opacity-30 disabled:cursor-not-allowed"
          >
            <ChevronLeft size={18} />
          </button>
          <span className="text-xs text-gray-400">
            {page} / {totalPages}
            <span className="ml-2 text-gray-600">
              ({totalCount} tasks)
            </span>
          </span>
          <button
            onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
            disabled={page >= totalPages}
            className="p-1.5 rounded text-gray-400 hover:text-foreground disabled:opacity-30 disabled:cursor-not-allowed"
          >
            <ChevronRight size={18} />
          </button>
        </div>
      )}
    </>
  );

  const chatPanel = chatTask && (
    chatTask.mode === 'loop'
      ? <LoopChatView key={chatTask.id} task={chatTask} onBack={() => setChatTaskWrapped(null)} inline={isWide} />
      : <ChatView
          key={chatTask.id}
          task={chatTask}
          projects={projects}
          onBack={() => setChatTaskWrapped(null)}
          onTaskUpdated={(updated) => {
            if (updated) applyReturnedTaskUpdate(updated);
            else void refresh();
          }}
          onTaskForked={(forked) => {
            setChatTaskWrapped(forked);
            refresh();
          }}
          inline={isWide}
        />
  );

  const [sidebarOpen, setSidebarOpen] = useState(true);
  const [sidebarWidth, setSidebarWidth] = useState(() => {
    const saved = localStorage.getItem('ccm-sidebar-width');
    return saved ? Math.max(200, Math.min(600, Number(saved))) : 260;
  });
  const isDragging = useRef(false);
  const dragStartX = useRef(0);
  const dragStartWidth = useRef(260);

  const handleDragStart = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    isDragging.current = true;
    dragStartX.current = e.clientX;
    dragStartWidth.current = sidebarWidth;
    document.body.style.cursor = 'col-resize';
    document.body.style.userSelect = 'none';

    const onMove = (ev: MouseEvent) => {
      if (!isDragging.current) return;
      const newWidth = Math.max(200, Math.min(600, dragStartWidth.current + ev.clientX - dragStartX.current));
      setSidebarWidth(newWidth);
    };
    const onUp = () => {
      isDragging.current = false;
      document.body.style.cursor = '';
      document.body.style.userSelect = '';
      document.removeEventListener('mousemove', onMove);
      document.removeEventListener('mouseup', onUp);
      setSidebarWidth(w => { localStorage.setItem('ccm-sidebar-width', String(w)); return w; });
    };
    document.addEventListener('mousemove', onMove);
    document.addEventListener('mouseup', onUp);
  }, [sidebarWidth]);

  const teamShareModal = teamSharingTask && (
    <TeamShareModal
      type="task"
      itemId={teamSharingTask.id}
      itemTitle={teamSharingTask.title || `Task #${teamSharingTask.id}`}
      onClose={() => setTeamSharingTask(null)}
    />
  );

  if (splitMode) {
    const currentUserIdentity = readStoredUserIdentity();
    const sidebarStatusColors: Record<string, string> = {
      pending: 'bg-yellow-500',
      in_progress: 'bg-blue-500',
      executing: 'bg-blue-400 animate-pulse',
      waiting_capability: 'bg-violet-400 animate-pulse',
      background: 'bg-teal-400 animate-pulse',
      delivery_waiting: 'bg-indigo-400',
      delivery_paused: 'bg-amber-400',
      plan_review: 'bg-purple-500',
      superseded: 'bg-gray-500',
      completed: 'bg-green-500',
      failed: 'bg-red-500',
      cancelled: 'bg-gray-500',
    };
    return (
      <>
        <div className="flex h-[calc(100vh-49px)] -m-4">
        {sidebarOpen && (
          <div className="shrink-0 flex flex-col border-r border-gray-800 bg-gray-900/50" style={{ width: sidebarWidth }}>
            <div className="px-3 py-2 border-b border-gray-800 flex items-center justify-between shrink-0">
              <span className="text-xs font-medium text-gray-400">Tasks</span>
              <button
                onClick={() => setSidebarOpen(false)}
                className="p-1 text-gray-500 hover:text-gray-300 transition-colors"
                title="Collapse sidebar"
              >
                <PanelLeftClose size={14} />
              </button>
            </div>
            <div className="px-2 py-1.5 border-b border-gray-800 shrink-0">
              {filterControls}
            </div>
            <div className="flex-1 overflow-y-auto min-h-0">
              {sidebarTasks
                .map((t, idx) => {
                const proj = t.project_id ? projects.find((p) => p.id === t.project_id) : undefined;
                const colorDef = proj ? TAG_COLOR_OPTIONS.find((c) => c.key === proj.badge_color) : undefined;
                const deliveryOwned = isDeliveryOwnedTask(t);
                const taskControlAllowed = canControlTask(t);
                return (
                <div
                  key={t.id}
                  {...(deliveryOwned || !taskControlAllowed
                    ? sidebarReorder.dropTargetProps(t, idx)
                    : sidebarReorder.itemProps(t, idx))}
                  data-testid={`task-sidebar-row-${t.id}`}
                  onClick={() => handleOpenChat(t)}
                  className={`w-full text-left px-3 py-2.5 transition-colors border-b border-gray-800/50 cursor-pointer ${
                    sidebarReorder.draggingId === t.id ? 'opacity-40' : ''
                  } ${sidebarReorder.overIndex === idx && sidebarReorder.draggingId !== null && sidebarReorder.draggingId !== t.id ? 'ring-2 ring-inset ring-indigo-400' : ''} ${
                    chatTask?.id === t.id
                      ? 'bg-indigo-900/40 border-l-2 border-l-indigo-400'
                      : 'hover:bg-gray-800/50 border-l-2 border-l-transparent'
                  }`}
                >
                  <div className="flex items-center gap-2">
                    {/* 状态只靠圆点颜色表达（绿=完成 红=失败 蓝=运行 黄=等待） */}
                    <span
                      data-testid={`task-sidebar-status-${t.id}`}
                      className={`w-2 h-2 rounded-full shrink-0 ${sidebarStatusColors[taskStatusColorKey(t)] || 'bg-gray-500'}`}
                      title={getTaskStatusLabel(t)}
                    />
                    <span className={`text-xs truncate flex-1 ${chatTask?.id === t.id ? 'text-foreground font-medium' : 'text-gray-300'}`}>
                      {t.title || t.description?.slice(0, 50) || `Task #${t.id}`}
                    </span>
                    {t.has_unread && <span className="w-1.5 h-1.5 rounded-full bg-indigo-400 shrink-0" />}
                  </div>
                  {t.attention_tag && (
                    <div className="mt-1 ml-4 min-w-0">
                      <span
                        title={t.attention_tag}
                        className="inline-flex max-w-full items-center gap-1 rounded-md border border-amber-400/25 bg-amber-500/15 px-1.5 py-0.5 text-[10px] font-medium text-amber-300"
                      >
                        <Pin size={10} className="shrink-0" />
                        <span className="truncate">{t.attention_tag}</span>
                      </span>
                    </div>
                  )}
                  <div className="flex items-center gap-1.5 mt-1 ml-4 flex-wrap">
                    <span className="text-[10px] text-gray-500">#{t.id}</span>
                    {proj && (
                      <span className={`text-[10px] px-1 rounded font-medium whitespace-nowrap ${colorDef ? `${colorDef.bg} ${colorDef.text}` : 'bg-emerald-600/30 text-emerald-300'}`}>
                        {proj.name}
                      </span>
                    )}
                  </div>
                  <div className="flex items-center gap-1.5 mt-1 ml-4" onClick={(e) => e.stopPropagation()}>
                    {taskControlAllowed && !deliveryOwned && <PluginsBadge task={t} onRefresh={refresh} />}
                    <SubAgentsBadge task={t} />
                    <span className="flex-1" />
                    {taskControlAllowed && <button
                      onClick={async () => { await api.starTask(t.id); refresh(); }}
                      className={`p-1 transition-colors ${t.starred ? 'text-yellow-400 hover:text-yellow-300' : 'text-gray-600 hover:text-yellow-400'}`}
                      title={t.starred ? 'Unstar' : 'Star'}
                    >
                      <Star size={13} fill={t.starred ? 'currentColor' : 'none'} />
                    </button>}
                    {taskControlAllowed && !deliveryOwned && (
                      <>
                        {canManageTaskShare(t, currentUserIdentity) && (
                          <button
                            onClick={() => window.dispatchEvent(new CustomEvent('ccm-team-share-task', { detail: { task: t } }))}
                            className="p-1 text-gray-600 hover:text-blue-400 transition-colors"
                            title="Team Share"
                          >
                            <Share2 size={13} />
                          </button>
                        )}
                        <button
                          onClick={async () => { await api.archiveTask(t.id); skipFreezeOnce.current = true; refresh(); }}
                          className="p-1 text-gray-600 hover:text-amber-400 transition-colors"
                          title={t.archived ? 'Unarchive' : 'Archive'}
                        >
                          {t.archived ? <ArchiveRestore size={13} /> : <Archive size={13} />}
                        </button>
                      </>
                    )}
                  </div>
                </div>
              );})}
            </div>
            {totalPages > 1 && (
              <div className="flex items-center justify-center gap-2 py-1.5 border-t border-gray-800 shrink-0">
                <button
                  onClick={() => setPage((p) => Math.max(1, p - 1))}
                  disabled={page <= 1}
                  className="p-1 rounded text-gray-400 hover:text-foreground disabled:opacity-30"
                >
                  <ChevronLeft size={14} />
                </button>
                <span className="text-[10px] text-gray-500">{page}/{totalPages}</span>
                <button
                  onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
                  disabled={page >= totalPages}
                  className="p-1 rounded text-gray-400 hover:text-foreground disabled:opacity-30"
                >
                  <ChevronRight size={14} />
                </button>
              </div>
            )}
          </div>
        )}
        {sidebarOpen && (
          <div
            onMouseDown={handleDragStart}
            className="w-1 shrink-0 cursor-col-resize bg-transparent hover:bg-indigo-500/40 active:bg-indigo-500/60 transition-colors"
          />
        )}
        {!sidebarOpen && (
          <div className="shrink-0 border-r border-gray-800 bg-gray-900/50 flex flex-col items-center pt-2">
            <button
              onClick={() => setSidebarOpen(true)}
              className="p-1.5 text-gray-500 hover:text-gray-300 transition-colors"
              title="Expand sidebar"
            >
              <PanelLeftOpen size={16} />
            </button>
          </div>
        )}
        <div className="flex min-w-0 flex-1 flex-col">
          {showPRReviewResults && (
            <div className="max-h-[35vh] shrink-0 overflow-y-auto border-b border-gray-800 p-3">
              {prReviewResultsContent}
            </div>
          )}
          <div className="min-h-0 flex-1">
            {chatPanel}
          </div>
        </div>
        </div>
        {teamShareModal}
      </>
    );
  }

  return (
    <div className="space-y-4">
      {taskListContent}
      {chatPanel}

      {teamShareModal}
    </div>
  );
}
