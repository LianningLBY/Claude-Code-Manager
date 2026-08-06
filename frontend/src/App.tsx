import { useState, useEffect, Component } from 'react';
import type { ReactNode, ErrorInfo } from 'react';
import { AppShell } from './components/Layout/AppShell';
import { Dashboard } from './pages/Dashboard';
import { TasksPage } from './pages/TasksPage';
import { PlansPage } from './pages/PlansPage';
import { LoginPage } from './pages/LoginPage';
import { ServerConfigPage } from './pages/ServerConfigPage';
import { ProjectsPage } from './pages/ProjectsPage';
import { SecretsPage } from './pages/SecretsPage';
import { FilesPage } from './pages/FilesPage';
import { DiscussionsPage } from './pages/DiscussionsPage';
import { PRMonitorPage } from './pages/PRMonitorPage';
import WorkersPage from './pages/WorkersPage';
import TeamPage from './pages/TeamPage';
import { SkillsPage } from './pages/SkillsPage';
import { SettingsPage } from './pages/SettingsPage';

import { getToken } from './api/client';
import { isCapacitor, getServerUrl, getApiBase } from './config/server';

class ErrorBoundary extends Component<{ children: ReactNode }, { error: string | null }> {
  state = { error: null as string | null };
  static getDerivedStateFromError(error: Error) {
    return { error: error.message };
  }
  componentDidCatch(error: Error, info: ErrorInfo) {
    console.error('React error:', error, info);
  }
  render() {
    if (this.state.error) {
      return (
        <div style={{ padding: 40, color: '#f87171', background: '#1a1a2e', minHeight: '100vh' }}>
          <h1 style={{ fontSize: 24, marginBottom: 16 }}>Something went wrong</h1>
          <pre style={{ whiteSpace: 'pre-wrap' }}>{this.state.error}</pre>
          <button
            onClick={() => { this.setState({ error: null }); window.location.reload(); }}
            style={{ marginTop: 16, padding: '8px 16px', background: '#4f46e5', color: 'white', border: 'none', borderRadius: 4, cursor: 'pointer' }}
          >
            Reload
          </button>
        </div>
      );
    }
    return this.props.children;
  }
}

const VALID_PAGES = new Set(['tasks', 'plans', 'dashboard', 'projects', 'secrets', 'files', 'discussions', 'pr-monitor', 'workers', 'skills', 'team', 'settings', 'server']);

function parseHash(): { page: string; chatTaskId: number | null; planId: number | null } {
  const hash = window.location.hash.replace(/^#\/?/, '');
  const parts = hash.split('/');
  const page = VALID_PAGES.has(parts[0]) ? parts[0] : 'tasks';
  let chatTaskId: number | null = null;
  let planId: number | null = null;
  if (page === 'tasks' && parts[1] === 'chat' && parts[2]) {
    const id = parseInt(parts[2], 10);
    if (id > 0) chatTaskId = id;
  }
  if (page === 'plans' && parts[1]) {
    const id = parseInt(parts[1], 10);
    if (id > 0) planId = id;
  }
  return { page, chatTaskId, planId };
}

function updateHash(page: string, chatTaskId: number | null, planId: number | null) {
  let hash = `#/${page}`;
  if (page === 'tasks' && chatTaskId) hash += `/chat/${chatTaskId}`;
  if (page === 'plans' && planId) hash += `/${planId}`;
  if (window.location.hash !== hash) {
    window.history.replaceState(null, '', hash);
  }
}

function App() {
  const initial = parseHash();
  const [page, setPage] = useState(initial.page);
  const [chatTaskId, setChatTaskId] = useState<number | null>(initial.chatTaskId);
  const [planId, setPlanId] = useState<number | null>(initial.planId);
  const [authenticated, setAuthenticated] = useState(false);
  const [checking, setChecking] = useState(true);
  const [needsServerConfig, setNeedsServerConfig] = useState(false);

  useEffect(() => {
    updateHash(page, chatTaskId, planId);
  }, [page, chatTaskId, planId]);

  useEffect(() => {
    const onHashChange = () => {
      const parsed = parseHash();
      setPage(parsed.page);
      setChatTaskId(parsed.chatTaskId);
      setPlanId(parsed.planId);
    };
    window.addEventListener('hashchange', onHashChange);
    return () => window.removeEventListener('hashchange', onHashChange);
  }, []);

  const handleNavigate = (p: string) => {
    const nextHash = `#/${p}`;
    if (window.location.hash !== nextHash) {
      // Page navigation must create a history entry so the browser Back button
      // returns to the exact page URL that preceded it. The state-sync effect
      // continues to use replaceState for modal/chat URL updates.
      window.history.pushState(null, '', nextHash);
    }
    setPage(p);
    if (p !== 'tasks') setChatTaskId(null);
    if (p !== 'plans') setPlanId(null);
  };

  const handleNavigateTask = (taskId: number) => {
    setPlanId(null);
    setChatTaskId(taskId);
    setPage('tasks');
  };

  useEffect(() => {
    // In Capacitor, require server URL to be configured first
    if (isCapacitor() && !getServerUrl()) {
      setNeedsServerConfig(true);
      setChecking(false);
      return;
    }

    const base = getApiBase();
    // Health is public. Use the identity endpoint as the protected probe so
    // ordinary members do not need access to the admin-only Instance module.
    fetch(`${base}/api/system/health`)
      .then((res) => {
        if (res.ok) {
          const token = getToken();
          return fetch(`${base}/api/auth/me`, {
            headers: token ? { Authorization: `Bearer ${token}` } : {},
          });
        }
        throw new Error('Server unreachable');
      })
      .then(async (res) => {
        if (res.ok) {
          // Reuse the probe response to refresh the cached identity. Token and
          // no-auth deployments may legitimately have no user object.
          const data = await res.json();
          if (data?.user) {
            localStorage.setItem('cc_user', JSON.stringify(data.user));
          } else if (data?.role) {
            localStorage.setItem('cc_user', JSON.stringify({
              name: data.auth_type === 'none' ? 'Local Admin' : 'Admin',
              role: data.role,
            }));
          }
          // AppShell reads the cached identity during render, so authenticate
          // only after the authoritative response has replaced stale state.
          setAuthenticated(true);
        }
      })
      .catch(() => {
        // Server down, show login anyway
      })
      .finally(() => setChecking(false));
  }, []);

  if (needsServerConfig) {
    return (
      <ServerConfigPage onConfigured={() => {
        setNeedsServerConfig(false);
        window.location.reload();
      }} />
    );
  }

  if (checking) {
    return (
      <div className="min-h-screen bg-gray-950 flex flex-col items-center justify-center gap-3">
        <div className="h-8 w-8 rounded-full border-2 border-gray-700 border-t-indigo-500 animate-spin" />
        <p className="text-gray-500 text-sm">Connecting...</p>
      </div>
    );
  }

  if (!authenticated) {
    return <LoginPage onLogin={() => setAuthenticated(true)} />;
  }

  return (
    <ErrorBoundary>
      <AppShell currentPage={page} onNavigate={handleNavigate} wide={page === 'tasks' && !!chatTaskId}>
        {page === 'dashboard' && <Dashboard />}
        {page === 'tasks' && <TasksPage chatTaskId={chatTaskId} onChatTaskChange={setChatTaskId} />}
        {page === 'plans' && <PlansPage selectedPlanId={planId} onSelectedPlanChange={setPlanId} onNavigateTask={handleNavigateTask} onNavigateSettings={() => handleNavigate('settings')} />}
        {page === 'projects' && <ProjectsPage />}
        {page === 'secrets' && <SecretsPage />}
        {page === 'files' && <FilesPage />}
        {page === 'discussions' && <DiscussionsPage />}
        {page === 'pr-monitor' && <PRMonitorPage />}
        {page === 'workers' && <WorkersPage />}
        {page === 'skills' && <SkillsPage />}
        {page === 'team' && <TeamPage />}
        {page === 'settings' && <SettingsPage />}
        {page === 'server' && (
          <ServerConfigPage onConfigured={() => window.location.reload()} />
        )}
      </AppShell>
    </ErrorBoundary>
  );
}

export default App;
