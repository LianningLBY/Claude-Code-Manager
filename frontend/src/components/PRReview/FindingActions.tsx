import { useEffect, useState } from 'react';
import { api } from '../../api/client';
import type { PRFinding, PRFindingAction } from '../../api/client';

function actionKey(prefix: string, findingId: number): string {
  const nonce = globalThis.crypto?.randomUUID?.() || `${Date.now()}-${Math.random().toString(16).slice(2)}`;
  return `${prefix}-${findingId}-${nonce}`.slice(0, 64);
}

export function FindingActions({ finding, currentSnapshot, onChanged }: {
  finding: PRFinding;
  currentSnapshot: boolean;
  onChanged: () => Promise<void>;
}) {
  const [action, setAction] = useState<PRFindingAction | null>(finding.latest_action);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [reviewedDiff, setReviewedDiff] = useState<{
    actionId: number;
    patchSha256: string | null;
  } | null>(null);

  useEffect(() => setAction(finding.latest_action), [finding.latest_action]);

  useEffect(() => {
    if (!action || !['pending', 'running'].includes(action.status)) return;
    const timer = window.setInterval(async () => {
      try {
        const next = await api.getReviewFindingAction(action.id);
        setAction(next);
        if (!['pending', 'running'].includes(next.status)) await onChanged();
      } catch (reason) {
        setError(String(reason));
      }
    }, 2000);
    return () => window.clearInterval(timer);
  }, [action, onChanged]);

  const run = async (operation: () => Promise<PRFindingAction>) => {
    setBusy(true);
    setError(null);
    try {
      const next = await operation();
      setAction(next);
      await onChanged();
    } catch (reason) {
      setError(String(reason));
    } finally {
      setBusy(false);
    }
  };

  const downloadDiff = async () => {
    if (!action) return;
    const identity = { actionId: action.id, patchSha256: action.patch_sha256 };
    setBusy(true);
    setError(null);
    try {
      const file = await api.downloadReviewFindingDiff(action.id);
      const url = URL.createObjectURL(file.blob);
      const anchor = document.createElement('a');
      anchor.href = url;
      anchor.download = file.filename;
      anchor.click();
      URL.revokeObjectURL(url);
      setReviewedDiff(identity);
    } catch (reason) {
      setError(String(reason));
    } finally {
      setBusy(false);
    }
  };

  const diffIsCurrent = Boolean(
    action
    && reviewedDiff?.actionId === action.id
    && reviewedDiff.patchSha256 === action.patch_sha256,
  );
  const canStart = currentSnapshot && finding.status === 'open';
  const canConfirm = Boolean(
    currentSnapshot
    && action?.status === 'awaiting_confirmation'
    && action.confirmation_token
    && action.patch_sha256,
  );
  const canReconcile = Boolean(
    action?.status === 'running'
    && action.confirmation_token
    && action.patch_sha256,
  );

  return (
    <div className="mt-3 border-t border-gray-700/60 pt-3">
      {action && <p className="mb-2 text-gray-500">Action: {action.action_type} · {action.status}</p>}
      {error && <p role="alert" className="mb-2 text-red-400">{error}</p>}
      {!currentSnapshot && <p className="mb-2 text-gray-500">Historical snapshot — new actions are locked.</p>}
      <div className="flex flex-wrap gap-2">
        {canStart && (
          <>
            <button disabled={busy} className="rounded bg-gray-700 px-2 py-1 disabled:opacity-50" onClick={() => {
              if (window.confirm('Ignore this finding in CCM? The Panel gate remains authoritative.')) {
                void run(() => api.ignoreReviewFinding(finding.id, actionKey('ignore', finding.id)));
              }
            }}>Ignore</button>
            <button disabled={busy} className="rounded bg-gray-700 px-2 py-1 disabled:opacity-50" onClick={() => {
              const advice = window.prompt('Human repair advice');
              if (advice?.trim()) {
                void run(() => api.saveReviewFindingAdvice(finding.id, advice.trim(), actionKey('advice', finding.id)));
              }
            }}>Human advice</button>
            <button disabled={busy} className="rounded bg-indigo-600 px-2 py-1 text-white disabled:opacity-50" onClick={() => {
              void run(() => api.createReviewFindingFix(finding.id, actionKey('fix', finding.id)));
            }}>Generate AI fix</button>
          </>
        )}
        {(canConfirm || canReconcile) && (
          <>
            <button disabled={busy} className="rounded bg-gray-700 px-2 py-1 disabled:opacity-50" onClick={() => void downloadDiff()}>Download diff</button>
            <button disabled={busy || !diffIsCurrent} className="rounded bg-amber-600 px-2 py-1 text-white disabled:opacity-50" onClick={() => {
              if (!action?.confirmation_token || !action.patch_sha256) return;
              if (!window.confirm('Create a commit and non-force push this reviewed diff to the PR source branch?')) return;
              void run(() => api.confirmReviewFindingFix(action.id, action.confirmation_token!, action.patch_sha256!));
            }}>{canReconcile ? 'Reconcile push' : 'Confirm and push'}</button>
          </>
        )}
      </div>
    </div>
  );
}
