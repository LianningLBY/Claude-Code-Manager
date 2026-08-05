import { useEffect, useState } from 'react';

import { api } from '../../api/client';
import type { Task } from '../../api/client';
import { Check, Loader2, Pin, X } from '../icons';

interface AttentionTagProps {
  taskId: number;
  value?: string | null;
  editing: boolean;
  onEdit: () => void;
  onCancel: () => void;
  onSaved?: (task: Task) => void;
  showAddButton?: boolean;
  className?: string;
}

export function AttentionTag({
  taskId,
  value,
  editing,
  onEdit,
  onCancel,
  onSaved,
  showAddButton = false,
  className = '',
}: AttentionTagProps) {
  const [displayValue, setDisplayValue] = useState<string | null>(value || null);
  const [draft, setDraft] = useState(value || '');
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState('');

  useEffect(() => {
    const nextValue = value || null;
    setDisplayValue(nextValue);
    setDraft(nextValue || '');
    setError('');
  }, [taskId, value]);

  useEffect(() => {
    if (editing) return;
    setDraft(displayValue || '');
    setError('');
  }, [displayValue, editing]);

  const cancel = () => {
    setDraft(displayValue || '');
    setError('');
    onCancel();
  };

  const save = async () => {
    if (saving) return;
    const normalized = draft.trim() || null;
    if (normalized === displayValue) {
      cancel();
      return;
    }

    setSaving(true);
    setError('');
    try {
      const updated = await api.updateTask(taskId, {
        attention_tag: normalized,
      });
      setDisplayValue(updated.attention_tag || null);
      setDraft(updated.attention_tag || '');
      onCancel();
      onSaved?.(updated);
    } catch (caught) {
      setError(
        caught instanceof Error && caught.message
          ? caught.message
          : 'Failed to save attention tag',
      );
    } finally {
      setSaving(false);
    }
  };

  if (!editing) {
    if (displayValue) {
      return (
        <button
          type="button"
          onClick={onEdit}
          title="Edit attention tag"
          className={`inline-flex min-w-0 max-w-full items-center gap-1 rounded-md border border-amber-400/25 bg-amber-500/15 px-1.5 py-0.5 text-xs font-medium text-amber-300 transition-colors hover:border-amber-400/45 hover:bg-amber-500/25 ${className}`}
        >
          <Pin size={11} className="shrink-0" />
          <span className="truncate">{displayValue}</span>
        </button>
      );
    }
    if (!showAddButton) return null;
    return (
      <button
        type="button"
        onClick={onEdit}
        title="Add attention tag"
        className={`inline-flex items-center gap-1 rounded-md border border-dashed border-amber-400/25 px-1.5 py-0.5 text-xs text-amber-400/70 transition-colors hover:border-amber-400/50 hover:bg-amber-500/10 hover:text-amber-300 ${className}`}
      >
        <Pin size={11} />
        <span className="hidden sm:inline">Tag</span>
      </button>
    );
  }

  return (
    <div className={`min-w-0 ${className}`}>
      <div className="flex min-w-0 items-center gap-1 rounded-lg border border-amber-400/30 bg-amber-500/10 p-1">
        <Pin size={13} className="ml-1 shrink-0 text-amber-400" />
        <input
          autoFocus
          aria-label="Attention tag"
          value={draft}
          maxLength={80}
          disabled={saving}
          onChange={(event) => setDraft(event.target.value)}
          onKeyDown={(event) => {
            if (event.key === 'Enter') {
              event.preventDefault();
              void save();
            }
            if (event.key === 'Escape') cancel();
          }}
          placeholder="例如：等任务结束后再看"
          className="min-w-0 flex-1 bg-transparent px-1 py-0.5 text-xs text-foreground outline-none placeholder:text-gray-600"
        />
        <button
          type="button"
          onClick={() => void save()}
          disabled={saving}
          title="Save attention tag"
          className="rounded p-1 text-amber-300 transition-colors hover:bg-amber-400/15 disabled:opacity-50"
        >
          {saving ? <Loader2 size={13} className="animate-spin" /> : <Check size={13} />}
        </button>
        <button
          type="button"
          onClick={cancel}
          disabled={saving}
          title="Cancel attention tag editing"
          className="rounded p-1 text-gray-500 transition-colors hover:bg-gray-700 hover:text-gray-300 disabled:opacity-50"
        >
          <X size={13} />
        </button>
      </div>
      {error && (
        <p role="alert" className="mt-1 text-[11px] text-red-400">
          {error}
        </p>
      )}
    </div>
  );
}
