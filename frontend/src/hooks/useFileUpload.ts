import { useState, useCallback, useMemo, useRef } from 'react';
import { api, type UploadResult } from '../api/client';

export interface UploadEntry {
  id: string;
  file?: File;
  preview: string;
  status: 'uploading' | 'uploaded' | 'failed';
  result?: UploadResult;
  error?: string;
}

const MAX_FILE_SIZE = 50 * 1024 * 1024;
export const MAX_FILES = 10;
const BLOCKED_EXTENSIONS = new Set(['.exe']);
const IMAGE_EXTS = ['.png', '.jpg', '.jpeg', '.gif', '.webp'];
const isImageFile = (name: string) => IMAGE_EXTS.some(ext => name.toLowerCase().endsWith(ext));

export function isUploadResult(value: unknown): value is UploadResult {
  if (!value || typeof value !== 'object') return false;
  const candidate = value as Partial<UploadResult>;
  return (
    typeof candidate.id === 'string'
    && candidate.id.length > 0
    && (candidate.filename === null || typeof candidate.filename === 'string')
    && typeof candidate.path === 'string'
    && candidate.path.length > 0
    && typeof candidate.url === 'string'
    && candidate.url.length > 0
    && typeof candidate.is_image === 'boolean'
  );
}

export function sameUploadResult(left: UploadResult, right: UploadResult): boolean {
  return left.id === right.id || left.path === right.path;
}

export function dedupeUploadResults(results: UploadResult[]): UploadResult[] {
  const unique: UploadResult[] = [];
  for (const result of results) {
    if (!unique.some((existing) => sameUploadResult(existing, result))) {
      unique.push(result);
    }
  }
  return unique;
}

function restoredUploadEntry(result: UploadResult): UploadEntry {
  return {
    id: `uploaded:${result.id}:${result.path}`,
    preview: '',
    status: 'uploaded',
    result,
  };
}

export function useFileUpload(initialUploadedResults: UploadResult[] = []) {
  const [uploads, setUploads] = useState<UploadEntry[]>(() =>
    dedupeUploadResults(initialUploadedResults)
      .slice(0, MAX_FILES)
      .map(restoredUploadEntry)
  );
  const uploadsRef = useRef<UploadEntry[]>([]);
  uploadsRef.current = uploads;

  const doUpload = useCallback(async (entry: UploadEntry) => {
    const file = entry.file;
    if (!file) return;
    try {
      const results = await api.uploadImages([file]);
      setUploads(prev => prev.map(u =>
        u.id === entry.id ? { ...u, status: 'uploaded' as const, result: results[0] } : u
      ));
    } catch (e) {
      setUploads(prev => prev.map(u =>
        u.id === entry.id ? { ...u, status: 'failed' as const, error: e instanceof Error ? e.message : String(e) } : u
      ));
    }
  }, []);

  const addFiles = useCallback((incoming: File[], onError?: (msg: string) => void) => {
    const blocked = incoming.filter(f => {
      const ext = f.name.toLowerCase().slice(f.name.lastIndexOf('.'));
      return BLOCKED_EXTENSIONS.has(ext);
    });
    if (blocked.length > 0) {
      onError?.(`File type not allowed: ${blocked.map(f => f.name).join(', ')}`);
    }
    const allowed = incoming.filter(f => {
      const ext = f.name.toLowerCase().slice(f.name.lastIndexOf('.'));
      return !BLOCKED_EXTENSIONS.has(ext);
    });
    const oversized = allowed.filter(f => f.size > MAX_FILE_SIZE);
    if (oversized.length > 0) {
      onError?.(oversized.length === 1
        ? `File "${oversized[0].name}" exceeds 50MB limit`
        : `${oversized.length} files exceed 50MB limit`);
    }
    const valid = allowed.filter(f => f.size <= MAX_FILE_SIZE);
    if (valid.length === 0) return;

    const slots = MAX_FILES - uploadsRef.current.length;
    if (slots <= 0) {
      onError?.(`Maximum ${MAX_FILES} files allowed`);
      return;
    }
    const accepted = valid.slice(0, slots);

    const newEntries: UploadEntry[] = accepted.map(f => ({
      id: `${Date.now()}-${Math.random().toString(36).slice(2)}`,
      file: f,
      preview: isImageFile(f.name) ? URL.createObjectURL(f) : '',
      status: 'uploading' as const,
    }));

    setUploads(prev => [...prev, ...newEntries]);
    newEntries.forEach(entry => doUpload(entry));
  }, [doUpload]);

  const addUploadedResults = useCallback((incoming: UploadResult[]): boolean => {
    const current = uploadsRef.current;
    const existingResults = current
      .map((entry) => entry.result)
      .filter((result): result is UploadResult => !!result);
    const additions = dedupeUploadResults(incoming).filter(
      (result) => !existingResults.some((existing) => sameUploadResult(existing, result)),
    );
    if (current.length + additions.length > MAX_FILES) return false;
    if (additions.length === 0) return true;
    const next = [
      ...current,
      ...additions.map(restoredUploadEntry),
    ];
    uploadsRef.current = next;
    setUploads(next);
    return true;
  }, []);

  const removeFile = useCallback((id: string) => {
    setUploads(prev => {
      const removed = prev.find(u => u.id === id);
      if (removed?.preview) URL.revokeObjectURL(removed.preview);
      return prev.filter(u => u.id !== id);
    });
  }, []);

  const retryFile = useCallback((id: string) => {
    const entry = uploadsRef.current.find(u => u.id === id);
    if (!entry || entry.status !== 'failed') return;
    const updated = { ...entry, status: 'uploading' as const, error: undefined };
    setUploads(prev => prev.map(u => u.id === id ? updated : u));
    doUpload(updated);
  }, [doUpload]);

  const clear = useCallback(() => {
    const current = uploadsRef.current;
    current.forEach(u => { if (u.preview) URL.revokeObjectURL(u.preview); });
    uploadsRef.current = [];
    setUploads([]);
  }, []);

  const uploadedResults = useMemo(
    () => uploads
      .filter((u): u is UploadEntry & { result: UploadResult } => u.status === 'uploaded' && !!u.result)
      .map(u => u.result),
    [uploads],
  );

  return {
    uploads,
    addFiles,
    addUploadedResults,
    removeFile,
    retryFile,
    clear,
    uploadedResults,
    isUploading: uploads.some(u => u.status === 'uploading'),
    hasFailed: uploads.some(u => u.status === 'failed'),
    allDone: uploads.length > 0 && uploads.every(u => u.status !== 'uploading'),
  };
}
