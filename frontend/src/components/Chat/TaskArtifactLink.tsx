import { useState, type MouseEvent, type ReactNode } from 'react';

import { api } from '../../api/client';
import { Download, Loader2 } from '../icons';


const EXTERNAL_SCHEME_RE = /^[a-z][a-z0-9+.-]*:/i;


function isTaskArtifactHref(href?: string): boolean {
  const value = href?.trim();
  if (!value || value.startsWith('#') || value.startsWith('?') || value.startsWith('//')) {
    return false;
  }
  if (value.startsWith('/api/') || value.startsWith('/ws')) {
    return false;
  }
  const scheme = value.match(EXTERNAL_SCHEME_RE)?.[0].slice(0, -1).toLowerCase();
  return !scheme || scheme === 'file';
}


function startBrowserDownload(blob: Blob, filename: string): void {
  const objectUrl = URL.createObjectURL(blob);
  const anchor = document.createElement('a');
  anchor.href = objectUrl;
  anchor.download = filename || 'download';
  anchor.style.display = 'none';
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  window.setTimeout(() => URL.revokeObjectURL(objectUrl), 5000);
}


export function TaskArtifactLink({
  taskId,
  href,
  children,
}: {
  taskId: number;
  href?: string;
  children: ReactNode;
}) {
  const [downloading, setDownloading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  if (!isTaskArtifactHref(href)) {
    return (
      <a
        href={href}
        target="_blank"
        rel="noopener noreferrer"
        className="text-indigo-400 hover:text-indigo-300 underline"
      >
        {children}
      </a>
    );
  }

  const download = async (event: MouseEvent<HTMLAnchorElement>) => {
    event.preventDefault();
    if (!href || downloading) return;
    setDownloading(true);
    setError(null);
    try {
      const result = await api.downloadTaskArtifact(taskId, href);
      startBrowserDownload(result.blob, result.filename);
    } catch (downloadError) {
      setError(downloadError instanceof Error ? downloadError.message : '下载失败');
    } finally {
      setDownloading(false);
    }
  };

  return (
    <span className="inline">
      <a
        href={href}
        onClick={download}
        className="inline-flex items-center gap-1 text-indigo-400 hover:text-indigo-300 underline"
        title={downloading ? '正在下载…' : '下载任务文件'}
        aria-busy={downloading}
      >
        {children}
        {downloading
          ? <Loader2 size={12} className="shrink-0 animate-spin" />
          : <Download size={12} className="shrink-0" />}
      </a>
      {error && (
        <span role="alert" className="ml-1 text-xs text-red-400">
          （{error}）
        </span>
      )}
    </span>
  );
}
