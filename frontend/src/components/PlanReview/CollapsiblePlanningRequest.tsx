import { useId, useLayoutEffect, useRef, useState } from 'react';
import type { ReactNode } from 'react';

interface CollapsiblePlanningRequestProps {
  content: string;
  children?: ReactNode;
}

const COLLAPSED_HEIGHT = 200;

export function CollapsiblePlanningRequest({
  content,
  children,
}: CollapsiblePlanningRequestProps) {
  const contentId = useId();
  const contentRef = useRef<HTMLDivElement>(null);
  const [expandedContent, setExpandedContent] = useState<string | null>(null);
  const [canExpand, setCanExpand] = useState(false);
  const expanded = expandedContent === content;

  useLayoutEffect(() => {
    const element = contentRef.current;
    if (!element) return;
    const measure = () => {
      setCanExpand(element.scrollHeight > COLLAPSED_HEIGHT + 1);
    };
    measure();

    if (typeof ResizeObserver === 'undefined') return;
    const observer = new ResizeObserver(measure);
    observer.observe(element);
    return () => observer.disconnect();
  }, [content, children]);

  return (
    <div className="mb-3 rounded-lg border border-gray-800 bg-gray-950/40 px-3 py-2">
      <div
        id={contentId}
        ref={contentRef}
        className={expanded ? undefined : 'max-h-[200px] overflow-hidden'}
      >
        <div className="text-[10px] font-medium uppercase tracking-wide text-gray-600">
          Planning request
        </div>
        <p className="mt-1 whitespace-pre-wrap text-xs leading-5 text-gray-400">
          {content}
        </p>
        {children}
      </div>
      {canExpand && (
        <button
          type="button"
          aria-controls={contentId}
          aria-expanded={expanded}
          onClick={() => setExpandedContent((current) => (
            current === content ? null : content
          ))}
          className="mt-2 text-xs font-medium text-indigo-300 hover:text-indigo-200"
        >
          {expanded ? 'Show less' : 'Show more'}
        </button>
      )}
    </div>
  );
}
