import { useEffect } from 'react';
import type { RefObject } from 'react';

/**
 * Keep a fixed overlay inside the part of the page that is actually visible.
 *
 * iOS in-app browsers keep the layout viewport at its pre-keyboard height while
 * shrinking `visualViewport`, so a bottom composer can otherwise sit behind the
 * software keyboard. Direct style writes avoid rerendering the full chat during
 * WebKit's keyboard animation.
 */
export function useVisualViewportBounds(
  rootRef: RefObject<HTMLElement | null>,
  enabled: boolean,
) {
  useEffect(() => {
    const root = rootRef.current;
    const viewport = window.visualViewport;
    if (!enabled || !root || !viewport) return;

    const syncBounds = () => {
      const height = Number.isFinite(viewport.height)
        ? Math.max(0, viewport.height)
        : 0;
      const offsetTop = Number.isFinite(viewport.offsetTop)
        ? Math.max(0, viewport.offsetTop)
        : 0;

      if (height > 0) root.style.height = `${height}px`;
      root.style.top = `${offsetTop}px`;
      root.style.bottom = 'auto';
    };

    syncBounds();
    viewport.addEventListener('resize', syncBounds);
    viewport.addEventListener('scroll', syncBounds);
    window.addEventListener('orientationchange', syncBounds);

    return () => {
      viewport.removeEventListener('resize', syncBounds);
      viewport.removeEventListener('scroll', syncBounds);
      window.removeEventListener('orientationchange', syncBounds);
      root.style.height = '';
      root.style.top = '';
      root.style.bottom = '';
    };
  }, [enabled, rootRef]);
}
