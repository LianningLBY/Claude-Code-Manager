import { afterEach, describe, expect, it, vi } from 'vitest';
import { act, render, screen } from '@testing-library/react';
import { useRef } from 'react';
import { useVisualViewportBounds } from './useVisualViewportBounds';

class MockVisualViewport extends EventTarget {
  height = 844;
  offsetTop = 0;
}

function Harness({ enabled = true }: { enabled?: boolean }) {
  const rootRef = useRef<HTMLDivElement>(null);
  useVisualViewportBounds(rootRef, enabled);
  return <div ref={rootRef} data-testid="viewport-root" />;
}

const originalVisualViewport = Object.getOwnPropertyDescriptor(
  window,
  'visualViewport',
);

function installVisualViewport(viewport: MockVisualViewport | undefined) {
  Object.defineProperty(window, 'visualViewport', {
    configurable: true,
    value: viewport,
  });
}

afterEach(() => {
  vi.restoreAllMocks();
  if (originalVisualViewport) {
    Object.defineProperty(window, 'visualViewport', originalVisualViewport);
  } else {
    Reflect.deleteProperty(window, 'visualViewport');
  }
});

describe('useVisualViewportBounds', () => {
  it('keeps the root inside the visual viewport as the keyboard opens and scrolls', () => {
    const viewport = new MockVisualViewport();
    installVisualViewport(viewport);

    render(<Harness />);
    const root = screen.getByTestId('viewport-root');

    expect(root).toHaveStyle({ height: '844px', top: '0px', bottom: 'auto' });

    act(() => {
      viewport.height = 486;
      viewport.dispatchEvent(new Event('resize'));
    });
    expect(root).toHaveStyle({ height: '486px', top: '0px', bottom: 'auto' });

    act(() => {
      viewport.offsetTop = 47;
      viewport.dispatchEvent(new Event('scroll'));
    });
    expect(root).toHaveStyle({ height: '486px', top: '47px', bottom: 'auto' });
  });

  it('resynchronizes after an orientation change and removes overrides on cleanup', () => {
    const viewport = new MockVisualViewport();
    installVisualViewport(viewport);
    const removeViewportListener = vi.spyOn(viewport, 'removeEventListener');
    const removeWindowListener = vi.spyOn(window, 'removeEventListener');

    const { unmount } = render(<Harness />);
    const root = screen.getByTestId('viewport-root');

    act(() => {
      viewport.height = 390;
      viewport.offsetTop = 12;
      window.dispatchEvent(new Event('orientationchange'));
    });
    expect(root).toHaveStyle({ height: '390px', top: '12px' });

    unmount();

    expect(root.style.height).toBe('');
    expect(root.style.top).toBe('');
    expect(root.style.bottom).toBe('');
    expect(removeViewportListener).toHaveBeenCalledWith('resize', expect.any(Function));
    expect(removeViewportListener).toHaveBeenCalledWith('scroll', expect.any(Function));
    expect(removeWindowListener).toHaveBeenCalledWith(
      'orientationchange',
      expect.any(Function),
    );
  });

  it('leaves layout untouched when disabled', () => {
    installVisualViewport(new MockVisualViewport());

    render(<Harness enabled={false} />);

    expect(screen.getByTestId('viewport-root').getAttribute('style')).toBeNull();
  });

  it('keeps the CSS fallback when visualViewport is unavailable', () => {
    installVisualViewport(undefined);

    render(<Harness />);

    expect(screen.getByTestId('viewport-root').getAttribute('style')).toBeNull();
  });
});
