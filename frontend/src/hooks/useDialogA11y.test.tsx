import { act, render, screen, waitFor } from '@testing-library/react';
import { useDialogA11y } from './useDialogA11y';

function Dialog({ onClose }: { onClose: () => void }) {
  const dialogRef = useDialogA11y(true, onClose);
  return (
    <div ref={dialogRef} role="dialog">
      <button type="button">Close</button>
      <textarea aria-label="Plan request" />
    </div>
  );
}

describe('useDialogA11y', () => {
  it('preserves focus when the parent supplies a new onClose callback', async () => {
    const firstClose = vi.fn();
    const secondClose = vi.fn();
    const { rerender } = render(<Dialog onClose={firstClose} />);

    await waitFor(() => expect(screen.getByRole('button', { name: 'Close' })).toHaveFocus());
    const textarea = screen.getByRole('textbox', { name: 'Plan request' });
    textarea.focus();

    rerender(<Dialog onClose={secondClose} />);
    await act(() => new Promise((resolve) => window.setTimeout(resolve, 0)));

    expect(textarea).toHaveFocus();
    act(() => document.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape' })));
    expect(firstClose).not.toHaveBeenCalled();
    expect(secondClose).toHaveBeenCalledOnce();
  });
});
