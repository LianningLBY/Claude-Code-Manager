import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';

import { MarkdownContent } from './MarkdownContent';


describe('MarkdownContent', () => {
  it('constrains the document while keeping wide code locally scrollable', () => {
    const { container } = render(
      <MarkdownContent content={'Paragraph\n\n```text\n' + 'x'.repeat(500) + '\n```'} />,
    );

    const body = container.querySelector('.markdown-body');
    const pre = screen.getByText('x'.repeat(500)).closest('pre');
    expect(body).toHaveClass('min-w-0', 'max-w-full');
    expect(body).not.toHaveClass('overflow-x-auto');
    expect(pre).toHaveClass('max-w-full', 'overflow-x-auto');
  });
});
