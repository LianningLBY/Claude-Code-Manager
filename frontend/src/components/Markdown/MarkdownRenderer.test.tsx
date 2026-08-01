import { render } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import {
  MarkdownRenderer,
} from './MarkdownRenderer';
import { normalizeMathDelimiters } from './markdownMath';

describe('MarkdownRenderer math support', () => {
  it('renders the Codex display-math format preserved in CCM logs', () => {
    const { container } = render(
      <MarkdownRenderer content={'\\[\n\\nabla_z L_{\\text{SFT}}=p_s-e_y\n\\]'} />,
    );

    expect(container.querySelector('.katex-display')).not.toBeNull();
    expect(container.textContent).toContain('∇');
    expect(container.querySelector('.katex-html')).not.toBeNull();
  });

  it('renders inline LaTeX and native dollar delimiters', () => {
    const { container } = render(
      <MarkdownRenderer content={'Inline \\(q\\) and $p$, then:\n\n$$\nr^2\n$$'} />,
    );

    expect(container.querySelectorAll('.katex')).toHaveLength(3);
    expect(container.querySelector('.katex-display')).not.toBeNull();
  });

  it('does not normalize formula delimiters inside code', () => {
    const markdown = [
      'Inline `\\(not_math\\)`.',
      '',
      '```tex',
      '\\[',
      '\\nabla x',
      '\\]',
      '```',
    ].join('\n');

    expect(normalizeMathDelimiters(markdown)).toBe(markdown);
    const { container } = render(<MarkdownRenderer content={markdown} />);
    expect(container.querySelector('.katex')).toBeNull();
  });

  it('leaves an unmatched delimiter unchanged', () => {
    expect(normalizeMathDelimiters('unfinished \\[x')).toBe('unfinished \\[x');
  });
});
