import { render } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import { MarkdownRenderer } from './MarkdownRenderer';

interface CapturedNode {
  type?: string;
  value?: string;
  children?: CapturedNode[];
}

function nodesOfType(root: CapturedNode | null, type: string): CapturedNode[] {
  if (!root) return [];
  const matches = root.type === type ? [root] : [];
  for (const child of root.children || []) {
    matches.push(...nodesOfType(child, type));
  }
  return matches;
}

describe('MarkdownRenderer math support', () => {
  it('renders Codex backslash inline and whole-paragraph display math', () => {
    const markdown = String.raw`Inline \(q_1\) remains in prose.

\[
\nabla_z L_{\text{SFT}}=p_s-e_y
\]`;
    const { container } = render(<MarkdownRenderer content={markdown} />);

    expect(container.querySelectorAll('.katex')).toHaveLength(2);
    expect(container.querySelector('.katex-display')).not.toBeNull();
    expect(container.textContent).toContain('∇');
    expect(container.querySelector('.katex-html')).not.toBeNull();
  });

  it('keeps Markdown-like tokens inside whole-paragraph display math', () => {
    const markdown = String.raw`\[
\text{**not Markdown strong**}
\]`;
    const { container } = render(<MarkdownRenderer content={markdown} />);

    expect(container.querySelector('.katex-display')).not.toBeNull();
    expect(container.querySelector('strong')).toBeNull();
  });

  it('supports display dollars while leaving single-dollar prose literal', () => {
    const markdown = String.raw`The parameter $p$ stays literal.

$$
r^2
$$`;
    const { container } = render(<MarkdownRenderer content={markdown} />);

    expect(container.querySelectorAll('.katex')).toHaveLength(1);
    expect(container.querySelector('.katex-display')).not.toBeNull();
    expect(container.textContent).toContain('$p$ stays literal');
  });

  it('does not interpret ordinary currency as math', () => {
    const { container } = render(
      <MarkdownRenderer content={'Tickets cost $20 today and $30 tomorrow.'} />,
    );

    expect(container.querySelector('.katex')).toBeNull();
    expect(container.textContent).toContain('$20 today and $30 tomorrow');
  });

  it('preserves link, image, autolink, and reference destinations', () => {
    const markdown = String.raw`[docs](https://example.test/\(section\))

![plot](https://example.test/\(image\).png)

<https://example.test/\(autolink\)>

[reference][formula-ref]

[formula-ref]: https://example.test/\(reference\)`;
    const { container } = render(<MarkdownRenderer content={markdown} />);

    const docs = container.querySelector('a[href*="section"]');
    const image = container.querySelector('img');
    const autolink = container.querySelector('a[href*="autolink"]');
    const reference = container.querySelector('a[href*="reference"]');
    expect(docs?.getAttribute('href')).toBe('https://example.test/(section)');
    expect(image?.getAttribute('src')).toBe('https://example.test/(image).png');
    expect(autolink?.getAttribute('href')).not.toContain('$');
    expect(reference?.getAttribute('href')).toBe('https://example.test/(reference)');
    expect(container.querySelector('.katex')).toBeNull();
  });

  it('leaves inline HTML attributes untouched in the Markdown AST', () => {
    let capturedTree: CapturedNode | null = null;
    const captureTree = () => (tree: CapturedNode): void => {
      capturedTree = tree;
    };
    const markdown = String.raw`<span data-formula="\(not_math\)">safe</span>`;

    const { container } = render(
      <MarkdownRenderer content={markdown} remarkPlugins={[captureTree]} />,
    );

    expect(nodesOfType(capturedTree, 'html').map((node) => node.value)).toEqual([
      String.raw`<span data-formula="\(not_math\)">`,
      '</span>',
    ]);
    expect(container.querySelector('.katex')).toBeNull();
  });

  it('does not parse inline, fenced, or indented code as math', () => {
    const markdown = [
      'Inline code: `\\(not_inline_math\\)`.',
      '',
      '    \\[not_indented_math\\]',
      '',
      '```tex',
      '\\[',
      '\\nabla x',
      '\\]',
      '```',
    ].join('\n');
    const { container } = render(<MarkdownRenderer content={markdown} />);

    expect(container.querySelector('.katex')).toBeNull();
    expect(container.textContent).toContain(String.raw`\(not_inline_math\)`);
    expect(container.textContent).toContain(String.raw`\[not_indented_math\]`);
    expect(container.textContent).toContain(String.raw`\nabla x`);
  });

  it('does not pair delimiters across AST nodes or paragraphs', () => {
    const markdown = String.raw`Opening \(x

closing \) and opening display \[

x

closing display \]`;
    const { container } = render(<MarkdownRenderer content={markdown} />);

    expect(container.querySelector('.katex')).toBeNull();
    expect(container.textContent).toContain('Opening (x');
    expect(container.textContent).toContain('closing )');
  });

  it('does not let an old unmatched opener capture a later pair', () => {
    const markdown = String.raw`Old \( opening; later \(q\) is complete.`;
    const { container } = render(<MarkdownRenderer content={markdown} />);

    expect(container.querySelectorAll('.katex')).toHaveLength(1);
    expect(container.textContent).toContain('Old ( opening; later');
  });

  it('bounds KaTeX dimensions and rejects trusted links', () => {
    const markdown = String.raw`\[
\rule{1000000em}{1em}
\]

Inline \(\href{javascript:alert(1)}{click}\).`;
    const { container } = render(<MarkdownRenderer content={markdown} />);

    expect(container.querySelectorAll('.katex')).toHaveLength(2);
    expect(container.querySelector('.katex-html .rule')?.getAttribute('style')).toContain(
      'border-right-width: 20em',
    );
    expect(container.querySelector('.katex-html .rule')?.getAttribute('style')).not.toContain(
      '1000000em',
    );
    expect(container.querySelector('.katex a')).toBeNull();
  });
});
