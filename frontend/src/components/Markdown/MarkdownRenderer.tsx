import type { Components } from 'react-markdown';
import ReactMarkdown from 'react-markdown';
import type { PluggableList } from 'unified';
import rehypeKatex from 'rehype-katex';
import remarkGfm from 'remark-gfm';
import remarkMath from 'remark-math';
import { normalizeMathDelimiters } from './markdownMath';

interface MarkdownRendererProps {
  content: string;
  components?: Components;
  remarkPlugins?: PluggableList;
}

export function MarkdownRenderer({ content, components, remarkPlugins = [] }: MarkdownRendererProps) {
  return (
    <ReactMarkdown
      remarkPlugins={[remarkGfm, remarkMath, ...remarkPlugins]}
      rehypePlugins={[[rehypeKatex, { strict: false, throwOnError: false }]]}
      components={components}
    >
      {normalizeMathDelimiters(content)}
    </ReactMarkdown>
  );
}
