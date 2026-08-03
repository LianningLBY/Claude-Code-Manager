import type { Components } from 'react-markdown';
import ReactMarkdown from 'react-markdown';
import type { PluggableList } from 'unified';
import rehypeKatex from 'rehype-katex';
import remarkGfm from 'remark-gfm';
import remarkMath from 'remark-math';
import { remarkBackslashMath } from './markdownMath';

interface MarkdownRendererProps {
  content: string;
  components?: Components;
  remarkPlugins?: PluggableList;
}

export function MarkdownRenderer({ content, components, remarkPlugins = [] }: MarkdownRendererProps) {
  return (
    <ReactMarkdown
      remarkPlugins={[
        remarkGfm,
        [remarkMath, { singleDollarTextMath: false }],
        remarkBackslashMath,
        ...remarkPlugins,
      ]}
      rehypePlugins={[[
        rehypeKatex,
        {
          maxSize: 20,
          strict: false,
          throwOnError: false,
          trust: false,
        },
      ]]}
      components={components}
    >
      {content}
    </ReactMarkdown>
  );
}
