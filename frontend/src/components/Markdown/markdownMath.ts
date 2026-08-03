import { decodeString } from 'micromark-util-decode-string';

interface MarkdownPosition {
  start?: { offset?: number };
  end?: { offset?: number };
}

interface MarkdownNode {
  type: string;
  value?: string;
  children?: MarkdownNode[];
  position?: MarkdownPosition;
  data?: Record<string, unknown>;
}

interface MarkdownFile {
  value?: unknown;
}

const SKIP_DESCENDANTS = new Set([
  'code',
  'definition',
  'html',
  'image',
  'imageReference',
  'inlineCode',
  'link',
  'linkReference',
  'math',
  'inlineMath',
]);

function isMarkdownNode(value: unknown): value is MarkdownNode {
  return Boolean(
    value
    && typeof value === 'object'
    && typeof (value as { type?: unknown }).type === 'string',
  );
}

function sourceForNode(node: MarkdownNode, source: string): string | null {
  const start = node.position?.start?.offset;
  const end = node.position?.end?.offset;
  if (
    typeof start !== 'number'
    || typeof end !== 'number'
    || start < 0
    || end < start
    || end > source.length
  ) {
    return null;
  }
  return source.slice(start, end);
}

function isEscaped(source: string, index: number): boolean {
  let precedingBackslashes = 0;
  for (let cursor = index - 1; cursor >= 0 && source[cursor] === '\\'; cursor -= 1) {
    precedingBackslashes += 1;
  }
  return precedingBackslashes % 2 === 1;
}

function findDelimiter(
  source: string,
  closingCharacter: '(' | ')' | '[' | ']',
  fromIndex: number,
): number {
  for (let index = fromIndex; index < source.length - 1; index += 1) {
    if (
      source[index] === '\\'
      && source[index + 1] === closingCharacter
      && !isEscaped(source, index)
    ) {
      return index;
    }
  }
  return -1;
}

function inlineMathNode(value: string): MarkdownNode {
  return {
    type: 'inlineMath',
    value,
    data: {
      hName: 'code',
      hProperties: { className: ['language-math', 'math-inline'] },
      hChildren: [{ type: 'text', value }],
    },
  };
}

function displayMathNode(value: string): MarkdownNode {
  return {
    type: 'math',
    value,
    data: {
      hName: 'pre',
      hChildren: [{
        type: 'element',
        tagName: 'code',
        properties: { className: ['language-math', 'math-display'] },
        children: [{ type: 'text', value }],
      }],
    },
  };
}

function stripOneLineEnding(value: string, fromStart: boolean): string {
  if (fromStart) return value.replace(/^(?:\r\n|\r|\n)/, '');
  return value.replace(/(?:\r\n|\r|\n)$/, '');
}

function parseDisplayMath(paragraph: MarkdownNode, source: string): MarkdownNode | null {
  const raw = sourceForNode(paragraph, source);
  if (raw === null) return null;

  const leadingWhitespace = raw.match(/^[ \t]*/)?.[0].length || 0;
  const opening = findDelimiter(raw, '[', leadingWhitespace);
  if (opening !== leadingWhitespace) return null;

  const closing = findDelimiter(raw, ']', opening + 2);
  if (closing < 0 || !/^[ \t]*$/.test(raw.slice(closing + 2))) return null;

  let value = raw.slice(opening + 2, closing);
  value = stripOneLineEnding(value, true);
  value = stripOneLineEnding(value, false);
  return displayMathNode(value);
}

function splitInlineMath(node: MarkdownNode, source: string): MarkdownNode[] | null {
  const raw = sourceForNode(node, source);
  if (raw === null) return null;

  const transformed: MarkdownNode[] = [];
  let cursor = 0;
  let searchFrom = 0;
  let found = false;

  while (searchFrom < raw.length - 1) {
    const opening = findDelimiter(raw, '(', searchFrom);
    if (opening < 0) break;

    const closing = findDelimiter(raw, ')', opening + 2);
    const nextOpening = findDelimiter(raw, '(', opening + 2);
    const lineEnding = raw.slice(opening + 2).search(/[\r\n]/);
    if (
      closing < 0
      || (lineEnding >= 0 && opening + 2 + lineEnding < closing)
    ) {
      searchFrom = opening + 2;
      continue;
    }
    if (nextOpening >= 0 && nextOpening < closing) {
      searchFrom = nextOpening;
      continue;
    }

    if (opening > cursor) {
      transformed.push({ type: 'text', value: decodeString(raw.slice(cursor, opening)) });
    }
    transformed.push(inlineMathNode(raw.slice(opening + 2, closing)));
    cursor = closing + 2;
    searchFrom = cursor;
    found = true;
  }

  if (!found) return null;
  if (cursor < raw.length) {
    transformed.push({ type: 'text', value: decodeString(raw.slice(cursor)) });
  }
  return transformed;
}

function transformChildren(parent: MarkdownNode, source: string): void {
  if (!parent.children || SKIP_DESCENDANTS.has(parent.type)) return;

  const transformed: MarkdownNode[] = [];
  for (const child of parent.children) {
    if (child.type === 'paragraph') {
      const displayMath = parseDisplayMath(child, source);
      if (displayMath) {
        transformed.push(displayMath);
        continue;
      }
    }

    if (child.type === 'text') {
      transformed.push(...(splitInlineMath(child, source) || [child]));
      continue;
    }

    transformChildren(child, source);
    transformed.push(child);
  }
  parent.children = transformed;
}

/**
 * Parse Codex's `\\(...\\)` and `\\[...\\]` notation after Markdown has
 * formed its AST. Source positions recover the escaped delimiters without
 * rewriting URLs, HTML, code, definitions, images, or link destinations.
 * Inline pairs are deliberately confined to one text node, and display math
 * must occupy a whole paragraph, so delimiters cannot capture unrelated AST.
 */
export function remarkBackslashMath() {
  return (tree: unknown, file: MarkdownFile): void => {
    if (!isMarkdownNode(tree) || typeof file.value !== 'string') return;
    transformChildren(tree, file.value);
  };
}
