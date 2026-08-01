interface MarkdownNode {
  type: string;
  value?: string;
  url?: string;
  children?: MarkdownNode[];
}


const SKIP_DESCENDANTS = new Set([
  'code',
  'inlineCode',
  'link',
  'linkReference',
  'html',
]);

// Only infer absolute POSIX paths that end in a conventional file extension.
// Requiring an extension keeps ordinary slash-separated prose and directories
// as text. Spaces inside the path are supported; trailing prose is excluded by
// stopping at the first extension followed by whitespace or punctuation.
const ABSOLUTE_FILE_PATH_RE =
  /(^|[\s:：([（])((?:\/(?!\/)[^\r\n<>{}\]`]*?)\.[A-Za-z0-9](?:[A-Za-z0-9+_-]{0,14}[A-Za-z0-9])?)(?=$|[\s。，、；;！!？?,)\]）}])/gu;


function isMarkdownNode(value: unknown): value is MarkdownNode {
  return Boolean(
    value
    && typeof value === 'object'
    && typeof (value as { type?: unknown }).type === 'string',
  );
}


function isTaskFilePath(path: string): boolean {
  if (
    !path.startsWith('/')
    || path.startsWith('//')
    || path.startsWith('/api/')
    || path.startsWith('/ws')
    || path.includes('\0')
  ) {
    return false;
  }
  const components = path.slice(1).split('/');
  if (
    components.length < 2
    || components.some((component) => (
      !component
      || component !== component.trim()
      || component.includes('\t')
    ))
  ) {
    return false;
  }
  return /\.[A-Za-z0-9](?:[A-Za-z0-9+_-]{0,14}[A-Za-z0-9])?$/.test(
    components[components.length - 1],
  );
}


function splitBareArtifactPaths(value: string): MarkdownNode[] | null {
  const nodes: MarkdownNode[] = [];
  let cursor = 0;
  let found = false;
  ABSOLUTE_FILE_PATH_RE.lastIndex = 0;

  for (const match of value.matchAll(ABSOLUTE_FILE_PATH_RE)) {
    const boundary = match[1] || '';
    const path = match[2];
    if (!path || !isTaskFilePath(path) || match.index === undefined) continue;

    const pathStart = match.index + boundary.length;
    if (pathStart > cursor) {
      nodes.push({ type: 'text', value: value.slice(cursor, pathStart) });
    }
    nodes.push({
      type: 'link',
      url: path,
      // Preserve the assistant's visible text exactly; this fallback only
      // adds link semantics and the download affordance.
      children: [{ type: 'text', value: path }],
    });
    cursor = pathStart + path.length;
    found = true;
  }

  if (!found) return null;
  if (cursor < value.length) {
    nodes.push({ type: 'text', value: value.slice(cursor) });
  }
  return nodes;
}


function transformTextNodes(parent: MarkdownNode): void {
  if (!parent.children || SKIP_DESCENDANTS.has(parent.type)) return;

  const transformed: MarkdownNode[] = [];
  for (const child of parent.children) {
    if (child.type === 'text' && typeof child.value === 'string') {
      transformed.push(...(splitBareArtifactPaths(child.value) || [child]));
      continue;
    }
    transformTextNodes(child);
    transformed.push(child);
  }
  parent.children = transformed;
}


/**
 * Turn bare absolute file paths in task chat prose into Markdown links.
 * Existing links, inline code, code blocks, and HTML are deliberately left
 * untouched. The task-scoped backend endpoint remains the security boundary.
 */
export function remarkTaskArtifactPaths() {
  return (tree: unknown): void => {
    if (isMarkdownNode(tree)) transformTextNodes(tree);
  };
}
