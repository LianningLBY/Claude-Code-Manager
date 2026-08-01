type MathDelimiter = 'inline' | 'display';

interface OpenDelimiter {
  kind: MathDelimiter;
  position: number;
}

function markRange(protectedCharacters: Uint8Array, start: number, end: number) {
  protectedCharacters.fill(1, start, end);
}

/**
 * Mark fenced and inline code before normalizing LaTeX delimiters. Formula-like
 * text in source snippets must remain byte-for-byte unchanged.
 */
function findCodeCharacters(source: string): Uint8Array {
  const protectedCharacters = new Uint8Array(source.length);
  let fence: { character: '`' | '~'; length: number } | null = null;
  let offset = 0;

  for (const line of source.matchAll(/.*(?:\n|$)/g)) {
    const value = line[0];
    if (!value) continue;
    const lineWithoutEnding = value.replace(/[\r\n]+$/, '');

    if (fence) {
      markRange(protectedCharacters, offset, offset + value.length);
      const closingFence = lineWithoutEnding.match(/^ {0,3}(`+|~+)\s*$/);
      if (
        closingFence
        && closingFence[1][0] === fence.character
        && closingFence[1].length >= fence.length
      ) {
        fence = null;
      }
    } else {
      const openingFence = lineWithoutEnding.match(/^ {0,3}(`{3,}|~{3,})/);
      if (openingFence) {
        const marker = openingFence[1];
        fence = {
          character: marker[0] as '`' | '~',
          length: marker.length,
        };
        markRange(protectedCharacters, offset, offset + value.length);
      }
    }
    offset += value.length;
  }

  for (let index = 0; index < source.length;) {
    if (protectedCharacters[index] || source[index] !== '`') {
      index += 1;
      continue;
    }

    let markerLength = 1;
    while (source[index + markerLength] === '`') markerLength += 1;
    let closing = index + markerLength;
    while (closing < source.length) {
      if (protectedCharacters[closing] || source[closing] !== '`') {
        closing += 1;
        continue;
      }
      let closingLength = 1;
      while (source[closing + closingLength] === '`') closingLength += 1;
      if (closingLength === markerLength) break;
      closing += closingLength;
    }

    if (closing < source.length) {
      const end = closing + markerLength;
      markRange(protectedCharacters, index, end);
      index = end;
    } else {
      index += markerLength;
    }
  }

  return protectedCharacters;
}

/**
 * remark-math understands dollar delimiters, while Codex commonly emits the
 * LaTeX forms \(...\) and \[...\]. Convert only complete pairs outside code.
 */
export function normalizeMathDelimiters(source: string): string {
  const protectedCharacters = findCodeCharacters(source);
  const replacements = new Map<number, string>();
  let open: OpenDelimiter | null = null;

  for (let index = 0; index < source.length - 1; index += 1) {
    if (protectedCharacters[index] || source[index] !== '\\') continue;
    if (index > 0 && source[index - 1] === '\\') continue;

    const next = source[index + 1];
    if (!open) {
      if (next === '(') open = { kind: 'inline', position: index };
      if (next === '[') open = { kind: 'display', position: index };
      continue;
    }

    const isClosing = (
      (open.kind === 'inline' && next === ')')
      || (open.kind === 'display' && next === ']')
    );
    if (!isClosing) continue;

    const replacement = open.kind === 'inline' ? '$' : '$$';
    replacements.set(open.position, replacement);
    replacements.set(index, replacement);
    open = null;
    index += 1;
  }

  if (replacements.size === 0) return source;

  let normalized = '';
  for (let index = 0; index < source.length; index += 1) {
    const replacement = replacements.get(index);
    if (replacement !== undefined) {
      normalized += replacement;
      index += 1;
    } else {
      normalized += source[index];
    }
  }
  return normalized;
}
