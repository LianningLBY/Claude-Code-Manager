import { describe, expect, it } from 'vitest';

import { skillSupportedByProvider } from './skillCapabilities';

describe('skillSupportedByProvider', () => {
  it('keeps Claude skills unchanged', () => {
    expect(skillSupportedByProvider('claude', 'monitor', false)).toBe(true);
  });

  it('always excludes Monitor from Codex', () => {
    expect(skillSupportedByProvider('codex', 'monitor', true)).toBe(false);
  });

  it('keeps only Sub-Agent when Codex main-task MCP is disabled', () => {
    expect(skillSupportedByProvider('codex', 'sub-agent', false)).toBe(true);
    expect(skillSupportedByProvider('codex', 'code-review', false)).toBe(false);
  });
});
