import { describe, expect, it } from 'vitest';

import { skillSupportedByProvider } from './skillCapabilities';

describe('skillSupportedByProvider', () => {
  it('keeps Claude skills unchanged', () => {
    expect(skillSupportedByProvider('claude', 'monitor', false)).toBe(true);
  });

  it('allows Monitor only for a confirmed local Codex scope', () => {
    expect(skillSupportedByProvider(
      'codex',
      'monitor',
      true,
      true,
      false,
    )).toBe(true);
    expect(skillSupportedByProvider(
      'codex',
      'monitor',
      true,
      true,
      true,
    )).toBe(false);
    expect(skillSupportedByProvider(
      'codex',
      'monitor',
      true,
      false,
      false,
    )).toBe(false);
  });

  it('keeps only Sub-Agent when Codex main-task MCP is disabled', () => {
    expect(skillSupportedByProvider('codex', 'sub-agent', false)).toBe(true);
    expect(skillSupportedByProvider('codex', 'code-review', false)).toBe(false);
  });
});
