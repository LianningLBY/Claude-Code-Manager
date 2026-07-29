export function skillSupportedByProvider(
  provider: string,
  skillKey: string,
  codexTaskSkillsEnabled = true,
): boolean {
  if (provider !== 'codex') return true;
  if (skillKey === 'monitor') return false;
  return codexTaskSkillsEnabled || skillKey === 'sub-agent';
}
