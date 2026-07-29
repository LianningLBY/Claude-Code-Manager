export function skillSupportedByProvider(
  provider: string,
  skillKey: string,
  codexTaskSkillsEnabled = true,
  codexMonitorEnabled = false,
  remoteTaskScope = false,
): boolean {
  if (provider !== 'codex') return true;
  if (skillKey === 'monitor') {
    return codexTaskSkillsEnabled
      && codexMonitorEnabled
      && !remoteTaskScope;
  }
  return codexTaskSkillsEnabled || skillKey === 'sub-agent';
}
