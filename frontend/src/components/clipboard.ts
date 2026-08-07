function fallbackCopy(text: string): Promise<void> {
  return new Promise((resolve, reject) => {
    const textarea = document.createElement('textarea');
    textarea.value = text;
    textarea.style.position = 'fixed';
    textarea.style.left = '-9999px';
    textarea.style.opacity = '0';
    document.body.appendChild(textarea);
    textarea.focus();
    textarea.select();
    try {
      if (document.execCommand('copy')) resolve();
      else reject();
    } catch {
      reject();
    } finally {
      document.body.removeChild(textarea);
    }
  });
}

export function copyToClipboard(text: string): Promise<void> {
  if (navigator.clipboard?.writeText) {
    return navigator.clipboard.writeText(text).catch(() => fallbackCopy(text));
  }
  return fallbackCopy(text);
}
