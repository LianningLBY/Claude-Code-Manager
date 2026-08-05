import '@testing-library/jest-dom/vitest';

// Newer Node runtimes expose a placeholder global localStorage unless a
// persistence file is configured. It shadows jsdom's implementation but has
// no Storage methods, so normalize it for browser-focused tests.
if (typeof globalThis.localStorage?.getItem !== 'function') {
  const values = new Map<string, string>();
  Object.defineProperty(globalThis, 'localStorage', {
    configurable: true,
    value: {
      get length() {
        return values.size;
      },
      clear: () => values.clear(),
      getItem: (key: string) => values.get(String(key)) ?? null,
      key: (index: number) => [...values.keys()][index] ?? null,
      removeItem: (key: string) => values.delete(String(key)),
      setItem: (key: string, value: string) => values.set(String(key), String(value)),
    },
  });
}

// jsdom doesn't implement scrollIntoView
Element.prototype.scrollIntoView = () => {};
