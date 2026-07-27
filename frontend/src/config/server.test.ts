import { afterEach, describe, expect, it } from 'vitest';
import { getApiBase, getWsUrl, setServerUrl } from './server';

afterEach(() => {
  localStorage.clear();
  delete (window as unknown as { Capacitor?: unknown }).Capacitor;
});

describe('dynamic server configuration', () => {
  it('uses a manually configured remote server for web API and WebSocket calls', () => {
    setServerUrl('https://ccm.example.com/');

    expect(getApiBase()).toBe('https://ccm.example.com');
    expect(getWsUrl()).toBe('wss://ccm.example.com/ws');
  });

  it('falls back to the page origin on web when no remote server is configured', () => {
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    expect(getApiBase()).toBe('');
    expect(getWsUrl()).toBe(`${protocol}//${window.location.host}/ws`);
  });
});
