import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { api } from './client';
import { setServerUrl } from '../config/server';

describe('remote API routing', () => {
  const fetchMock = vi.fn();

  beforeEach(() => {
    fetchMock.mockResolvedValue({
      status: 200,
      ok: true,
      headers: { get: () => null },
      json: async () => ({ ok: true, text: 'transcribed' }),
    });
    vi.stubGlobal('fetch', fetchMock);
    setServerUrl('https://ccm.example.com');
    localStorage.setItem('cc_token', 'test-token');
  });

  afterEach(() => {
    localStorage.clear();
    vi.unstubAllGlobals();
    vi.clearAllMocks();
  });

  it('routes voice and team/account mutations through the configured base with auth', async () => {
    await api.transcribeVoice(new Blob(['audio'], { type: 'audio/webm' }));
    await api.getTeamGroups();
    await api.changePassword('old', 'new');
    await api.updateTeamUserRole(7, 'admin');

    expect(fetchMock.mock.calls.map(([url]) => url)).toEqual([
      'https://ccm.example.com/api/voice/transcribe',
      'https://ccm.example.com/api/team/groups',
      'https://ccm.example.com/api/auth/me/password',
      'https://ccm.example.com/api/team/users/7/role',
    ]);
    expect(fetchMock.mock.calls[0][1]).toMatchObject({
      method: 'POST',
      headers: { Authorization: 'Bearer test-token' },
    });
    expect(fetchMock.mock.calls[0][1].headers).not.toHaveProperty('Content-Type');
    expect(fetchMock.mock.calls.slice(1).every(([, options]) =>
      options.headers.Authorization === 'Bearer test-token'
    )).toBe(true);
  });
});
