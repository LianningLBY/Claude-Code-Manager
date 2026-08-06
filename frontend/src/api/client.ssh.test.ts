import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { api } from './client';
import { setServerUrl } from '../config/server';

describe('Task SSH grant API', () => {
  const fetchMock = vi.fn();

  beforeEach(() => {
    vi.stubGlobal('fetch', fetchMock);
    setServerUrl('https://ccm.example.com');
    localStorage.setItem('cc_token', 'task-token');
  });

  afterEach(() => {
    localStorage.clear();
    vi.unstubAllGlobals();
    vi.clearAllMocks();
  });

  it('replaces grants with profile ids and capabilities only', async () => {
    fetchMock.mockResolvedValue({
      status: 200,
      ok: true,
      headers: { get: () => null },
      json: async () => [],
    });

    await api.updateTaskSSHGrants(17, [
      { profile_id: 41, capabilities: ['read', 'write'] },
    ]);

    expect(fetchMock).toHaveBeenCalledWith(
      'https://ccm.example.com/api/tasks/17/ssh-grants',
      expect.objectContaining({
        method: 'PUT',
        body: JSON.stringify({
          grants: [{ profile_id: 41, capabilities: ['read', 'write'] }],
        }),
        headers: expect.objectContaining({ Authorization: 'Bearer task-token' }),
      }),
    );
  });

  it('surfaces structured profile validation messages', async () => {
    fetchMock.mockResolvedValue({
      status: 409,
      statusText: 'Conflict',
      ok: false,
      headers: { get: () => null },
      json: async () => ({
        detail: { code: 'profile_revision_changed', message: 'Re-authorize this SSH profile' },
      }),
    });

    await expect(api.updateTaskSSHGrants(17, [
      { profile_id: 41, capabilities: ['exec'] },
    ])).rejects.toThrow('Re-authorize this SSH profile');
  });
});
