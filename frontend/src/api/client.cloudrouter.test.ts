import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { api } from './client';

describe('CloudRouter account API routing', () => {
  const fetchMock = vi.fn();

  beforeEach(() => {
    fetchMock.mockResolvedValue({
      status: 200,
      ok: true,
      headers: { get: () => null },
      json: async () => ({ id: 'api/account 1', name: 'CloudRouter' }),
    });
    vi.stubGlobal('fetch', fetchMock);
  });

  afterEach(() => {
    localStorage.clear();
    vi.unstubAllGlobals();
    vi.clearAllMocks();
  });

  it('lists and creates API accounts through the shared endpoint', async () => {
    await api.getCloudRouterAccounts(true);
    await api.createCloudRouterAccount({
      name: 'CloudRouter Claude',
      api_key: 'cr-secret',
    });

    expect(fetchMock.mock.calls.map(([url]) => url)).toEqual([
      '/api/cloudrouter/accounts?force=true',
      '/api/cloudrouter/accounts',
    ]);
    expect(fetchMock.mock.calls[1][1]).toMatchObject({
      method: 'POST',
      body: JSON.stringify({
        name: 'CloudRouter Claude',
        api_key: 'cr-secret',
      }),
    });
  });

  it('encodes account ids for refresh and safe retirement', async () => {
    await api.refreshCloudRouterAccount('api/account 1');
    await api.deleteCloudRouterAccount('api/account 1');

    expect(fetchMock.mock.calls.map(([url]) => url)).toEqual([
      '/api/cloudrouter/accounts/api%2Faccount%201/refresh',
      '/api/cloudrouter/accounts/api%2Faccount%201',
    ]);
    expect(fetchMock.mock.calls[0][1]).toMatchObject({ method: 'POST' });
    expect(fetchMock.mock.calls[1][1]).toMatchObject({ method: 'DELETE' });
  });
});
