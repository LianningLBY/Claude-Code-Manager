import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { api } from './client';

describe('System update API routing', () => {
  const fetchMock = vi.fn();

  beforeEach(() => {
    fetchMock.mockResolvedValue({
      status: 200,
      ok: true,
      headers: { get: () => null },
      json: async () => ({ ok: true }),
    });
    vi.stubGlobal('fetch', fetchMock);
  });

  afterEach(() => {
    localStorage.clear();
    vi.unstubAllGlobals();
    vi.clearAllMocks();
  });

  it('uses dedicated reconcile, repair, restart, and confirmed rollback endpoints', async () => {
    await api.reconcileUpdateState();
    await api.repairUpdate();
    await api.restartService();
    await api.rollbackUpdate({ confirm_database_restore: true });

    expect(fetchMock.mock.calls.map(([url]) => url)).toEqual([
      '/api/system/update/reconcile',
      '/api/system/update/repair',
      '/api/system/restart',
      '/api/system/update/rollback',
    ]);
    expect(fetchMock.mock.calls[0][1]).toMatchObject({
      method: 'POST',
    });
    expect(fetchMock.mock.calls[1][1]).toMatchObject({
      method: 'POST',
      body: '{}',
    });
    expect(fetchMock.mock.calls[2][1]).toMatchObject({ method: 'POST' });
    expect(fetchMock.mock.calls[3][1]).toMatchObject({
      method: 'POST',
      body: JSON.stringify({ confirm_database_restore: true }),
    });
  });

  it('preserves structured API error details while exposing a readable message', async () => {
    const detail = { error: '部署正在执行', code: 'deployment_busy' };
    fetchMock.mockResolvedValueOnce({
      status: 409,
      ok: false,
      headers: { get: () => null },
      json: async () => ({ detail }),
    });

    const error = await api.restartService().catch((caught: unknown) => caught) as Error & {
      status?: number;
      detail?: unknown;
    };

    expect(error).toBeInstanceOf(Error);
    expect(error.message).toBe('部署正在执行');
    expect(error.status).toBe(409);
    expect(error.detail).toEqual(detail);
  });
});
