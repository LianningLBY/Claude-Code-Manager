import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { api } from './client';
import { setServerUrl } from '../config/server';

describe('PR finding action API', () => {
  const fetchMock = vi.fn();

  beforeEach(() => {
    vi.stubGlobal('fetch', fetchMock);
    setServerUrl('https://ccm.example.com');
    localStorage.setItem('cc_token', 'review-token');
  });

  afterEach(() => {
    localStorage.clear();
    vi.unstubAllGlobals();
    vi.clearAllMocks();
  });

  it('returns both confirmation credentials only with the downloaded diff', async () => {
    const blob = new Blob(['diff --git a/file b/file']);
    fetchMock.mockResolvedValue({
      status: 200,
      ok: true,
      headers: {
        get: (name: string) => ({
          'content-disposition': 'inline; filename="reviewed-fix.diff"',
          'x-refreshed-token': 'renewed-review-token',
          'x-ccm-pr-fix-receipt': 'receipt-value',
          'x-ccm-pr-fix-token': 'confirmation-token-value',
        })[name.toLowerCase()] ?? null,
      },
      blob: async () => blob,
    });

    const result = await api.downloadReviewFindingDiff(37);

    expect(fetchMock).toHaveBeenCalledWith(
      'https://ccm.example.com/api/pr-monitor/actions/37/diff',
      { headers: { Authorization: 'Bearer review-token' } },
    );
    expect(result).toEqual({
      blob,
      filename: 'reviewed-fix.diff',
      receipt: 'receipt-value',
      confirmationToken: 'confirmation-token-value',
    });
    expect(localStorage.getItem('cc_token')).toBe('renewed-review-token');
  });

  it('clears authentication and rejects a 401 diff response', async () => {
    const reload = vi.fn();
    vi.stubGlobal('window', { location: { reload } });
    fetchMock.mockResolvedValue({
      status: 401,
      statusText: 'Unauthorized',
      ok: false,
      headers: { get: () => null },
    });

    await expect(api.downloadReviewFindingDiff(40)).rejects.toThrow('Unauthorized');

    expect(localStorage.getItem('cc_token')).toBeNull();
    expect(reload).toHaveBeenCalledOnce();
  });

  it('surfaces a FastAPI JSON detail instead of the raw response body', async () => {
    const text = vi.fn();
    fetchMock.mockResolvedValue({
      status: 409,
      statusText: 'Conflict',
      ok: false,
      headers: { get: () => null },
      json: async () => ({ detail: 'Validated PR fix diff is not available' }),
      text,
    });

    await expect(api.downloadReviewFindingDiff(41)).rejects.toThrow(
      'Validated PR fix diff is not available',
    );
    expect(text).not.toHaveBeenCalled();
  });

  it.each([
    ['X-CCM-PR-Fix-Receipt', 'PR fix download receipt is missing'],
    ['X-CCM-PR-Fix-Token', 'PR fix confirmation token is missing'],
  ])('rejects a diff response missing %s', async (missingHeader, message) => {
    fetchMock.mockResolvedValue({
      status: 200,
      ok: true,
      headers: {
        get: (name: string) => {
          if (name.toLowerCase() === missingHeader.toLowerCase()) return null;
          if (name.toLowerCase() === 'x-ccm-pr-fix-receipt') return 'receipt-value';
          if (name.toLowerCase() === 'x-ccm-pr-fix-token') return 'confirmation-token-value';
          return null;
        },
      },
      blob: vi.fn(),
    });

    await expect(api.downloadReviewFindingDiff(38)).rejects.toThrow(message);
  });

  it('posts cancellation to the audited action endpoint', async () => {
    fetchMock.mockResolvedValue({
      status: 200,
      ok: true,
      headers: { get: () => null },
      json: async () => ({ id: 39, status: 'cancelled' }),
    });

    await api.cancelPRFindingAction(39);

    expect(fetchMock).toHaveBeenCalledWith(
      'https://ccm.example.com/api/pr-monitor/actions/39/cancel',
      expect.objectContaining({
        method: 'POST',
        headers: expect.objectContaining({ Authorization: 'Bearer review-token' }),
      }),
    );
  });
});
