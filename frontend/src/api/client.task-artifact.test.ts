import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { api } from './client';
import { setServerUrl } from '../config/server';


describe('task artifact downloads', () => {
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

  it('uses authenticated task scope and decodes the response filename', async () => {
    const blob = new Blob(['report']);
    fetchMock.mockResolvedValue({
      status: 200,
      ok: true,
      headers: {
        get: (name: string) => name.toLowerCase() === 'content-disposition'
          ? "attachment; filename*=UTF-8''%E6%B1%87%E6%8A%A5%E7%A8%BF.md"
          : null,
      },
      blob: async () => blob,
    });

    const result = await api.downloadTaskArtifact(17, '输出/汇报稿.md');

    expect(fetchMock).toHaveBeenCalledWith(
      'https://ccm.example.com/api/tasks/17/artifacts/download?path=%E8%BE%93%E5%87%BA%2F%E6%B1%87%E6%8A%A5%E7%A8%BF.md',
      { headers: { Authorization: 'Bearer task-token' } },
    );
    expect(result).toEqual({ blob, filename: '汇报稿.md' });
  });

  it('surfaces a task artifact error returned by the server', async () => {
    fetchMock.mockResolvedValue({
      status: 403,
      statusText: 'Forbidden',
      ok: false,
      headers: { get: () => null },
      json: async () => ({
        detail: 'Artifact path is outside the task workspace',
      }),
    });

    await expect(
      api.downloadTaskArtifact(17, '../private.txt'),
    ).rejects.toThrow('Artifact path is outside the task workspace');
  });
});
