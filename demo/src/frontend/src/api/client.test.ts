import { describe, expect, it, vi } from 'vitest';
import { ApiError, createApiClient } from './client';
import { parseProblemDetails } from './problem';
import { parseBaseUrl } from '../config/parseBaseUrl';

describe('parseBaseUrl', () => {
  it('rejects missing and relative values', () => {
    for (const v of [undefined, '', '  ', '/api', 'localhost:5080', 'ftp://x']) {
      expect(parseBaseUrl(v).ok).toBe(false);
    }
  });
  it('accepts absolute http URLs and strips the trailing slash', () => {
    expect(parseBaseUrl('http://localhost:5080/')).toEqual({ ok: true, baseUrl: 'http://localhost:5080' });
  });
});

describe('parseProblemDetails', () => {
  it('reads field errors and correlation id', () => {
    const p = parseProblemDetails(400, { title: 'Validation', errors: { a: ['bad'] } }, 'abc');
    expect(p.errors).toEqual({ a: ['bad'] });
    expect(p.correlationId).toBe('abc');
  });
  it('tolerates a non-object body', () => {
    expect(parseProblemDetails(502, 'x').title).toContain('502');
  });
});

describe('api client', () => {
  it('maps error responses to ApiError with problem details', async () => {
    const fetchImpl = vi.fn(async () =>
      new Response(JSON.stringify({ title: 'Conflict', detail: 'busy' }), { status: 409 }),
    );
    const client = createApiClient({ baseUrl: 'http://x', fetchImpl });
    const err = await client.getSession().catch((e: unknown) => e);
    expect(err).toBeInstanceOf(ApiError);
    expect((err as ApiError).problem?.status).toBe(409);
    expect(fetchImpl).toHaveBeenCalledWith('http://x/api/v1/session', expect.anything());
  });

  it('aborts after the timeout', async () => {
    vi.useFakeTimers();
    try {
      const fetchImpl = vi.fn(
        (_u: RequestInfo | URL, init?: RequestInit) =>
          new Promise<Response>((_res, rej) => {
            init?.signal?.addEventListener('abort', () => rej(new DOMException('a', 'AbortError')));
          }),
      );
      const client = createApiClient({ baseUrl: 'http://x', fetchImpl: fetchImpl as typeof fetch });
      const pending = client.listProviders().catch((e: unknown) => e);
      await vi.advanceTimersByTimeAsync(10_000);
      expect(((await pending) as ApiError).kind).toBe('timeout');
    } finally {
      vi.useRealTimers();
    }
  });
});
