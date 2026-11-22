import { parseProblemDetails, type ProblemDetails } from './problem';
import type {
  ConsumerId,
  FailureMode,
  IntegrationDetailsDto,
  ProviderDefaultsDto,
  ProviderDto,
  SessionStatusDto,
  StartSessionRequest,
  TriggerResultDto,
  ValidateSettingsResponse,
} from './types';

export const REQUEST_TIMEOUT_MS = 10_000;
export const API_PREFIX = '/api/v1';

export type ApiErrorKind = 'problem' | 'timeout' | 'network' | 'aborted';

/** Failure of an API call. `problem` is set when the BFF answered with an error status. */
export class ApiError extends Error {
  constructor(
    readonly kind: ApiErrorKind,
    message: string,
    readonly problem: ProblemDetails | null = null,
  ) {
    super(message);
    this.name = 'ApiError';
  }
}

export interface ApiClientOptions {
  baseUrl: string;
  timeoutMs?: number;
  fetchImpl?: typeof fetch;
}

export interface RequestOptions {
  /** Caller cancellation, for example to drop a superseded request. */
  signal?: AbortSignal;
}

export interface ApiClient {
  listProviders(o?: RequestOptions): Promise<ProviderDto[]>;
  getProviderDefaults(id: string, o?: RequestOptions): Promise<ProviderDefaultsDto>;
  validateSettings(
    id: string,
    overrides: Record<string, string>,
    o?: RequestOptions,
  ): Promise<ValidateSettingsResponse>;
  startSession(request: StartSessionRequest, o?: RequestOptions): Promise<SessionStatusDto>;
  stopSession(o?: RequestOptions): Promise<SessionStatusDto>;
  getSession(o?: RequestOptions): Promise<SessionStatusDto>;
  triggerQuoteAccepted(o?: RequestOptions): Promise<TriggerResultDto>;
  setFailureMode(consumer: ConsumerId, mode: FailureMode, o?: RequestOptions): Promise<void>;
  clearFlowSteps(o?: RequestOptions): Promise<void>;
  getIntegrationDetails(o?: RequestOptions): Promise<IntegrationDetailsDto>;
  /** URL of the SSE endpoint, for EventSource. */
  eventsUrl(): string;
}

async function readBody(response: Response): Promise<unknown> {
  const text = await response.text();
  if (text === '') {
    return undefined;
  }
  try {
    return JSON.parse(text) as unknown;
  } catch {
    return undefined;
  }
}

export function createApiClient(options: ApiClientOptions): ApiClient {
  const base = options.baseUrl.replace(/\/+$/, '');
  const timeoutMs = options.timeoutMs ?? REQUEST_TIMEOUT_MS;
  const doFetch = options.fetchImpl ?? ((...args: Parameters<typeof fetch>) => fetch(...args));

  async function request<T>(
    method: string,
    path: string,
    body?: unknown,
    o?: RequestOptions,
  ): Promise<T> {
    const controller = new AbortController();
    let timedOut = false;
    const timer = setTimeout(() => {
      timedOut = true;
      controller.abort();
    }, timeoutMs);
    const onCallerAbort = () => controller.abort();
    if (o?.signal) {
      if (o.signal.aborted) {
        controller.abort();
      } else {
        o.signal.addEventListener('abort', onCallerAbort, { once: true });
      }
    }

    try {
      const headers: Record<string, string> = { Accept: 'application/json' };
      const init: RequestInit = { method, headers, signal: controller.signal };
      if (body !== undefined) {
        headers['Content-Type'] = 'application/json';
        init.body = JSON.stringify(body);
      }
      const response = await doFetch(`${base}${API_PREFIX}${path}`, init);
      const parsed = await readBody(response);
      if (!response.ok) {
        const problem = parseProblemDetails(
          response.status,
          parsed,
          response.headers.get('X-Correlation-Id'),
        );
        throw new ApiError('problem', problem.detail ?? problem.title, problem);
      }
      return parsed as T;
    } catch (error) {
      if (error instanceof ApiError) {
        throw error;
      }
      if (timedOut) {
        throw new ApiError('timeout', `The request timed out after ${timeoutMs / 1000} s.`);
      }
      if (controller.signal.aborted) {
        throw new ApiError('aborted', 'The request was cancelled.');
      }
      throw new ApiError('network', 'The backend could not be reached.');
    } finally {
      clearTimeout(timer);
      o?.signal?.removeEventListener('abort', onCallerAbort);
    }
  }

  const enc = encodeURIComponent;
  return {
    listProviders: (o) => request('GET', '/providers', undefined, o),
    getProviderDefaults: (id, o) => request('GET', `/providers/${enc(id)}/defaults`, undefined, o),
    validateSettings: (id, overrides, o) =>
      request('POST', `/providers/${enc(id)}/settings/validate`, { overrides }, o),
    startSession: (req, o) => request('POST', '/session', req, o),
    stopSession: (o) => request('DELETE', '/session', undefined, o),
    getSession: (o) => request('GET', '/session', undefined, o),
    triggerQuoteAccepted: (o) => request('POST', '/session/quotes', undefined, o),
    setFailureMode: (consumer, mode, o) =>
      request<void>('PUT', `/session/consumers/${enc(consumer)}/failure-mode`, { mode }, o),
    clearFlowSteps: (o) => request<void>('DELETE', '/session/flow-steps', undefined, o),
    getIntegrationDetails: (o) => request('GET', '/session/integration-details', undefined, o),
    eventsUrl: () => `${base}${API_PREFIX}/events`,
  };
}
