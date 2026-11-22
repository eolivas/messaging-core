import { useEffect } from 'react';
import type { Dispatch } from 'react';
import type { CountersDto, FlowStepDto, SessionStatusDto } from '../api/types';
import type { Action } from '../state/reducer';
import { useStore } from '../state/StoreContext';

/** No event of any kind for this long means the stream is disconnected (9.11). */
export const WATCHDOG_MS = 1500;
export const RECONNECT_DELAYS_MS = [1000, 2000, 4000, 8000, 10000] as const;

/** Delay before reconnect attempt `attempt` (0-based): 1, 2, 4, 8, then 10 s. */
export function reconnectDelayMs(attempt: number): number {
  const i = Math.max(0, Math.floor(attempt));
  return RECONNECT_DELAYS_MS[Math.min(i, RECONNECT_DELAYS_MS.length - 1)];
}

export interface EventSourceLike {
  onopen: ((ev: Event) => void) | null;
  onerror: ((ev: Event) => void) | null;
  addEventListener(type: string, listener: (ev: MessageEvent) => void): void;
  close(): void;
}

export type EventSourceFactory = (url: string) => EventSourceLike;

const defaultFactory: EventSourceFactory = (url) =>
  new EventSource(url) as unknown as EventSourceLike;

/**
 * Connects to the SSE endpoint, keeps a heartbeat watchdog and reconnects with
 * backoff. Feed entries are never cleared here, so they stay visible while
 * disconnected (9.11). Replayed steps are de-duplicated by sequence in the
 * reducer (9.10). Returns a function that stops the stream.
 */
export function connectEventStream(
  url: string,
  dispatch: Dispatch<Action>,
  createSource: EventSourceFactory = defaultFactory,
): () => void {
  let stopped = false;
  let source: EventSourceLike | null = null;
  let watchdog: ReturnType<typeof setTimeout> | undefined;
  let retry: ReturnType<typeof setTimeout> | undefined;
  let attempt = 0;

  const armWatchdog = () => {
    clearTimeout(watchdog);
    watchdog = setTimeout(lost, WATCHDOG_MS);
  };

  const parse = <T>(ev: MessageEvent): T | null => {
    try {
      return JSON.parse(String(ev.data)) as T;
    } catch {
      return null;
    }
  };

  const alive = () => {
    attempt = 0;
    armWatchdog();
    dispatch({ type: 'connectionChanged', status: 'connected' });
  };

  function lost() {
    if (stopped) return;
    clearTimeout(watchdog);
    source?.close();
    source = null;
    dispatch({ type: 'connectionChanged', status: 'disconnected' });
    const delay = reconnectDelayMs(attempt);
    attempt += 1;
    clearTimeout(retry);
    retry = setTimeout(open, delay);
  }

  function open() {
    if (stopped) return;
    const es = createSource(url);
    source = es;
    // Watchdog also covers a connection that never delivers anything.
    armWatchdog();
    const guard = (fn: (ev: MessageEvent) => void) => (ev: MessageEvent) => {
      if (stopped || source !== es) return;
      alive();
      fn(ev);
    };
    es.onopen = () => {
      if (stopped || source !== es) return;
      alive();
    };
    es.onerror = () => {
      if (stopped || source !== es) return;
      lost();
    };
    es.addEventListener('heartbeat', guard(() => undefined));
    es.addEventListener(
      'flowStep',
      guard((ev) => {
        const step = parse<FlowStepDto>(ev);
        if (step) dispatch({ type: 'stepReceived', step });
      }),
    );
    es.addEventListener(
      'session',
      guard((ev) => {
        const session = parse<SessionStatusDto>(ev);
        if (session) dispatch({ type: 'sessionUpdated', session });
      }),
    );
    es.addEventListener(
      'counters',
      guard((ev) => {
        const counters = parse<CountersDto>(ev);
        if (counters) dispatch({ type: 'countersReceived', counters });
      }),
    );
  }

  open();

  return () => {
    stopped = true;
    clearTimeout(watchdog);
    clearTimeout(retry);
    source?.close();
    source = null;
  };
}

export function useEventStream(url: string): void {
  const { dispatch } = useStore();
  useEffect(() => connectEventStream(url, dispatch), [url, dispatch]);
}
