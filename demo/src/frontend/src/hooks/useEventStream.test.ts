import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import fc from 'fast-check';
import {
  connectEventStream,
  reconnectDelayMs,
  type EventSourceLike,
} from './useEventStream';
import { initialState, reducer, type Action, type AppState } from '../state/reducer';
import type { FlowStepDto } from '../api/types';

class FakeSource implements EventSourceLike {
  onopen: ((ev: Event) => void) | null = null;
  onerror: ((ev: Event) => void) | null = null;
  closed = false;
  private listeners = new Map<string, (ev: MessageEvent) => void>();
  addEventListener(type: string, l: (ev: MessageEvent) => void) {
    this.listeners.set(type, l);
  }
  close() {
    this.closed = true;
  }
  emit(type: string, data: unknown = {}) {
    this.listeners.get(type)?.({ data: JSON.stringify(data) } as MessageEvent);
  }
}

function step(sequence: number): FlowStepDto {
  return {
    sequence,
    service: 'SalesLeads',
    kind: 'Published',
    correlationId: `c${sequence}`,
    leadId: null,
    occurredAtUtc: new Date(1_700_000_000_000 + sequence * 1000).toISOString(),
    attempt: null,
    failureKind: null,
    reason: null,
    completed: false,
  };
}

describe('reconnectDelayMs', () => {
  it('follows 1, 2, 4, 8, then 10 s', () => {
    expect([0, 1, 2, 3, 4, 5, 9].map(reconnectDelayMs)).toEqual([
      1000, 2000, 4000, 8000, 10000, 10000, 10000,
    ]);
  });

  it('is non-decreasing and capped at 10 s', () => {
    fc.assert(
      fc.property(fc.nat(1000), (n) => {
        expect(reconnectDelayMs(n + 1)).toBeGreaterThanOrEqual(reconnectDelayMs(n));
        expect(reconnectDelayMs(n)).toBeLessThanOrEqual(10000);
      }),
    );
  });
});

describe('connectEventStream', () => {
  let sources: FakeSource[];
  let state: AppState;
  let actions: Action[];
  const dispatch = (a: Action) => {
    actions.push(a);
    state = reducer(state, a);
  };
  const factory = () => {
    const s = new FakeSource();
    sources.push(s);
    return s;
  };

  beforeEach(() => {
    vi.useFakeTimers();
    sources = [];
    actions = [];
    state = initialState;
  });
  afterEach(() => vi.useRealTimers());

  it('reports disconnected after 1.5 s without events and keeps the feed', () => {
    const stop = connectEventStream('u', dispatch, factory);
    sources[0].onopen?.(new Event('open'));
    sources[0].emit('flowStep', step(1));
    expect(state.connection).toBe('connected');
    vi.advanceTimersByTime(1499);
    expect(state.connection).toBe('connected');
    vi.advanceTimersByTime(1);
    expect(state.connection).toBe('disconnected');
    expect(state.feed).toHaveLength(1);
    expect(sources[0].closed).toBe(true);
    stop();
  });

  it('heartbeats keep the connection alive', () => {
    const stop = connectEventStream('u', dispatch, factory);
    for (let i = 0; i < 6; i++) {
      vi.advanceTimersByTime(500);
      sources[0].emit('heartbeat');
    }
    expect(state.connection).toBe('connected');
    stop();
  });

  it('reconnects with growing delays and de-duplicates replayed steps', () => {
    const stop = connectEventStream('u', dispatch, factory);
    sources[0].emit('flowStep', step(1));
    sources[0].emit('flowStep', step(2));
    vi.advanceTimersByTime(1500); // lost; retry in 1 s
    expect(sources).toHaveLength(1);
    vi.advanceTimersByTime(1000);
    expect(sources).toHaveLength(2);
    sources[1].emit('flowStep', step(1)); // replay
    sources[1].emit('flowStep', step(2));
    sources[1].emit('flowStep', step(3));
    expect(state.feed.map((s) => s.sequence).sort()).toEqual([1, 2, 3]);
    expect(state.connection).toBe('connected');
    // Failure without any event: next delay is 1 s again (reset after success).
    sources[1].onerror?.(new Event('error'));
    vi.advanceTimersByTime(999);
    expect(sources).toHaveLength(2);
    vi.advanceTimersByTime(1);
    expect(sources).toHaveLength(3);
    // Consecutive failures without events: 2 s next.
    sources[2].onerror?.(new Event('error'));
    vi.advanceTimersByTime(1999);
    expect(sources).toHaveLength(3);
    vi.advanceTimersByTime(1);
    expect(sources).toHaveLength(4);
    sources[3].onerror?.(new Event('error'));
    vi.advanceTimersByTime(3999);
    expect(sources).toHaveLength(4);
    vi.advanceTimersByTime(1);
    expect(sources).toHaveLength(5);
    stop();
  });

  it('stops cleanly', () => {
    const stop = connectEventStream('u', dispatch, factory);
    stop();
    vi.advanceTimersByTime(60_000);
    expect(sources).toHaveLength(1);
    expect(sources[0].closed).toBe(true);
  });
});
