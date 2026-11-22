import { describe, expect, it } from 'vitest';
import type { FlowStepDto, SessionStatusDto } from '../api/types';
import { FEED_CAP, initialState, isCompleted, reducer, highlightedEntries } from './reducer';

function step(sequence: number, over: Partial<FlowStepDto> = {}): FlowStepDto {
  return {
    sequence,
    service: 'SalesLeads',
    kind: 'Published',
    correlationId: 'c1',
    leadId: 'l1',
    occurredAtUtc: new Date(Date.UTC(2024, 0, 1, 0, 0, 0, sequence)).toISOString(),
    attempt: null,
    failureKind: null,
    reason: null,
    completed: false,
    ...over,
  };
}

const session: SessionStatusDto = {
  state: 'Active',
  providerId: 'rabbitmq',
  providerName: 'RabbitMQ',
  health: 'Healthy',
  services: [],
  lastError: null,
};

describe('reducer', () => {
  it('orders newest first, de-duplicates by sequence and counts once', () => {
    let s = reducer(initialState, { type: 'stepReceived', step: step(1) });
    s = reducer(s, { type: 'stepReceived', step: step(2, { correlationId: 'c2' }) });
    s = reducer(s, { type: 'stepReceived', step: step(1) });
    expect(s.feed.map((x) => x.sequence)).toEqual([2, 1]);
    expect(s.counters.published).toBe(1 + 1);
  });

  it('caps the feed at 500 keeping the newest', () => {
    const steps = Array.from({ length: FEED_CAP + 20 }, (_, i) => step(i + 1));
    const s = reducer(initialState, { type: 'stepsReceived', steps });
    expect(s.feed).toHaveLength(FEED_CAP);
    expect(s.feed[0]!.sequence).toBe(FEED_CAP + 20);
  });

  it('keeps entries with missing correlation and lead ids', () => {
    const s = reducer(initialState, {
      type: 'stepReceived',
      step: step(1, { correlationId: null, leadId: null }),
    });
    expect(s.feed).toHaveLength(1);
    expect(isCompleted(s, s.feed[0]!)).toBe(false);
  });

  it('marks an event completed after Published, CustomerCreated and EmailSent', () => {
    const kinds = ['Published', 'CustomerCreated'] as const;
    let s = initialState;
    kinds.forEach((kind, i) => {
      s = reducer(s, { type: 'stepReceived', step: step(i + 1, { kind }) });
    });
    expect(s.completed['c1']).toBeUndefined();
    s = reducer(s, { type: 'stepReceived', step: step(3, { kind: 'EmailSent' }) });
    expect(isCompleted(s, s.feed[0]!)).toBe(true);
  });

  it('clear resets feed and counters but keeps the session', () => {
    let s = reducer(initialState, { type: 'sessionUpdated', session });
    s = reducer(s, { type: 'stepReceived', step: step(1) });
    s = reducer(s, { type: 'feedCleared' });
    expect(s.feed).toEqual([]);
    expect(s.counters.published).toBe(0);
    expect(s.session).toBe(session);
  });

  it('resets counters on a new session', () => {
    let s = reducer(initialState, { type: 'stepReceived', step: step(1) });
    s = reducer(s, { type: 'sessionStarted', session });
    expect(s.counters).toEqual({ published: 0, customersCreated: 0, emailsSent: 0 });
  });

  it('discards stale defaults responses', () => {
    const data = { providerId: 'a', messagingMode: 'x', settings: [] };
    let s = reducer(initialState, { type: 'defaultsRequested', providerId: 'a', token: 1 });
    s = reducer(s, { type: 'defaultsRequested', providerId: 'b', token: 2 });
    s = reducer(s, { type: 'defaultsLoaded', token: 1, data });
    expect(s.defaults.status).toBe('loading');
    s = reducer(s, { type: 'defaultsLoaded', token: 2, data: { ...data, providerId: 'b' } });
    expect(s.defaults.status).toBe('loaded');
  });

  it('highlights exactly entries with the selected correlation id', () => {
    let s = reducer(initialState, {
      type: 'stepsReceived',
      steps: [step(1), step(2, { correlationId: 'c2' })],
    });
    s = reducer(s, { type: 'correlationSelected', correlationId: 'c2' });
    expect(highlightedEntries(s).map((x) => x.sequence)).toEqual([2]);
  });
});
