import { act, render, screen, waitFor } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { App } from './App';
import type { ApiClient } from './api/client';
import type { SessionStatusDto } from './api/types';

type Listener = (ev: MessageEvent) => void;
const sources: { listeners: Record<string, Listener> }[] = [];

class FakeEventSource {
  onopen: ((ev: Event) => void) | null = null;
  onerror: ((ev: Event) => void) | null = null;
  listeners: Record<string, Listener> = {};
  constructor() {
    sources.push(this);
  }
  addEventListener(type: string, l: Listener) {
    this.listeners[type] = l;
  }
  close() {}
}

const activeSession: SessionStatusDto = {
  state: 'Active',
  providerId: 'rabbitmq',
  providerName: 'RabbitMQ',
  health: 'Reachable',
  services: [
    { service: 'SalesLeads', role: 'publisher', ready: true },
    { service: 'Customer', role: 'consumer', ready: true },
    { service: 'Communications', role: 'consumer', ready: true },
  ],
  lastError: null,
};

function fakeClient(): ApiClient {
  return {
    listProviders: vi.fn().mockResolvedValue([
      { id: 'rabbitmq', displayName: 'RabbitMQ', supported: true, unsupportedReason: null },
    ]),
    getProviderDefaults: vi.fn(),
    validateSettings: vi.fn(),
    startSession: vi.fn(),
    stopSession: vi.fn(),
    getSession: vi.fn(),
    triggerQuoteAccepted: vi.fn(),
    setFailureMode: vi.fn(),
    clearFlowSteps: vi.fn(),
    getIntegrationDetails: vi.fn().mockResolvedValue({
      messagingMode: 'Broker',
      providerName: 'RabbitMQ',
      idempotencyEnabled: true,
      retryMaxAttempts: 3,
      backoffStrategy: 'Exponential',
    }),
    eventsUrl: () => 'http://localhost:5080/api/v1/events',
  } as unknown as ApiClient;
}

beforeEach(() => {
  sources.length = 0;
  vi.stubGlobal('EventSource', FakeEventSource);
});
afterEach(() => vi.unstubAllGlobals());

describe('App', () => {
  it('shows ConfigError and sends no request for an invalid base URL', () => {
    const fetchSpy = vi.fn();
    vi.stubGlobal('fetch', fetchSpy);
    const createClient = vi.fn();
    render(<App config={{ ok: false, reason: 'VITE_BFF_BASE_URL is missing.' }} createClient={createClient} />);
    expect(screen.getByText('Configuration error')).toBeInTheDocument();
    expect(createClient).not.toHaveBeenCalled();
    expect(fetchSpy).not.toHaveBeenCalled();
    expect(sources).toHaveLength(0);
  });

  it('loads providers on startup and hides trigger and diagram until Active', async () => {
    const client = fakeClient();
    render(<App config={{ ok: true, baseUrl: 'http://localhost:5080' }} createClient={() => client} />);
    expect(await screen.findByText('RabbitMQ')).toBeInTheDocument();
    expect(client.listProviders).toHaveBeenCalledTimes(1);
    expect(screen.queryByRole('button', { name: 'Quote Accepted' })).not.toBeInTheDocument();
    expect(screen.queryByRole('img', { name: 'Flow diagram' })).not.toBeInTheDocument();
  });

  it('shows trigger and diagram once the Session is Active', async () => {
    const client = fakeClient();
    render(<App config={{ ok: true, baseUrl: 'http://localhost:5080' }} createClient={() => client} />);
    await waitFor(() => expect(sources).toHaveLength(1));
    act(() => {
      sources[0].listeners.session(new MessageEvent('session', { data: JSON.stringify(activeSession) }));
    });
    expect(await screen.findByRole('button', { name: 'Quote Accepted' })).toBeEnabled();
    expect(screen.getByRole('img', { name: 'Flow diagram' })).toBeInTheDocument();
  });
});
