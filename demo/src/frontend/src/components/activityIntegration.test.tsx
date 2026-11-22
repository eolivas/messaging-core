import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';
import type { FlowStepDto, SessionStatusDto } from '../api/types';
import { initialState, type AppState } from '../state/reducer';
import { StoreProvider } from '../state/StoreContext';
import { ActivityFeed } from './ActivityFeed';
import { IntegrationDetailsView } from './IntegrationDetailsView';
import { SessionBar } from './SessionBar';

function step(sequence: number, over: Partial<FlowStepDto> = {}): FlowStepDto {
  return {
    sequence,
    service: 'SalesLeads',
    kind: 'Published',
    correlationId: 'c1',
    leadId: 'l1',
    occurredAtUtc: new Date(Date.UTC(2024, 0, 1, 10, 20, 30, sequence)).toISOString(),
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

function withState(over: Partial<AppState>, ui: React.ReactNode) {
  return render(<StoreProvider initial={{ ...initialState, ...over }}>{ui}</StoreProvider>);
}

describe('ActivityFeed', () => {
  it('shows UTC time, service, correlation id and lead id', () => {
    withState({ feed: [step(1)] }, <ActivityFeed />);
    const row = screen.getByTestId('activity-entry');
    expect(row).toHaveTextContent('2024-01-01 10:20:30.001 UTC');
    expect(row).toHaveTextContent('SalesLeads');
    expect(row).toHaveTextContent('c1');
    expect(row).toHaveTextContent('l1');
  });

  it('shows empty fields for missing ids', () => {
    withState({ feed: [step(1, { correlationId: null, leadId: null })] }, <ActivityFeed />);
    expect(screen.getAllByTestId('activity-entry')).toHaveLength(1);
  });

  it('highlights only entries of the selected correlation id', async () => {
    const feed = [step(2, { correlationId: 'c2' }), step(1)];
    withState({ feed }, <ActivityFeed />);
    await userEvent.click(screen.getByRole('button', { name: 'c2' }));
    const rows = screen.getAllByTestId('activity-entry');
    expect(rows[0]).toHaveAttribute('data-highlighted', 'true');
    expect(rows[1]).toHaveAttribute('data-highlighted', 'false');
  });

  it('marks completed events', () => {
    withState({ feed: [step(1)], completed: { c1: true } }, <ActivityFeed />);
    expect(screen.getByTestId('activity-entry')).toHaveAttribute('data-completed', 'true');
  });

  it('clear removes entries after the server call succeeds', async () => {
    const onClear = vi.fn().mockResolvedValue(undefined);
    withState({ feed: [step(1)] }, <ActivityFeed onClear={onClear} />);
    await userEvent.click(screen.getByRole('button', { name: 'Clear' }));
    await waitFor(() => expect(screen.queryAllByTestId('activity-entry')).toHaveLength(0));
    expect(onClear).toHaveBeenCalledOnce();
  });

  it('keeps entries and shows an error when clearing fails', async () => {
    const onClear = vi.fn().mockRejectedValue(new Error('x'));
    withState({ feed: [step(1)] }, <ActivityFeed onClear={onClear} />);
    await userEvent.click(screen.getByRole('button', { name: 'Clear' }));
    expect(await screen.findByRole('alert')).toBeInTheDocument();
    expect(screen.getAllByTestId('activity-entry')).toHaveLength(1);
  });
});

describe('SessionBar', () => {
  it('shows provider name and health and calls change provider', async () => {
    const onChange = vi.fn();
    withState({ session }, <SessionBar onChangeProvider={onChange} />);
    expect(screen.getByTestId('provider-name')).toHaveTextContent('RabbitMQ');
    expect(screen.getByTestId('provider-health')).toHaveTextContent('Healthy');
    expect(screen.queryByTestId('disconnected-indicator')).toBeNull();
    await userEvent.click(screen.getByRole('button', { name: 'Change provider' }));
    expect(onChange).toHaveBeenCalledOnce();
  });

  it('shows a disconnected indicator', () => {
    withState({ session, connection: 'disconnected' }, <SessionBar onChangeProvider={vi.fn()} />);
    expect(screen.getByTestId('disconnected-indicator')).toBeInTheDocument();
  });

  it('renders nothing without a session', () => {
    const { container } = withState({}, <SessionBar onChangeProvider={vi.fn()} />);
    expect(container).toBeEmptyDOMElement();
  });
});

describe('IntegrationDetailsView', () => {
  it('shows the details', async () => {
    const api = {
      getIntegrationDetails: vi.fn().mockResolvedValue({
        messagingMode: 'PublishSubscribe',
        providerName: 'RabbitMQ',
        idempotencyEnabled: true,
        retryMaxAttempts: 3,
        backoffStrategy: 'Exponential',
      }),
    };
    render(<IntegrationDetailsView api={api} />);
    expect(await screen.findByText('PublishSubscribe')).toBeInTheDocument();
    expect(screen.getByText('Enabled')).toBeInTheDocument();
    expect(screen.getByText('3')).toBeInTheDocument();
    expect(screen.getByText('Exponential')).toBeInTheDocument();
  });

  it('shows an error and no stale values when a reload fails', async () => {
    const ok = {
      messagingMode: 'PublishSubscribe',
      providerName: 'RabbitMQ',
      idempotencyEnabled: true,
      retryMaxAttempts: 3,
      backoffStrategy: 'Exponential',
    };
    const api = {
      getIntegrationDetails: vi.fn().mockResolvedValueOnce(ok).mockRejectedValueOnce(new Error('x')),
    };
    const { rerender } = render(<IntegrationDetailsView api={api} refreshKey="a" />);
    await screen.findByText('RabbitMQ');
    rerender(<IntegrationDetailsView api={api} refreshKey="b" />);
    expect(await screen.findByRole('alert')).toBeInTheDocument();
    expect(screen.queryByText('RabbitMQ')).toBeNull();
    expect(screen.queryByText('PublishSubscribe')).toBeNull();
  });
});
