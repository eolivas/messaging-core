import { act, fireEvent, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import type { ReactNode } from 'react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import type { FlowStepDto, SessionStatusDto } from '../api/types';
import { initialState, type Action, type AppState } from '../state/reducer';
import { StoreProvider, useStore } from '../state/StoreContext';
import { FailureToggle } from './FailureToggle';
import { FlowDiagram } from './FlowDiagram';
import { MessageMarker } from './MessageMarker';
import { TriggerButton } from './TriggerButton';

afterEach(() => vi.useRealTimers());

const session = (over: Partial<SessionStatusDto> = {}): SessionStatusDto => ({
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
  ...over,
});

const step = (over: Partial<FlowStepDto>): FlowStepDto => ({
  sequence: 1,
  service: 'SalesLeads',
  kind: 'Published',
  correlationId: 'c1',
  leadId: 'lead-1',
  occurredAtUtc: '2024-01-01T00:00:00Z',
  attempt: null,
  failureKind: null,
  reason: null,
  completed: false,
  ...over,
});

let dispatchRef: (a: Action) => void = () => {};
function Capture() {
  dispatchRef = useStore().dispatch;
  return null;
}
function renderWith(ui: ReactNode, initial: AppState = initialState) {
  return render(
    <StoreProvider initial={initial}>
      <Capture />
      {ui}
    </StoreProvider>,
  );
}

describe('FlowDiagram', () => {
  it('shows blocks, roles, broker label, three connections and counts', () => {
    renderWith(<FlowDiagram />, {
      ...initialState,
      session: session(),
      counters: { published: 3, customersCreated: 2, emailsSent: 1 },
    });
    expect(screen.getByText('Sales Leads MS')).toBeInTheDocument();
    expect(screen.getByText('Publisher')).toBeInTheDocument();
    expect(screen.getAllByText('Consumer')).toHaveLength(2);
    expect(screen.getByTestId('broker-label')).toHaveTextContent('RabbitMQ');
    expect(screen.getByTestId('connection-SalesLeads-Broker')).toBeInTheDocument();
    expect(screen.getByTestId('connection-Broker-Customer')).toBeInTheDocument();
    expect(screen.getByTestId('connection-Broker-Communications')).toBeInTheDocument();
    expect(screen.getByTestId('count-SalesLeads')).toHaveTextContent('3');
    expect(screen.getByTestId('count-Customer')).toHaveTextContent('2');
    expect(screen.getByTestId('count-Communications')).toHaveTextContent('1');
  });

  it('animates markers, then highlights with LeadId for 2 s', () => {
    vi.useFakeTimers();
    renderWith(<FlowDiagram />, { ...initialState, session: session() });
    act(() => dispatchRef({ type: 'stepReceived', step: step({}) }));
    expect(screen.getAllByTestId('message-marker')).toHaveLength(1);
    act(() => {
      dispatchRef({
        type: 'stepReceived',
        step: step({ sequence: 2, service: 'Customer', kind: 'Delivered' }),
      });
      dispatchRef({
        type: 'stepReceived',
        step: step({ sequence: 3, service: 'Customer', kind: 'CustomerCreated' }),
      });
      dispatchRef({
        type: 'stepReceived',
        step: step({ sequence: 4, service: 'Communications', kind: 'EmailSent' }),
      });
    });
    expect(screen.getAllByTestId('message-marker')).toHaveLength(2);
    expect(screen.getByTestId('mock-customer')).toHaveTextContent('Mock_Customer lead-1');
    expect(screen.getByTestId('mock-email')).toHaveTextContent('Mock_Email lead-1');
    expect(screen.getByTestId('block-Customer')).toHaveAttribute('data-highlighted', 'true');
    act(() => {
      vi.advanceTimersByTime(1999);
    });
    expect(screen.getByTestId('mock-email')).toBeInTheDocument();
    act(() => {
      vi.advanceTimersByTime(2);
    });
    expect(screen.queryByTestId('mock-email')).toBeNull();
    expect(screen.queryByTestId('mock-customer')).toBeNull();
    expect(screen.queryByTestId('message-marker')).toBeNull();
  });
});

describe('MessageMarker', () => {
  it('starts moving immediately and clamps duration to 500..2000 ms', () => {
    vi.useFakeTimers();
    const done = vi.fn();
    const { rerender } = render(
      <svg>
        <MessageMarker from={{ x: 0, y: 0 }} to={{ x: 10, y: 0 }} durationMs={50} onDone={done} />
      </svg>,
    );
    const m = screen.getByTestId('message-marker');
    expect(m).toHaveAttribute('data-moving', 'true');
    expect(m).toHaveAttribute('data-duration-ms', '500');
    act(() => {
      vi.advanceTimersByTime(500);
    });
    expect(done).toHaveBeenCalledTimes(1);
    rerender(
      <svg>
        <MessageMarker from={{ x: 0, y: 0 }} to={{ x: 10, y: 0 }} durationMs={9000} />
      </svg>,
    );
    expect(screen.getByTestId('message-marker')).toHaveAttribute('data-duration-ms', '2000');
  });
});

describe('TriggerButton', () => {
  it('is disabled without an Active session or with a service not ready', () => {
    const a = { triggerQuoteAccepted: vi.fn() };
    const { unmount } = renderWith(<TriggerButton api={a} />);
    expect(screen.getByRole('button', { name: 'Quote Accepted' })).toBeDisabled();
    unmount();
    const s = session();
    s.services[1] = { ...s.services[1], ready: false };
    renderWith(<TriggerButton api={a} />, { ...initialState, session: s });
    expect(screen.getByRole('button', { name: 'Quote Accepted' })).toBeDisabled();
  });

  it('stays enabled while triggers are in flight and accepts every click', async () => {
    const a = { triggerQuoteAccepted: vi.fn(() => new Promise<never>(() => {})) };
    renderWith(<TriggerButton api={a} />, { ...initialState, session: session() });
    const button = screen.getByRole('button', { name: 'Quote Accepted' });
    await userEvent.click(button);
    await userEvent.click(button);
    expect(a.triggerQuoteAccepted).toHaveBeenCalledTimes(2);
    expect(button).toBeEnabled();
  });

  it('shows an error and stays enabled when the trigger fails', async () => {
    const a = { triggerQuoteAccepted: vi.fn().mockRejectedValue(new Error('boom')) };
    renderWith(<TriggerButton api={a} />, { ...initialState, session: session() });
    await userEvent.click(screen.getByRole('button', { name: 'Quote Accepted' }));
    expect(await screen.findByRole('alert')).toHaveTextContent('boom');
    expect(screen.getByRole('button', { name: 'Quote Accepted' })).toBeEnabled();
  });
});

describe('FailureToggle', () => {
  it('offers exactly Off, Fail once and Always fail with Off selected', () => {
    renderWith(<FailureToggle consumer="Customer" api={{ setFailureMode: vi.fn() }} />);
    const radios = screen.getAllByRole('radio');
    expect(radios.map((r) => r.parentElement?.textContent)).toEqual([
      'Off',
      'Fail once',
      'Always fail',
    ]);
    expect(screen.getByLabelText('Off')).toBeChecked();
  });

  it('saves the chosen mode', async () => {
    const setFailureMode = vi.fn().mockResolvedValue(undefined);
    renderWith(<FailureToggle consumer="Customer" api={{ setFailureMode }} />);
    fireEvent.click(screen.getByLabelText('Fail once'));
    await waitFor(() => expect(screen.getByLabelText('Fail once')).toBeChecked());
    expect(setFailureMode).toHaveBeenCalledWith('Customer', 'FailOnce');
  });

  it('reverts and shows an error when saving fails', async () => {
    const setFailureMode = vi.fn().mockRejectedValue(new Error('x'));
    renderWith(<FailureToggle consumer="Communications" api={{ setFailureMode }} />);
    fireEvent.click(screen.getByLabelText('Always fail'));
    expect(await screen.findByRole('alert')).toHaveTextContent('not saved');
    expect(screen.getByLabelText('Off')).toBeChecked();
  });
});
