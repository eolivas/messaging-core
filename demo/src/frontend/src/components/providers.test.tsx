import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';
import type { ApiClient } from '../api/client';
import type {
  ProviderDefaultsDto,
  ProviderDto,
  SessionStatusDto,
} from '../api/types';
import { initialState, type AppState } from '../state/reducer';
import { StoreProvider } from '../state/StoreContext';
import { ProviderSelector } from './ProviderSelector';
import { SettingsPanel } from './SettingsPanel';
import { StartControl } from './StartControl';

const providers: ProviderDto[] = [
  { id: 'rabbit', displayName: 'RabbitMQ', supported: true, unsupportedReason: null },
  { id: 'kafka', displayName: 'Kafka', supported: true, unsupportedReason: null },
  {
    id: 'azure',
    displayName: 'Azure Service Bus',
    supported: false,
    unsupportedReason: 'No library implementation.',
  },
];

const defaults = (providerId: string): ProviderDefaultsDto => ({
  providerId,
  messagingMode: 'PublishSubscribe',
  settings: [
    { group: 'G', name: 'Host', value: 'localhost', isSecret: false, editable: true, permittedValuesOrRange: null },
    { group: 'G', name: 'Password', value: '••••••', isSecret: true, editable: false, permittedValuesOrRange: null },
  ],
});

const session = (over: Partial<SessionStatusDto>): SessionStatusDto => ({
  state: 'Idle',
  providerId: null,
  providerName: null,
  health: 'Unknown',
  services: [],
  lastError: null,
  ...over,
});

function mockClient(over: Partial<ApiClient> = {}): ApiClient {
  return { ...({} as ApiClient), ...over };
}

function renderWith(ui: React.ReactNode, initial: Partial<AppState> = {}) {
  return render(<StoreProvider initial={{ ...initialState, providers, ...initial }}>{ui}</StoreProvider>);
}

describe('ProviderSelector', () => {
  it('shows the unsupported reason and ignores selection', async () => {
    const getProviderDefaults = vi.fn();
    renderWith(<ProviderSelector client={mockClient({ getProviderDefaults })} />);
    expect(screen.getByText(/No library implementation/)).toBeVisible();
    const azure = screen.getByLabelText('Azure Service Bus');
    expect(azure).toBeDisabled();
    await userEvent.click(azure);
    expect(getProviderDefaults).not.toHaveBeenCalled();
    expect(azure).not.toBeChecked();
  });

  it('discards a stale defaults response', async () => {
    let resolveFirst!: (d: ProviderDefaultsDto) => void;
    const getProviderDefaults = vi.fn((id: string) =>
      id === 'rabbit'
        ? new Promise<ProviderDefaultsDto>((r) => (resolveFirst = r))
        : Promise.resolve(defaults('kafka')),
    );
    renderWith(
      <>
        <ProviderSelector client={mockClient({ getProviderDefaults })} />
        <SettingsPanel client={mockClient()} />
      </>,
    );
    await userEvent.click(screen.getByLabelText('RabbitMQ'));
    await userEvent.click(screen.getByLabelText('Kafka'));
    await screen.findByText(/Messaging mode/);
    resolveFirst({ ...defaults('rabbit'), messagingMode: 'Stale' });
    await new Promise((r) => setTimeout(r, 0));
    expect(screen.queryByText(/Stale/)).toBeNull();
    expect(screen.getByLabelText('Kafka')).toBeChecked();
  });

  it('is disabled while a session is starting', () => {
    renderWith(<ProviderSelector client={mockClient()} />, {
      session: session({ state: 'Starting' }),
    });
    expect(screen.getByLabelText('RabbitMQ')).toBeDisabled();
  });
});

describe('SettingsPanel', () => {
  const loaded: Partial<AppState> = {
    defaults: { status: 'loaded', providerId: 'rabbit', data: defaults('rabbit') },
  };

  it('keeps secrets read-only with the placeholder', () => {
    renderWith(<SettingsPanel client={mockClient()} />, loaded);
    const secret = screen.getByLabelText(/Password/);
    expect(secret).toHaveValue('••••••');
    expect(secret).toHaveAttribute('readonly');
  });

  it('keeps the last valid value when validation fails', async () => {
    const validateSettings = vi.fn().mockResolvedValue({
      valid: false,
      errors: [{ field: 'Host', message: 'Bad host', permittedValuesOrRange: 'a-z' }],
    });
    renderWith(<SettingsPanel client={mockClient({ validateSettings })} />, loaded);
    const input = screen.getByLabelText(/Host/);
    await userEvent.clear(input);
    await userEvent.type(input, 'x!');
    await userEvent.tab();
    expect(await screen.findByText(/Bad host/)).toBeVisible();
    expect(input).toHaveValue('localhost');
    expect(validateSettings).toHaveBeenCalledWith('rabbit', { Host: 'x!' });
  });

  it('accepts a valid edit', async () => {
    const validateSettings = vi.fn().mockResolvedValue({ valid: true, errors: [] });
    renderWith(<SettingsPanel client={mockClient({ validateSettings })} />, loaded);
    const input = screen.getByLabelText(/Host/);
    await userEvent.clear(input);
    await userEvent.type(input, 'broker');
    await userEvent.tab();
    await waitFor(() => expect(validateSettings).toHaveBeenCalled());
    expect(input).toHaveValue('broker');
  });

  it('shows no pairs in the error state', () => {
    renderWith(<SettingsPanel client={mockClient()} />, {
      defaults: { status: 'error', providerId: 'rabbit', message: 'down' },
    });
    expect(screen.getByRole('alert')).toHaveTextContent('down');
    expect(screen.queryByText(/Host/)).toBeNull();
  });
});

describe('StartControl', () => {
  const loaded: Partial<AppState> = {
    defaults: { status: 'loaded', providerId: 'rabbit', data: defaults('rabbit') },
  };

  it('disables Start while starting', () => {
    renderWith(<StartControl client={mockClient()} />, {
      ...loaded,
      session: session({ state: 'Starting' }),
    });
    expect(screen.getByRole('button', { name: 'Start' })).toBeDisabled();
    expect(screen.getByRole('status')).toHaveTextContent(/Starting/);
  });

  it('shows an error and retries', async () => {
    const startSession = vi
      .fn()
      .mockRejectedValueOnce(new Error('Broker rabbit unreachable'))
      .mockResolvedValueOnce(session({ state: 'Starting', providerId: 'rabbit' }));
    renderWith(<StartControl client={mockClient({ startSession })} />, loaded);
    await userEvent.click(screen.getByRole('button', { name: 'Start' }));
    expect(await screen.findByRole('alert')).toHaveTextContent('Broker rabbit unreachable');
    await userEvent.click(screen.getByRole('button', { name: 'Retry' }));
    await waitFor(() => expect(startSession).toHaveBeenCalledTimes(2));
    expect(startSession).toHaveBeenCalledWith({ providerId: 'rabbit', overrides: null });
  });
});
