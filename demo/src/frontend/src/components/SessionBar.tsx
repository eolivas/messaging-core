import { useStore } from '../state/StoreContext';

export interface SessionBarProps {
  /** Stops the Session and returns to the provider selector. */
  onChangeProvider: () => void;
  disabled?: boolean;
}

export function SessionBar({ onChangeProvider, disabled = false }: SessionBarProps) {
  const { state } = useStore();
  const session = state.session;
  if (!session || session.state === 'Idle') return null;

  return (
    <div role="region" aria-label="Session">
      <span data-testid="provider-name">{session.providerName ?? ''}</span>{' '}
      <span data-testid="provider-health">{session.health}</span>
      {state.connection === 'disconnected' && (
        <span role="status" data-testid="disconnected-indicator">
          {' '}
          Disconnected
        </span>
      )}{' '}
      <button type="button" disabled={disabled || session.state === 'Stopping'} onClick={onChangeProvider}>
        Change provider
      </button>
    </div>
  );
}
