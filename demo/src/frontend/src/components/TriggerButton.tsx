import { useState } from 'react';
import type { ApiClient } from '../api/client';
import { useStore } from '../state/StoreContext';

export interface TriggerButtonProps {
  api: Pick<ApiClient, 'triggerQuoteAccepted'>;
}

const REQUIRED_READY_SERVICES = 3;

export function TriggerButton({ api }: TriggerButtonProps) {
  const { state } = useStore();
  const [error, setError] = useState<string | null>(null);
  const session = state.session;
  const enabled =
    session?.state === 'Active' &&
    session.services.filter((s) => s.ready).length === REQUIRED_READY_SERVICES;

  // Not disabled while requests or events are in flight (6.10).
  const onClick = async () => {
    setError(null);
    try {
      await api.triggerQuoteAccepted();
    } catch (e) {
      setError(
        e instanceof Error ? `The trigger failed: ${e.message}` : 'The trigger failed.',
      );
    }
  };

  return (
    <div>
      <button type="button" disabled={!enabled} onClick={() => void onClick()}>
        Quote Accepted
      </button>
      {error && <p role="alert">{error}</p>}
    </div>
  );
}
