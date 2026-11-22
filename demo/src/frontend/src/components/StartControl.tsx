import { useState } from 'react';
import type { ApiClient } from '../api/client';
import { useStore } from '../state/StoreContext';

/** Start button, starting status and start error with retry (4.1, 4.3, 4.5, 4.6). */
export function StartControl({ client }: { client: ApiClient }) {
  const { state, dispatch } = useStore();
  const [submitting, setSubmitting] = useState(false);
  const [requestError, setRequestError] = useState<string | null>(null);

  const providers = state.providers ?? [];
  const selectedId = state.defaults.status === 'idle' ? null : state.defaults.providerId;
  const selected = providers.find((p) => p.id === selectedId);
  const sessionState = state.session?.state ?? 'Idle';
  const starting = submitting || sessionState === 'Starting';

  if (!selected || !selected.supported) return null;

  const canStart = !starting && sessionState === 'Idle' && state.defaults.status === 'loaded';
  const lastError = sessionState === 'Idle' ? (state.session?.lastError ?? null) : null;
  const errorText =
    requestError ?? (lastError ? `${lastError.title}: ${lastError.detail}` : null);

  async function start() {
    if (!canStart || !selected) return;
    setSubmitting(true);
    setRequestError(null);
    try {
      const overrides = Object.keys(state.overrides).length > 0 ? state.overrides : null;
      const session = await client.startSession({ providerId: selected.id, overrides });
      dispatch({ type: 'sessionStarted', session });
    } catch (error) {
      setRequestError(
        error instanceof Error ? error.message : 'The session could not be started.',
      );
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div>
      <button type="button" disabled={!canStart} onClick={() => void start()}>
        Start
      </button>
      {starting && <span role="status"> Starting {selected.displayName}…</span>}
      {errorText && !starting && (
        <div role="alert">
          <p>{errorText}</p>
          <button type="button" onClick={() => void start()}>
            Retry
          </button>
        </div>
      )}
    </div>
  );
}
