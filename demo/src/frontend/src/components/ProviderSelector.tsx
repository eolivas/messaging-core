import { useRef } from 'react';
import { ApiError, type ApiClient } from '../api/client';
import { useStore } from '../state/StoreContext';

/** Provider choices. Unsupported providers are disabled and show their reason (2.3, 2.4). */
export function ProviderSelector({ client }: { client: ApiClient }) {
  const { state, dispatch } = useStore();
  const tokenRef = useRef(state.defaultsRequestToken);
  const abortRef = useRef<AbortController | null>(null);

  const sessionState = state.session?.state ?? 'Idle';
  const locked = sessionState !== 'Idle';
  const selectedId = state.defaults.status === 'idle' ? null : state.defaults.providerId;

  function select(providerId: string, supported: boolean) {
    if (!supported || locked) return; // 2.4: no request, no state change
    abortRef.current?.abort();
    const controller = new AbortController();
    abortRef.current = controller;
    const token = ++tokenRef.current;
    dispatch({ type: 'defaultsRequested', providerId, token });
    client
      .getProviderDefaults(providerId, { signal: controller.signal })
      .then((data) => dispatch({ type: 'defaultsLoaded', token, data }))
      .catch((error: unknown) => {
        if (error instanceof ApiError && error.kind === 'aborted') return;
        const message =
          error instanceof Error ? error.message : 'The defaults could not be loaded.';
        dispatch({ type: 'defaultsFailed', token, message });
      });
  }

  if (!state.providers) return <p>Loading providers…</p>;

  return (
    <fieldset aria-label="Provider" disabled={locked}>
      <legend>Provider</legend>
      <ul>
        {state.providers.map((p) => {
          const reasonId = `provider-reason-${p.id}`;
          return (
            <li key={p.id}>
              <label>
                <input
                  type="radio"
                  name="provider"
                  value={p.id}
                  checked={selectedId === p.id}
                  disabled={!p.supported}
                  aria-describedby={p.supported ? undefined : reasonId}
                  onChange={() => select(p.id, p.supported)}
                />{' '}
                {p.displayName}
              </label>
              {!p.supported && (
                <span id={reasonId} className="provider-reason">
                  {' '}
                  Unavailable: {p.unsupportedReason}
                </span>
              )}
            </li>
          );
        })}
      </ul>
    </fieldset>
  );
}
