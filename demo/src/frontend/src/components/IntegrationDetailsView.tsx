import { useEffect, useState } from 'react';
import type { ApiClient } from '../api/client';
import type { IntegrationDetailsDto } from '../api/types';

export interface IntegrationDetailsViewProps {
  api: Pick<ApiClient, 'getIntegrationDetails'>;
  /** Change to reload, for example the active provider id. */
  refreshKey?: string | null;
}

type Load =
  | { status: 'loading' }
  | { status: 'loaded'; data: IntegrationDetailsDto }
  | { status: 'error' };

export function IntegrationDetailsView({ api, refreshKey }: IntegrationDetailsViewProps) {
  const [load, setLoad] = useState<Load>({ status: 'loading' });
  const [attempt, setAttempt] = useState(0);

  useEffect(() => {
    const controller = new AbortController();
    setLoad({ status: 'loading' }); // never show values of a previous load
    api
      .getIntegrationDetails({ signal: controller.signal })
      .then((data) => {
        if (!controller.signal.aborted) setLoad({ status: 'loaded', data });
      })
      .catch(() => {
        if (!controller.signal.aborted) setLoad({ status: 'error' });
      });
    return () => controller.abort();
  }, [api, refreshKey, attempt]);

  return (
    <section aria-label="Integration details">
      <h2>Integration details</h2>
      {load.status === 'loading' && <p>Loading…</p>}
      {load.status === 'error' && (
        <div role="alert">
          <p>Integration details could not be loaded.</p>
          <button type="button" onClick={() => setAttempt((n) => n + 1)}>
            Retry
          </button>
        </div>
      )}
      {load.status === 'loaded' && (
        <dl>
          <dt>Messaging mode</dt>
          <dd>{load.data.messagingMode}</dd>
          <dt>Provider</dt>
          <dd>{load.data.providerName}</dd>
          <dt>Idempotency</dt>
          <dd>{load.data.idempotencyEnabled ? 'Enabled' : 'Disabled'}</dd>
          <dt>Retry max attempts</dt>
          <dd>{load.data.retryMaxAttempts}</dd>
          <dt>Backoff strategy</dt>
          <dd>{load.data.backoffStrategy}</dd>
        </dl>
      )}
    </section>
  );
}
