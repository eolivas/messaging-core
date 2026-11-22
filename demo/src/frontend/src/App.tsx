import { useEffect, useMemo, useState } from 'react';
import { createApiClient, type ApiClient } from './api/client';
import { ActivityFeed } from './components/ActivityFeed';
import { FailureToggle } from './components/FailureToggle';
import { FlowDiagram } from './components/FlowDiagram';
import { IntegrationDetailsView } from './components/IntegrationDetailsView';
import { ProviderSelector } from './components/ProviderSelector';
import { SessionBar } from './components/SessionBar';
import { SettingsPanel } from './components/SettingsPanel';
import { StartControl } from './components/StartControl';
import { TriggerButton } from './components/TriggerButton';
import { ConfigError } from './config/ConfigError';
import type { BaseUrlResult } from './config/parseBaseUrl';
import { useEventStream } from './hooks/useEventStream';
import { StoreProvider, useStore } from './state/StoreContext';

export interface AppProps {
  config: BaseUrlResult;
  /** Overridable for tests. */
  createClient?: (options: { baseUrl: string }) => ApiClient;
}

/**
 * An invalid base URL shows the ConfigError screen. The API client is created only
 * for a valid URL, so no request is sent otherwise (13.10).
 */
export function App({ config, createClient = createApiClient }: AppProps) {
  const baseUrl = config.ok ? config.baseUrl : null;
  const client = useMemo(
    () => (baseUrl === null ? null : createClient({ baseUrl })),
    [baseUrl, createClient],
  );
  if (!config.ok || client === null) {
    return <ConfigError reason={config.ok ? 'Configuration is invalid.' : config.reason} />;
  }
  return (
    <StoreProvider>
      <Main client={client} />
    </StoreProvider>
  );
}

function Main({ client }: { client: ApiClient }) {
  const { state, dispatch } = useStore();
  const [stopping, setStopping] = useState(false);
  const [stopError, setStopError] = useState<string | null>(null);
  const [providersError, setProvidersError] = useState<string | null>(null);

  useEventStream(client.eventsUrl());

  useEffect(() => {
    const controller = new AbortController();
    client
      .listProviders({ signal: controller.signal })
      .then((providers) => dispatch({ type: 'providersLoaded', providers }))
      .catch((error: unknown) => {
        if (controller.signal.aborted) return;
        setProvidersError(
          error instanceof Error ? error.message : 'The providers could not be loaded.',
        );
      });
    return () => controller.abort();
  }, [client, dispatch]);

  async function changeProvider() {
    setStopping(true);
    setStopError(null);
    try {
      const session = await client.stopSession();
      dispatch({ type: 'sessionUpdated', session });
    } catch (error) {
      setStopError(error instanceof Error ? error.message : 'The session could not be stopped.');
    } finally {
      setStopping(false);
    }
  }

  const sessionState = state.session?.state ?? 'Idle';
  const active = sessionState === 'Active';
  const showSession = sessionState === 'Active' || sessionState === 'Stopping';

  return (
    <main>
      <h1>Messaging Demo</h1>
      <SessionBar onChangeProvider={() => void changeProvider()} disabled={stopping} />
      {stopError && <p role="alert">{stopError}</p>}
      {providersError && <p role="alert">Providers are unavailable: {providersError}</p>}
      {!showSession && (
        <>
          <ProviderSelector client={client} />
          <SettingsPanel client={client} />
          <StartControl client={client} />
        </>
      )}
      {active && (
        <>
          <TriggerButton api={client} />
          <FlowDiagram />
        </>
      )}
      {showSession && (
        <>
          <FailureToggle consumer="Customer" label="Customer" api={client} />
          <FailureToggle consumer="Communications" label="Communications" api={client} />
          <IntegrationDetailsView api={client} refreshKey={state.session?.providerId} />
          <ActivityFeed onClear={() => client.clearFlowSteps()} />
        </>
      )}
    </main>
  );
}
