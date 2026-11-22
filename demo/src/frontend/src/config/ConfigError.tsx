export function ConfigError({ reason }: { reason: string }) {
  return (
    <main role="alert">
      <h1>Configuration error</h1>
      <p>{reason}</p>
      <p>
        Set <code>VITE_BFF_BASE_URL</code> to the absolute URL of the backend, for example{' '}
        <code>http://localhost:5080</code>, then restart the Frontend. No requests were sent.
      </p>
    </main>
  );
}
