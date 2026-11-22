import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import { App } from './App';
import { readBaseUrl } from './config/parseBaseUrl';

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <App config={readBaseUrl()} />
  </StrictMode>,
);
