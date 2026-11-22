export type BaseUrlResult =
  | { readonly ok: true; readonly baseUrl: string }
  | { readonly ok: false; readonly reason: string };

/**
 * Validates the configured BFF base URL (VITE_BFF_BASE_URL).
 * Accepts only absolute http or https URLs with a host. A trailing slash is removed
 * so paths can be appended as "/api/v1/...".
 */
export function parseBaseUrl(raw: string | undefined | null): BaseUrlResult {
  if (raw === undefined || raw === null || raw.trim() === '') {
    return { ok: false, reason: 'VITE_BFF_BASE_URL is missing.' };
  }
  const value = raw.trim();
  let url: URL;
  try {
    url = new URL(value);
  } catch {
    return { ok: false, reason: 'VITE_BFF_BASE_URL must be an absolute URL.' };
  }
  if ((url.protocol !== 'http:' && url.protocol !== 'https:') || url.hostname === '') {
    return { ok: false, reason: 'VITE_BFF_BASE_URL must be an absolute http or https URL.' };
  }
  if (url.search !== '' || url.hash !== '') {
    return { ok: false, reason: 'VITE_BFF_BASE_URL must not contain a query or fragment.' };
  }
  return { ok: true, baseUrl: value.replace(/\/+$/, '') };
}

export function readBaseUrl(): BaseUrlResult {
  return parseBaseUrl(import.meta.env.VITE_BFF_BASE_URL);
}
