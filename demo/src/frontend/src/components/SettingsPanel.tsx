import { useRef, useState } from 'react';
import type { ApiClient } from '../api/client';
import type { SettingEntryDto, SettingErrorDto } from '../api/types';
import { useStore } from '../state/StoreContext';

/** Defaults display. Secrets are read-only (3.10); edits are validated by the BFF (3.8). */
export function SettingsPanel({ client }: { client: ApiClient }) {
  const { state, dispatch } = useStore();
  const [drafts, setDrafts] = useState<Record<string, string>>({});
  const [errors, setErrors] = useState<Record<string, SettingErrorDto[]>>({});
  const requestIds = useRef<Record<string, number>>({});
  const { defaults } = state;

  if (defaults.status === 'idle') return null;
  if (defaults.status === 'loading') return <p role="status">Loading settings…</p>;
  if (defaults.status === 'error') {
    // No partial name and value pairs in the error state (3.9).
    return <p role="alert">Settings are unavailable: {defaults.message}</p>;
  }

  const providerId = defaults.providerId;
  const lastValid = (e: SettingEntryDto) => state.overrides[e.name] ?? e.value;
  const shown = (e: SettingEntryDto) => drafts[e.name] ?? lastValid(e);

  async function commit(entry: SettingEntryDto) {
    if (entry.isSecret || !entry.editable) return;
    const value = shown(entry);
    if (value === lastValid(entry)) return;
    const id = (requestIds.current[entry.name] ?? 0) + 1;
    requestIds.current[entry.name] = id;
    const revert = (errs: SettingErrorDto[]) => {
      setErrors((c) => ({ ...c, [entry.name]: errs }));
      setDrafts((c) => {
        const rest = { ...c };
        delete rest[entry.name];
        return rest;
      });
    };
    try {
      const result = await client.validateSettings(providerId, {
        ...state.overrides,
        [entry.name]: value,
      });
      if (requestIds.current[entry.name] !== id) return;
      if (result.valid) {
        dispatch({ type: 'overrideSet', name: entry.name, value });
        setErrors((c) => ({ ...c, [entry.name]: [] }));
      } else {
        revert(result.errors);
      }
    } catch (error) {
      if (requestIds.current[entry.name] !== id) return;
      revert([
        {
          field: entry.name,
          message: error instanceof Error ? error.message : 'Validation failed.',
          permittedValuesOrRange: null,
        },
      ]);
    }
  }

  return (
    <section aria-label="Settings">
      <h2>Settings</h2>
      <p>Messaging mode: {defaults.data.messagingMode}</p>
      <dl>
        {defaults.data.settings.map((entry) => (
          <div key={`${entry.group}.${entry.name}`}>
            <dt>
              <label htmlFor={`setting-${entry.name}`}>
                {entry.group ? `${entry.group}.` : ''}
                {entry.name}
              </label>
            </dt>
            <dd>
              <input
                id={`setting-${entry.name}`}
                type="text"
                value={entry.isSecret ? entry.value : shown(entry)}
                readOnly={entry.isSecret || !entry.editable}
                onChange={(e) => setDrafts((c) => ({ ...c, [entry.name]: e.target.value }))}
                onBlur={() => void commit(entry)}
                onKeyDown={(e) => {
                  if (e.key === 'Enter') void commit(entry);
                }}
              />
              {(errors[entry.name] ?? []).map((err, i) => (
                <span key={i} role="alert">
                  {' '}
                  {err.message}
                  {err.permittedValuesOrRange ? ` (${err.permittedValuesOrRange})` : ''}
                </span>
              ))}
            </dd>
          </div>
        ))}
      </dl>
    </section>
  );
}
