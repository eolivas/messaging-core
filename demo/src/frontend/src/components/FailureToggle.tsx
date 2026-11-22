import { useState } from 'react';
import type { ApiClient } from '../api/client';
import type { ConsumerId, FailureMode } from '../api/types';
import { useStore } from '../state/StoreContext';

export interface FailureToggleProps {
  consumer: ConsumerId;
  label?: string;
  api: Pick<ApiClient, 'setFailureMode'>;
}

const OPTIONS: { mode: FailureMode; label: string }[] = [
  { mode: 'Off', label: 'Off' },
  { mode: 'FailOnce', label: 'Fail once' },
  { mode: 'AlwaysFail', label: 'Always fail' },
];

export function FailureToggle({ consumer, label, api }: FailureToggleProps) {
  const { state, dispatch } = useStore();
  const [error, setError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const saved = state.failureModes[consumer];
  const name = label ?? consumer;

  // The store only changes after a successful save, so a failed save leaves
  // the previously saved option selected (10.8).
  const choose = async (mode: FailureMode) => {
    if (mode === saved || saving) return;
    setError(null);
    setSaving(true);
    try {
      await api.setFailureMode(consumer, mode);
      dispatch({ type: 'failureModeSet', consumer, mode });
    } catch {
      setError(`The ${name} failure setting was not saved.`);
    } finally {
      setSaving(false);
    }
  };

  return (
    <fieldset disabled={saving}>
      <legend>{name} failure simulation</legend>
      {OPTIONS.map((o) => (
        <label key={o.mode}>
          <input
            type="radio"
            name={`failure-${consumer}`}
            value={o.mode}
            checked={saved === o.mode}
            onChange={() => void choose(o.mode)}
          />
          {o.label}
        </label>
      ))}
      {error && <p role="alert">{error}</p>}
    </fieldset>
  );
}
