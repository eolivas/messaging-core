import { useState } from 'react';
import { isCompleted } from '../state/reducer';
import { useStore } from '../state/StoreContext';

export interface ActivityFeedProps {
  /** Clears the server-side flow log (DELETE session/flow-steps). The feed is cleared only on success. */
  onClear?: () => Promise<void>;
}

/** Formats an ISO instant as UTC `YYYY-MM-DD HH:mm:ss.SSS`. */
export function formatUtc(iso: string): string {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return d.toISOString().replace('T', ' ').replace('Z', '');
}

export function ActivityFeed({ onClear }: ActivityFeedProps) {
  const { state, dispatch } = useStore();
  const [clearError, setClearError] = useState<string | null>(null);

  async function clear() {
    setClearError(null);
    try {
      await onClear?.();
      dispatch({ type: 'feedCleared' });
    } catch {
      setClearError('Could not clear the activity feed.');
    }
  }

  return (
    <section aria-label="Activity feed">
      <h2>Activity</h2>
      <button type="button" onClick={() => void clear()}>
        Clear
      </button>
      {clearError && <p role="alert">{clearError}</p>}
      <ul>
        {state.feed.map((step) => {
          const highlighted =
            state.highlightedCorrelationId !== null &&
            step.correlationId === state.highlightedCorrelationId;
          const completed = isCompleted(state, step);
          return (
            <li
              key={step.sequence}
              data-testid="activity-entry"
              data-highlighted={highlighted ? 'true' : 'false'}
              data-completed={completed ? 'true' : 'false'}
              style={highlighted ? { background: '#fff3bf' } : undefined}
            >
              <time dateTime={step.occurredAtUtc}>{formatUtc(step.occurredAtUtc)} UTC</time>{' '}
              <span data-field="service">{step.service}</span>{' '}
              <span data-field="kind">{step.kind}</span>{' '}
              {step.correlationId ? (
                <button
                  type="button"
                  data-field="correlation"
                  aria-pressed={highlighted}
                  onClick={() =>
                    dispatch({
                      type: 'correlationSelected',
                      correlationId: highlighted ? null : step.correlationId,
                    })
                  }
                >
                  {step.correlationId}
                </button>
              ) : (
                <span data-field="correlation" />
              )}{' '}
              <span data-field="lead">{step.leadId ?? ''}</span>
              {completed && <span aria-label="Completed"> ✔ completed</span>}
            </li>
          );
        })}
      </ul>
    </section>
  );
}
