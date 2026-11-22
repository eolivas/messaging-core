/** Parsed RFC 7807 Problem Details as sent by the BFF. */
export interface ProblemDetails {
  status: number;
  title: string;
  detail: string | null;
  /** Field name to messages, present on 400 responses. */
  errors: Record<string, string[]>;
  correlationId: string | null;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function parseErrors(value: unknown): Record<string, string[]> {
  const result: Record<string, string[]> = {};
  if (!isRecord(value)) {
    return result;
  }
  for (const [field, messages] of Object.entries(value)) {
    if (Array.isArray(messages)) {
      result[field] = messages.filter((m): m is string => typeof m === 'string');
    } else if (typeof messages === 'string') {
      result[field] = [messages];
    }
  }
  return result;
}

/**
 * Builds a ProblemDetails from a response body. Never throws: a body that is not
 * a Problem Details object yields a generic problem for the HTTP status.
 */
export function parseProblemDetails(
  status: number,
  body: unknown,
  headerCorrelationId: string | null = null,
): ProblemDetails {
  if (!isRecord(body)) {
    return {
      status,
      title: `Request failed (HTTP ${status})`,
      detail: null,
      errors: {},
      correlationId: headerCorrelationId,
    };
  }
  const bodyCorrelation = typeof body.correlationId === 'string' ? body.correlationId : null;
  return {
    status: typeof body.status === 'number' ? body.status : status,
    title: typeof body.title === 'string' && body.title !== '' ? body.title : `Request failed (HTTP ${status})`,
    detail: typeof body.detail === 'string' ? body.detail : null,
    errors: parseErrors(body.errors),
    correlationId: bodyCorrelation ?? headerCorrelationId,
  };
}
