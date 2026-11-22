import type {
  ConsumerId,
  CountersDto,
  FailureMode,
  FlowStepDto,
  ProviderDefaultsDto,
  ProviderDto,
  SessionStatusDto,
} from '../api/types';

export const FEED_CAP = 500;

export type ConnectionStatus = 'connecting' | 'connected' | 'disconnected';

export type DefaultsState =
  | { status: 'idle' }
  | { status: 'loading'; providerId: string }
  | { status: 'loaded'; providerId: string; data: ProviderDefaultsDto }
  | { status: 'error'; providerId: string; message: string };

export interface AppState {
  providers: ProviderDto[] | null;
  defaults: DefaultsState;
  /** Monotonic id of the latest defaults request. Older responses are discarded. */
  defaultsRequestToken: number;
  /** Validated overrides of editable settings for the selected provider, by setting name. */
  overrides: Record<string, string>;
  session: SessionStatusDto | null;
  /** Newest first, capped at FEED_CAP, unique by sequence. */
  feed: FlowStepDto[];
  /** Correlation ids for which Published, CustomerCreated and EmailSent all occurred. */
  completed: Record<string, true>;
  counters: CountersDto;
  connection: ConnectionStatus;
  highlightedCorrelationId: string | null;
  failureModes: Record<ConsumerId, FailureMode>;
}

export const initialCounters: CountersDto = {
  published: 0,
  customersCreated: 0,
  emailsSent: 0,
};

export const initialState: AppState = {
  providers: null,
  defaults: { status: 'idle' },
  defaultsRequestToken: 0,
  overrides: {},
  session: null,
  feed: [],
  completed: {},
  counters: initialCounters,
  connection: 'connecting',
  highlightedCorrelationId: null,
  failureModes: { Customer: 'Off', Communications: 'Off' },
};

export type Action =
  | { type: 'providersLoaded'; providers: ProviderDto[] }
  | { type: 'defaultsRequested'; providerId: string; token: number }
  | { type: 'defaultsLoaded'; token: number; data: ProviderDefaultsDto }
  | { type: 'defaultsFailed'; token: number; message: string }
  | { type: 'overrideSet'; name: string; value: string }
  | { type: 'sessionUpdated'; session: SessionStatusDto }
  | { type: 'sessionStarted'; session: SessionStatusDto }
  | { type: 'stepReceived'; step: FlowStepDto }
  | { type: 'stepsReceived'; steps: FlowStepDto[] }
  | { type: 'countersReceived'; counters: CountersDto }
  | { type: 'feedCleared' }
  | { type: 'connectionChanged'; status: ConnectionStatus }
  | { type: 'correlationSelected'; correlationId: string | null }
  | { type: 'failureModeSet'; consumer: ConsumerId; mode: FailureMode };

const REQUIRED_FOR_COMPLETION = ['Published', 'CustomerCreated', 'EmailSent'] as const;

function compareNewestFirst(a: FlowStepDto, b: FlowStepDto): number {
  const ta = Date.parse(a.occurredAtUtc);
  const tb = Date.parse(b.occurredAtUtc);
  if (ta !== tb) return tb - ta;
  return b.sequence - a.sequence;
}

function bumpCounters(c: CountersDto, step: FlowStepDto): CountersDto {
  switch (step.kind) {
    case 'Published':
      return { ...c, published: c.published + 1 };
    case 'CustomerCreated':
      return { ...c, customersCreated: c.customersCreated + 1 };
    case 'EmailSent':
      return { ...c, emailsSent: c.emailsSent + 1 };
    default:
      return c;
  }
}

function addSteps(state: AppState, steps: FlowStepDto[]): AppState {
  const seen = new Set(state.feed.map((s) => s.sequence));
  const fresh: FlowStepDto[] = [];
  for (const step of steps) {
    if (seen.has(step.sequence)) continue;
    seen.add(step.sequence);
    fresh.push(step);
  }
  if (fresh.length === 0) return state;

  let counters = state.counters;
  const completed = { ...state.completed };
  const kindsByCorrelation = new Map<string, Set<string>>();
  const kindsFor = (id: string): Set<string> => {
    let kinds = kindsByCorrelation.get(id);
    if (!kinds) {
      kinds = new Set(
        state.feed.filter((s) => s.correlationId === id).map((s) => s.kind),
      );
      kindsByCorrelation.set(id, kinds);
    }
    return kinds;
  };

  for (const step of fresh) {
    counters = bumpCounters(counters, step);
    // A missing correlation id is kept in the feed but cannot be completed (9.12).
    if (step.correlationId) {
      const kinds = kindsFor(step.correlationId);
      kinds.add(step.kind);
      if (REQUIRED_FOR_COMPLETION.every((k) => kinds.has(k))) {
        completed[step.correlationId] = true;
      }
    }
  }

  const feed = [...fresh, ...state.feed].sort(compareNewestFirst).slice(0, FEED_CAP);
  return { ...state, feed, counters, completed };
}

export function reducer(state: AppState, action: Action): AppState {
  switch (action.type) {
    case 'providersLoaded':
      return { ...state, providers: action.providers };
    case 'defaultsRequested':
      if (action.token <= state.defaultsRequestToken) return state;
      return {
        ...state,
        defaultsRequestToken: action.token,
        overrides: {},
        defaults: { status: 'loading', providerId: action.providerId },
      };
    case 'defaultsLoaded':
      if (action.token !== state.defaultsRequestToken) return state; // stale (2.8)
      return {
        ...state,
        defaults: {
          status: 'loaded',
          providerId: action.data.providerId,
          data: action.data,
        },
      };
    case 'defaultsFailed': {
      if (action.token !== state.defaultsRequestToken) return state;
      const providerId =
        state.defaults.status === 'idle' ? '' : state.defaults.providerId;
      return {
        ...state,
        defaults: { status: 'error', providerId, message: action.message },
      };
    }
    case 'overrideSet':
      return { ...state, overrides: { ...state.overrides, [action.name]: action.value } };
    case 'sessionUpdated':
      return { ...state, session: action.session };
    case 'sessionStarted':
      // A new Session starts with zero counters (5.10).
      return { ...state, session: action.session, counters: initialCounters };
    case 'stepReceived':
      return addSteps(state, [action.step]);
    case 'stepsReceived':
      return addSteps(state, action.steps);
    case 'countersReceived':
      return { ...state, counters: action.counters };
    case 'feedCleared':
      // Keeps session, providers, defaults and failure modes (9.9).
      return {
        ...state,
        feed: [],
        completed: {},
        counters: initialCounters,
        highlightedCorrelationId: null,
      };
    case 'connectionChanged':
      return { ...state, connection: action.status };
    case 'correlationSelected':
      return { ...state, highlightedCorrelationId: action.correlationId };
    case 'failureModeSet':
      return {
        ...state,
        failureModes: { ...state.failureModes, [action.consumer]: action.mode },
      };
  }
}

/** Entries carrying the selected correlation id (9.7). */
export function highlightedEntries(state: AppState): FlowStepDto[] {
  const id = state.highlightedCorrelationId;
  return id ? state.feed.filter((s) => s.correlationId === id) : [];
}

export function isCompleted(state: AppState, step: FlowStepDto): boolean {
  return step.correlationId !== null && state.completed[step.correlationId] === true;
}
