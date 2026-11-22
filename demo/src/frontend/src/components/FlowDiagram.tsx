import { useEffect, useRef, useState } from 'react';
import type { FlowStepDto, MicroserviceId } from '../api/types';
import { useStore } from '../state/StoreContext';
import { MessageMarker, type Point } from './MessageMarker';

export const HIGHLIGHT_MS = 2000;
export const MARKER_MS = 1000;

const BLOCKS: Record<MicroserviceId, { center: Point; name: string; role: string }> = {
  SalesLeads: { center: { x: 90, y: 150 }, name: 'Sales Leads MS', role: 'Publisher' },
  Customer: { center: { x: 510, y: 60 }, name: 'Customer MS', role: 'Consumer' },
  Communications: { center: { x: 510, y: 240 }, name: 'Communications MS', role: 'Consumer' },
};
const BROKER: Point = { x: 300, y: 150 };
const W = 140;
const H = 80;

interface Marker {
  id: number;
  from: Point;
  to: Point;
}

type IndicatorKey = 'Customer' | 'Communications';
type Indicators = Partial<Record<IndicatorKey, { leadId: string | null }>>;

function Block({
  id,
  count,
  highlighted,
}: {
  id: MicroserviceId;
  count: number;
  highlighted: boolean;
}) {
  const b = BLOCKS[id];
  return (
    <g data-testid={`block-${id}`} data-highlighted={highlighted}>
      <rect
        x={b.center.x - W / 2}
        y={b.center.y - H / 2}
        width={W}
        height={H}
        rx={8}
        fill={highlighted ? '#bbf7d0' : '#e5e7eb'}
        stroke="#374151"
      />
      <text x={b.center.x} y={b.center.y - 14} textAnchor="middle" fontWeight="bold">
        {b.name}
      </text>
      <text x={b.center.x} y={b.center.y + 4} textAnchor="middle" fontSize="12">
        {b.role}
      </text>
      <text
        x={b.center.x}
        y={b.center.y + 26}
        textAnchor="middle"
        data-testid={`count-${id}`}
      >
        {count}
      </text>
    </g>
  );
}

export function FlowDiagram() {
  const { state } = useStore();
  const [markers, setMarkers] = useState<Marker[]>([]);
  const [indicators, setIndicators] = useState<Indicators>({});
  const lastSequence = useRef<number | null>(null);
  const nextId = useRef(0);
  const timers = useRef<Record<string, ReturnType<typeof setTimeout>>>({});

  useEffect(() => {
    const timerMap = timers.current;
    return () => Object.values(timerMap).forEach(clearTimeout);
  }, []);

  useEffect(() => {
    const feed = state.feed;
    if (lastSequence.current === null) {
      // Steps already present at mount (replay) are not animated.
      lastSequence.current = feed.reduce((m, s) => Math.max(m, s.sequence), -1);
      return;
    }
    if (feed.length === 0) {
      lastSequence.current = -1;
      return;
    }
    const last = lastSequence.current;
    const fresh = feed.filter((s) => s.sequence > last).sort((a, b) => a.sequence - b.sequence);
    if (fresh.length === 0) return;
    lastSequence.current = fresh[fresh.length - 1].sequence;
    for (const step of fresh) apply(step);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [state.feed]);

  function addMarker(from: Point, to: Point) {
    const id = nextId.current++;
    setMarkers((m) => [...m, { id, from, to }]);
  }

  function highlight(service: IndicatorKey, leadId: string | null) {
    setIndicators((i) => ({ ...i, [service]: { leadId } }));
    clearTimeout(timers.current[service]);
    timers.current[service] = setTimeout(() => {
      setIndicators((i) => {
        const next = { ...i };
        delete next[service];
        return next;
      });
    }, HIGHLIGHT_MS);
  }

  function apply(step: FlowStepDto) {
    if (step.kind === 'Published') {
      addMarker(BLOCKS.SalesLeads.center, BROKER);
    } else if (step.kind === 'Delivered' && step.service !== 'SalesLeads') {
      addMarker(BROKER, BLOCKS[step.service].center);
    } else if (step.kind === 'CustomerCreated') {
      highlight('Customer', step.leadId);
    } else if (step.kind === 'EmailSent') {
      highlight('Communications', step.leadId);
    }
  }

  const providerName = state.session?.providerName ?? '';
  const { counters } = state;
  const edge = (p: Point, side: 'l' | 'r'): Point => ({
    x: p.x + (side === 'r' ? W / 2 : -W / 2),
    y: p.y,
  });
  const lines: [string, Point, Point][] = [
    ['SalesLeads-Broker', edge(BLOCKS.SalesLeads.center, 'r'), edge(BROKER, 'l')],
    ['Broker-Customer', edge(BROKER, 'r'), edge(BLOCKS.Customer.center, 'l')],
    ['Broker-Communications', edge(BROKER, 'r'), edge(BLOCKS.Communications.center, 'l')],
  ];

  return (
    <svg
      role="img"
      aria-label="Flow diagram"
      viewBox="0 0 600 300"
      width="100%"
      style={{ maxWidth: 720 }}
    >
      {lines.map(([id, a, b]) => (
        <line
          key={id}
          data-testid={`connection-${id}`}
          x1={a.x}
          y1={a.y}
          x2={b.x}
          y2={b.y}
          stroke="#374151"
          strokeWidth={2}
        />
      ))}
      <g data-testid="broker">
        <rect
          x={BROKER.x - W / 2}
          y={BROKER.y - H / 2}
          width={W}
          height={H}
          rx={8}
          fill="#dbeafe"
          stroke="#1d4ed8"
        />
        <text x={BROKER.x} y={BROKER.y - 8} textAnchor="middle" fontWeight="bold">
          Broker
        </text>
        <text x={BROKER.x} y={BROKER.y + 14} textAnchor="middle" data-testid="broker-label">
          {providerName}
        </text>
      </g>
      <Block id="SalesLeads" count={counters.published} highlighted={false} />
      <Block id="Customer" count={counters.customersCreated} highlighted={!!indicators.Customer} />
      <Block
        id="Communications"
        count={counters.emailsSent}
        highlighted={!!indicators.Communications}
      />
      {indicators.Customer && (
        <text
          data-testid="mock-customer"
          x={BLOCKS.Customer.center.x}
          y={BLOCKS.Customer.center.y - H / 2 - 6}
          textAnchor="middle"
          fontSize="12"
        >
          Mock_Customer {indicators.Customer.leadId ?? ''}
        </text>
      )}
      {indicators.Communications && (
        <text
          data-testid="mock-email"
          x={BLOCKS.Communications.center.x}
          y={BLOCKS.Communications.center.y + H / 2 + 14}
          textAnchor="middle"
          fontSize="12"
        >
          Mock_Email {indicators.Communications.leadId ?? ''}
        </text>
      )}
      {markers.map((m) => (
        <MessageMarker
          key={m.id}
          from={m.from}
          to={m.to}
          durationMs={MARKER_MS}
          onDone={() => setMarkers((all) => all.filter((x) => x.id !== m.id))}
        />
      ))}
    </svg>
  );
}
