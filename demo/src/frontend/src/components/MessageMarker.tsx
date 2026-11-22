import { useEffect, useState } from 'react';

export const MIN_MARKER_MS = 500;
export const MAX_MARKER_MS = 2000;
export const DEFAULT_MARKER_MS = 1000;

export interface Point {
  x: number;
  y: number;
}

export interface MessageMarkerProps {
  from: Point;
  to: Point;
  /** Clamped to 500..2000 ms (9.1, 9.2). */
  durationMs?: number;
  onDone?: () => void;
}

export function clampDuration(ms: number): number {
  return Math.min(MAX_MARKER_MS, Math.max(MIN_MARKER_MS, ms));
}

/**
 * SVG circle that travels from `from` to `to`. The move starts in the first
 * effect after mount, well inside the 500 ms start limit.
 */
export function MessageMarker({
  from,
  to,
  durationMs = DEFAULT_MARKER_MS,
  onDone,
}: MessageMarkerProps) {
  const duration = clampDuration(durationMs);
  const [moving, setMoving] = useState(false);

  useEffect(() => {
    setMoving(true);
    const timer = setTimeout(() => onDone?.(), duration);
    return () => clearTimeout(timer);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [duration]);

  const dx = moving ? to.x - from.x : 0;
  const dy = moving ? to.y - from.y : 0;
  return (
    <circle
      data-testid="message-marker"
      data-duration-ms={duration}
      data-moving={moving}
      cx={from.x}
      cy={from.y}
      r={8}
      fill="#f59e0b"
      style={{
        transform: `translate(${dx}px, ${dy}px)`,
        transition: `transform ${duration}ms linear`,
      }}
    />
  );
}
