using Demo.Contracts;

namespace Demo.Application.Flow;

/// <summary>
/// Thread-safe, bounded log of flow steps for the current Session. Assigns a monotonically
/// increasing sequence, derives counters, and tracks event completion per correlation id.
/// </summary>
public sealed class SessionFlowLog
{
    public const int Capacity = 500;

    private const int PublishedBit = 1;
    private const int CustomerCreatedBit = 2;
    private const int EmailSentBit = 4;
    private const int AllBits = PublishedBit | CustomerCreatedBit | EmailSentBit;

    private readonly object _gate = new();
    private readonly Queue<FlowStepEvent> _entries = new();
    private readonly Dictionary<string, int> _seenByCorrelation = new(StringComparer.Ordinal);
    private long _sequence;
    private long _published;
    private long _customersCreated;
    private long _emailsSent;

    /// <summary>Appends a step. Steps with missing correlation or lead ids are accepted.</summary>
    public FlowStepEvent Append(FlowStep step)
    {
        ArgumentNullException.ThrowIfNull(step);

        lock (_gate)
        {
            var bit = step.Kind switch
            {
                FlowStepKind.Published => PublishedBit,
                FlowStepKind.CustomerCreated => CustomerCreatedBit,
                FlowStepKind.EmailSent => EmailSentBit,
                _ => 0
            };

            switch (step.Kind)
            {
                case FlowStepKind.Published: _published++; break;
                case FlowStepKind.CustomerCreated: _customersCreated++; break;
                case FlowStepKind.EmailSent: _emailsSent++; break;
            }

            var completed = false;
            if (bit != 0 && !string.IsNullOrEmpty(step.CorrelationId))
            {
                _seenByCorrelation.TryGetValue(step.CorrelationId, out var seen);
                var updated = seen | bit;
                _seenByCorrelation[step.CorrelationId] = updated;
                completed = seen != AllBits && updated == AllBits;
            }

            var entry = new FlowStepEvent(++_sequence, step, completed);
            _entries.Enqueue(entry);
            while (_entries.Count > Capacity)
            {
                _entries.Dequeue();
            }

            return entry;
        }
    }

    /// <summary>Counters since the last <see cref="Clear"/>; unaffected by entry eviction.</summary>
    public CountersEvent GetCounters()
    {
        lock (_gate)
        {
            return new CountersEvent(_published, _customersCreated, _emailsSent);
        }
    }

    /// <summary>Retained entries, newest first by UTC timestamp, ties broken by higher sequence first.</summary>
    public IReadOnlyList<FlowStepEvent> GetEntriesNewestFirst()
    {
        FlowStepEvent[] snapshot;
        lock (_gate)
        {
            snapshot = _entries.ToArray();
        }

        return snapshot
            .OrderByDescending(e => e.Step.OccurredAtUtc.UtcDateTime)
            .ThenByDescending(e => e.Sequence)
            .ToList();
    }

    /// <summary>All retained steps after the last clear, in sequence order, for SSE replay.</summary>
    public IReadOnlyList<FlowStepEvent> Replay()
    {
        lock (_gate)
        {
            return _entries.ToArray();
        }
    }

    /// <summary>True when Published, CustomerCreated and EmailSent were all seen for the correlation id.</summary>
    public bool IsCompleted(string correlationId)
    {
        lock (_gate)
        {
            return _seenByCorrelation.TryGetValue(correlationId, out var seen) && seen == AllBits;
        }
    }

    /// <summary>Removes entries and resets counters and completion tracking. Sequence keeps increasing.</summary>
    public void Clear()
    {
        lock (_gate)
        {
            _entries.Clear();
            _seenByCorrelation.Clear();
            _published = 0;
            _customersCreated = 0;
            _emailsSent = 0;
        }
    }
}
