namespace Demo.Infrastructure.Messaging;

/// <summary>
/// Bounded cache (1,000 entries, oldest correlation id evicted first) of the last attempt
/// and failure kind per correlation id. Written by the handler chain, read by the
/// dead-letter log.
/// </summary>
public sealed class AttemptTracker
{
    public const int Capacity = 1_000;
    private readonly object _gate = new();
    private readonly Dictionary<string, AttemptInfo> _entries = new();
    private readonly Queue<string> _order = new();

    public int Count
    {
        get
        {
            lock (_gate)
            {
                return _entries.Count;
            }
        }
    }

    public void Record(string correlationId, int attempt, string failureKind)
    {
        ArgumentNullException.ThrowIfNull(correlationId);
        ArgumentNullException.ThrowIfNull(failureKind);
        lock (_gate)
        {
            if (!_entries.ContainsKey(correlationId))
            {
                _order.Enqueue(correlationId);
            }
            _entries[correlationId] = new AttemptInfo(attempt, failureKind);
            while (_entries.Count > Capacity && _order.TryDequeue(out var oldest))
            {
                _entries.Remove(oldest);
            }
        }
    }

    public bool TryGet(string correlationId, out AttemptInfo? info)
    {
        lock (_gate)
        {
            return _entries.TryGetValue(correlationId, out info);
        }
    }
}
