namespace Demo.Communications;

/// <summary>Thread-safe in-memory outbox holding at most <see cref="MaxEntries"/> emails.</summary>
public sealed class InMemoryMockEmailOutbox : IMockEmailOutbox
{
    public const int MaxEntries = 10_000;

    private readonly object _gate = new();
    private readonly Queue<MockEmail> _emails = new();

    public void Add(MockEmail email)
    {
        ArgumentNullException.ThrowIfNull(email);

        lock (_gate)
        {
            while (_emails.Count >= MaxEntries)
            {
                _emails.Dequeue();
            }

            _emails.Enqueue(email);
        }
    }

    public IReadOnlyList<MockEmail> Snapshot()
    {
        lock (_gate)
        {
            return _emails.ToArray();
        }
    }
}
