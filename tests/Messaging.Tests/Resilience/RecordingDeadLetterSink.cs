using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Messaging.Abstractions;

namespace Messaging.Tests.Resilience;

/// <summary>
/// An in-memory <see cref="IDeadLetterSink"/> that records every message routed to it, so a test can
/// assert the exact count and contents of dead-letter routing without a real sink (task 11.2).
/// </summary>
internal sealed class RecordingDeadLetterSink : IDeadLetterSink
{
    private readonly List<DeadLetterMessage> _sent = new();

    /// <summary>Every message routed to the sink, in order.</summary>
    public IReadOnlyList<DeadLetterMessage> Sent => _sent;

    /// <inheritdoc />
    public Task SendAsync(DeadLetterMessage message, CancellationToken cancellationToken)
    {
        _sent.Add(message);
        return Task.CompletedTask;
    }
}
