using System.Collections.Concurrent;
using Messaging.Abstractions;
namespace Demo.Infrastructure.Messaging;

/// <summary>
/// In-memory dead-letter sink for providers without one (RabbitMQ, Kafka). Keeps a bounded
/// list of the most recent dead-lettered messages.
/// </summary>
public sealed class InMemoryDeadLetterSink : IDeadLetterSink
{
    public const int Capacity = 1_000;
    private readonly ConcurrentQueue<DeadLetterMessage> _messages = new();

    public IReadOnlyCollection<DeadLetterMessage> Messages => _messages.ToArray();

    public Task SendAsync(DeadLetterMessage message, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(message);
        cancellationToken.ThrowIfCancellationRequested();
        _messages.Enqueue(message);
        while (_messages.Count > Capacity && _messages.TryDequeue(out _))
        {
        }
        return Task.CompletedTask;
    }
}
