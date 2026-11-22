using Messaging.MassTransit.AmazonSqs;
using Messaging.MassTransit.Transport;
using Microsoft.Extensions.DependencyInjection;

namespace Demo.Infrastructure.Composition;

/// <summary>
/// Resolves the bus adapter on first send. The adapter needs the consume processor, which needs
/// the dead-letter router and sink, which need this sender, so eager resolution would be a cycle.
/// </summary>
public sealed class LazyDeadLetterQueueSender : IDeadLetterQueueSender
{
    private readonly IServiceProvider _services;

    public LazyDeadLetterQueueSender(IServiceProvider services)
    {
        _services = services ?? throw new ArgumentNullException(nameof(services));
    }

    public Task SendToDeadLetterQueueAsync(
        string queueName,
        EnvelopeMessage message,
        IReadOnlyDictionary<string, string> attributes,
        string? messageGroupId,
        string? deduplicationId,
        CancellationToken cancellationToken) =>
        _services.GetRequiredService<AmazonSqsBusAdapter>().SendToDeadLetterQueueAsync(
            queueName, message, attributes, messageGroupId, deduplicationId, cancellationToken);
}
