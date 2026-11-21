using Messaging.MassTransit.Transport;

namespace Messaging.MassTransit.AmazonSqs;

/// <summary>
/// Sends an original <see cref="EnvelopeMessage"/> to the Dead_Letter_Queue regardless of the bus role,
/// so a Consumer role can dead-letter (Req 10.6, 11.4). Implemented by the bus adapter; a small port so
/// <see cref="SqsDeadLetterSink"/> never depends on MassTransit types.
/// </summary>
public interface IDeadLetterQueueSender
{
    /// <summary>Sends the message to <c>queue:{queueName}</c>.</summary>
    /// <param name="queueName">The Dead_Letter_Queue name.</param>
    /// <param name="message">The original envelope message, sent unchanged as the body.</param>
    /// <param name="attributes">Dead-letter context carried as message attributes (no payload bytes).</param>
    /// <param name="messageGroupId">The FIFO message group id, or <see langword="null"/> for a standard queue.</param>
    /// <param name="deduplicationId">The FIFO deduplication id, or <see langword="null"/> for a standard queue.</param>
    /// <param name="cancellationToken">Token to cancel the send.</param>
    /// <exception cref="Exception">Any send failure propagates to the caller.</exception>
    Task SendToDeadLetterQueueAsync(
        string queueName,
        EnvelopeMessage message,
        IReadOnlyDictionary<string, string> attributes,
        string? messageGroupId,
        string? deduplicationId,
        CancellationToken cancellationToken);
}
