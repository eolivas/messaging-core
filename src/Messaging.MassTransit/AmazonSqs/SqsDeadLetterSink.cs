using System.Globalization;
using Messaging.Abstractions;
using Messaging.MassTransit.Transport;

namespace Messaging.MassTransit.AmazonSqs;

/// <summary>
/// The <see cref="IDeadLetterSink"/> for Amazon SQS. Sends the original envelope, unchanged, as an
/// <see cref="EnvelopeMessage"/> body to the Dead_Letter_Queue and carries the dead-letter context
/// (reason, attempt count, timestamp, idempotency key, correlation id) as message attributes, so the
/// dead-lettered body stays consumable (Req 10.6).
/// </summary>
/// <remarks>
/// A send failure is not caught: it propagates so <c>DeadLetterRouter</c> returns
/// <c>RetainedUnacknowledged</c> and the message is retained for redelivery (Req 10.7, 10.8).
/// </remarks>
public sealed class SqsDeadLetterSink : IDeadLetterSink
{
    /// <summary>Attribute: the failure reason, truncated, never the payload.</summary>
    public const string ReasonAttribute = "x-dead-letter-reason";

    /// <summary>Attribute: total processing attempts.</summary>
    public const string AttemptCountAttribute = "x-attempt-count";

    /// <summary>Attribute: UTC instant of the dead-letter event (round-trip format).</summary>
    public const string DeadLetteredAtAttribute = "x-dead-lettered-at";

    /// <summary>Attribute: the idempotency key, present only when one was derived.</summary>
    public const string IdempotencyKeyAttribute = "x-idempotency-key";

    /// <summary>Attribute: the correlation id.</summary>
    public const string CorrelationIdAttribute = "x-correlation-id";

    /// <summary>Maximum length, in characters, of the failure reason attribute.</summary>
    public const int ReasonMaxLength = 1024;

    private readonly IDeadLetterQueueSender _sender;
    private readonly string _deadLetterQueueName;
    private readonly bool _fifo;
    private readonly FifoGroupSelection _groupSelection;

    /// <summary>Initializes the sink.</summary>
    /// <param name="sender">Sends to the Dead_Letter_Queue.</param>
    /// <param name="deadLetterQueueName">The Dead_Letter_Queue name.</param>
    /// <param name="fifo">Whether the queue is FIFO.</param>
    /// <param name="groupSelection">How the FIFO group id is derived (same rule as publish).</param>
    /// <exception cref="ArgumentNullException">If a dependency is null.</exception>
    /// <exception cref="ArgumentException">If the queue name is blank.</exception>
    public SqsDeadLetterSink(
        IDeadLetterQueueSender sender,
        string deadLetterQueueName,
        bool fifo,
        FifoGroupSelection groupSelection)
    {
        _sender = sender ?? throw new ArgumentNullException(nameof(sender));
        ArgumentException.ThrowIfNullOrWhiteSpace(deadLetterQueueName);
        _deadLetterQueueName = deadLetterQueueName;
        _fifo = fifo;
        _groupSelection = groupSelection;
    }

    /// <inheritdoc />
    public Task SendAsync(DeadLetterMessage message, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(message);

        var attributes = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [ReasonAttribute] = Truncate(message.FailureReason),
            [AttemptCountAttribute] = message.AttemptCount.ToString(CultureInfo.InvariantCulture),
            [DeadLetteredAtAttribute] = message.DeadLetteredAtUtc.ToString("O", CultureInfo.InvariantCulture),
            [CorrelationIdAttribute] = message.CorrelationId,
        };

        if (!string.IsNullOrEmpty(message.IdempotencyKey))
        {
            attributes[IdempotencyKeyAttribute] = message.IdempotencyKey;
        }

        string? groupId = null;
        string? deduplicationId = null;
        if (_fifo)
        {
            groupId = FifoMessageIds.GroupId(message.Envelope, _groupSelection);
            deduplicationId = string.Create(
                CultureInfo.InvariantCulture, $"{FifoMessageIds.DeduplicationId(message.Envelope)}:dlq");
        }

        return _sender.SendToDeadLetterQueueAsync(
            _deadLetterQueueName,
            EnvelopeMessage.FromEnvelope(message.Envelope),
            attributes,
            groupId,
            deduplicationId,
            cancellationToken);
    }

    private static string Truncate(string? reason)
    {
        if (string.IsNullOrEmpty(reason))
        {
            return string.Empty;
        }

        if (reason.Length <= ReasonMaxLength)
        {
            return reason;
        }

        // Do not split a surrogate pair at the cut.
        var length = char.IsHighSurrogate(reason[ReasonMaxLength - 1]) ? ReasonMaxLength - 1 : ReasonMaxLength;
        return reason[..length];
    }
}
