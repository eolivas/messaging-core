using Messaging.Contracts;

namespace Messaging.Abstractions;

/// <summary>
/// A message routed to the dead-letter sink after exhausting retries or being classified as a
/// poison message. Preserves the complete original envelope byte-for-byte alongside the context
/// needed to inspect and reprocess it later (Req 6.6, 8.2).
/// </summary>
/// <param name="Envelope">The complete original <see cref="MessageEnvelope"/>, preserved unchanged (Req 8.2).</param>
/// <param name="CorrelationId">The correlation id carried from the original envelope (Req 8.2).</param>
/// <param name="IdempotencyKey">The idempotency key for the message, when one was derived (Req 6.6).</param>
/// <param name="FailureReason">The reason the message could not be processed (Req 6.6, 8.2).</param>
/// <param name="AttemptCount">The total number of processing attempts made before dead-lettering (Req 8.2).</param>
/// <param name="DeadLetteredAtUtc">The UTC timestamp of the dead-letter event (Req 8.2).</param>
public sealed record DeadLetterMessage(
    MessageEnvelope Envelope,
    string CorrelationId,
    string? IdempotencyKey,
    string FailureReason,
    int AttemptCount,
    DateTimeOffset DeadLetteredAtUtc);
