using System.Diagnostics;
using Messaging.Configuration;
using Messaging.Consuming;
using Messaging.Contracts;
using Messaging.Observability;
using Messaging.Resilience;

namespace Messaging.Kafka.Streaming;

/// <summary>
/// The production <see cref="IStreamMessageProcessor"/> that drives the transport-neutral core consume
/// steps for each envelope the Kafka consumer host delivers: observability, the bounded
/// retry/dead-letter policy (<see cref="RetryExecutor"/>), and the per-message
/// <see cref="ConsumePipeline.DeliverAsync"/> (Req 5.1-5.8, 7.1-7.6).
/// </summary>
/// <remarks>
/// <para>
/// This adapter only drives the core; idempotency, binding, classification, retry, and dead-letter
/// routing all live behind the injected collaborators. An unknown contract, missing required header,
/// or missing idempotency key surfaces from the pipeline as a non-transient failure, so it is
/// dead-lettered with zero retries and never redelivered by the client.
/// </para>
/// <para>
/// Terminal outcome mapping: success or a routed-and-acknowledged dead-letter returns normally (the
/// host commits the offset); a retained-unacknowledged dead-letter throws
/// <see cref="RetainOffsetException"/> so the offset is not committed (Req 6.1-6.5).
/// </para>
/// <para>
/// Any exception leaving this processor carries the envelope's correlation id under
/// <see cref="KafkaConsumerHostedService.CorrelationIdDataKey"/> so the hosting loop can log it. The
/// payload is never read or logged here.
/// </para>
/// </remarks>
public sealed class KafkaStreamMessageProcessor : IStreamMessageProcessor
{
    private readonly ObservabilityContext _observability;
    private readonly RetryExecutor _retryExecutor;
    private readonly ConsumePipeline _consumePipeline;
    private readonly DeadLetterRouter _deadLetterRouter;
    private readonly RetryPolicyConfig _retryPolicy;

    /// <summary>Creates the processor with the core collaborators it drives.</summary>
    /// <param name="observability">Observability facade used to record the consume.</param>
    /// <param name="retryExecutor">The bounded retry / dead-letter policy.</param>
    /// <param name="consumePipeline">The per-message consume pipeline used as the processing attempt.</param>
    /// <param name="deadLetterRouter">The dead-letter routing seam shared with the executor.</param>
    /// <param name="retryPolicy">The uniform retry policy.</param>
    /// <exception cref="ArgumentNullException">If any dependency is null.</exception>
    public KafkaStreamMessageProcessor(
        ObservabilityContext observability,
        RetryExecutor retryExecutor,
        ConsumePipeline consumePipeline,
        DeadLetterRouter deadLetterRouter,
        RetryPolicyConfig retryPolicy)
    {
        _observability = observability ?? throw new ArgumentNullException(nameof(observability));
        _retryExecutor = retryExecutor ?? throw new ArgumentNullException(nameof(retryExecutor));
        _consumePipeline = consumePipeline ?? throw new ArgumentNullException(nameof(consumePipeline));
        _deadLetterRouter = deadLetterRouter ?? throw new ArgumentNullException(nameof(deadLetterRouter));
        _retryPolicy = retryPolicy ?? throw new ArgumentNullException(nameof(retryPolicy));
    }

    /// <inheritdoc />
    /// <exception cref="ArgumentNullException">If <paramref name="envelope"/> is null.</exception>
    /// <exception cref="RetainOffsetException">
    /// If the message was dead-lettered but the sink retained it unacknowledged (Req 6.3, 6.4).
    /// </exception>
    public async Task ProcessAsync(MessageEnvelope envelope, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(envelope);

        try
        {
            // Best-effort observation: a failing sink must never fail the consume.
            using Activity? consumeSpan = TryObserve(envelope);

            RetryResult result = await _retryExecutor
                .ExecuteAsync(
                    envelope,
                    _retryPolicy,
                    processAsync: ct => _consumePipeline.DeliverAsync(envelope, ct),
                    idempotencyKey: null,
                    cancellationToken)
                .ConfigureAwait(false);

            if (result.Outcome == RetryOutcome.Succeeded)
            {
                return;
            }

            if (result.DeadLetterRouting == DeadLetterRoutingResult.RetainedUnacknowledged)
            {
                throw new RetainOffsetException();
            }

            // Routed and acknowledged: return so the host commits the offset.
        }
        catch (Exception ex)
        {
            AttachCorrelationId(ex, envelope.CorrelationId);
            throw;
        }
    }

    private Activity? TryObserve(MessageEnvelope envelope)
    {
        try
        {
            return _observability.OnConsume(envelope.CorrelationId, envelope.Headers);
        }
        catch (Exception)
        {
            return null;
        }
    }

    private static void AttachCorrelationId(Exception ex, string? correlationId)
    {
        if (string.IsNullOrWhiteSpace(correlationId))
        {
            return;
        }

        try
        {
            ex.Data[KafkaConsumerHostedService.CorrelationIdDataKey] = correlationId;
        }
        catch (Exception)
        {
            // Data may be read-only or reject the value; the correlation id is diagnostic only.
        }
    }
}
