using System.Diagnostics;
using Messaging.Configuration;
using Messaging.Consuming;
using Messaging.Contracts;
using Messaging.Observability;
using Messaging.Resilience;

namespace Messaging.MassTransit.Consuming;

/// <summary>
/// The production <see cref="IMassTransitConsumeProcessor"/> that drives the transport-neutral core
/// consume steps for each envelope MassTransit delivers. It composes the Application-layer pieces —
/// observability (<see cref="ObservabilityContext"/>), the bounded retry/dead-letter policy
/// (<see cref="RetryExecutor"/>), and the per-message consume pipeline
/// (<see cref="ConsumePipeline.DeliverAsync"/>) — so a message consumed over RabbitMQ behaves exactly
/// as it does in every other mode (design §2, Req 1.3, 1.9).
/// </summary>
/// <remarks>
/// <para>
/// <b>Single responsibility.</b> This adapter only <em>drives</em> the core; it never reimplements
/// idempotency, contract binding, failure classification, retry, or dead-letter routing. Each of
/// those lives behind the injected core collaborators. The processor's one job is to translate the
/// core's terminal <see cref="RetryOutcome"/> / <see cref="DeadLetterRoutingResult"/> into the
/// MassTransit acknowledge-vs-redeliver contract (Req 6.4, 6.5, 6.6, 6.7).
/// </para>
/// <para>
/// <b>Per-message flow.</b>
/// </para>
/// <list type="number">
///   <item><description>Observe the consume (<see cref="ObservabilityContext.OnConsume"/>) with the
///   envelope's correlation id and headers (Req 9.2). This is best-effort: a throwing observability
///   sink must never fail the operation (Req 9.8).</description></item>
///   <item><description>Run the message through <see cref="RetryExecutor.ExecuteAsync"/>, whose
///   processing attempt is <see cref="ConsumePipeline.DeliverAsync"/>. The executor owns the bounded
///   attempt count, transient-vs-non-transient classification, and dead-letter routing
///   (Req 7.2, 7.3, 7.5, 7.6, 8.1, 8.3, 8.5, 8.8). An unknown contract or missing idempotency key
///   surfaces as a non-transient failure, so the handler is never invoked and the message is
///   dead-lettered with zero retries.</description></item>
///   <item><description>Map the terminal outcome to the ack contract: a success or a
///   routed-and-acknowledged dead-letter returns normally (MassTransit acknowledges the delivery); a
///   retained-unacknowledged dead-letter throws <see cref="RetainForRedeliveryException"/> so
///   MassTransit redelivers it for a later attempt rather than dropping it (Req 1.9, 6.7, 8.6,
///   8.7).</description></item>
/// </list>
/// <para>
/// DIP: every collaborator is supplied via constructor injection and null-guarded; the adapter holds
/// no static state and owns no transport types (Req 8.8).
/// </para>
/// </remarks>
public sealed class CoreConsumeProcessor : IMassTransitConsumeProcessor
{
    private readonly ObservabilityContext _observability;
    private readonly RetryExecutor _retryExecutor;
    private readonly ConsumePipeline _consumePipeline;
    private readonly DeadLetterRouter _deadLetterRouter;
    private readonly RetryPolicyConfig _retryPolicy;

    /// <summary>
    /// Creates the processor with the core collaborators it drives.
    /// </summary>
    /// <param name="observability">The observability facade used to record the consume (Req 9.2).</param>
    /// <param name="retryExecutor">The bounded retry / dead-letter policy the message runs through (Req 8).</param>
    /// <param name="consumePipeline">The per-message consume pipeline invoked as the processing attempt (Req 1.3).</param>
    /// <param name="deadLetterRouter">
    /// The dead-letter routing contract. Injected so the processor composes the same routing seam the
    /// executor uses, keeping the adapter aligned with the core wiring (Req 8.1).
    /// </param>
    /// <param name="retryPolicy">The uniform retry policy supplying the bounded attempt count (Req 8.1, 8.8).</param>
    /// <exception cref="ArgumentNullException">If any dependency is null.</exception>
    public CoreConsumeProcessor(
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
    /// <exception cref="RetainForRedeliveryException">
    /// If the message was dead-lettered but the sink retained it unacknowledged, so MassTransit must
    /// redeliver it rather than acknowledge the delivery (Req 6.7, 8.7).
    /// </exception>
    public async Task ProcessAsync(MessageEnvelope envelope, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(envelope);

        // Step 1: observe the consume (correlation id + headers). ObservabilityContext already
        // swallows sink failures (Req 9.8); dispose the returned span once this delivery resolves.
        using Activity? consumeSpan = _observability.OnConsume(envelope.CorrelationId, envelope.Headers);

        // Step 2: run the message through the bounded retry / dead-letter policy. The processing
        // attempt is the core consume pipeline, so idempotency, binding, classification, retry, and
        // dead-letter routing are all owned by the core — this adapter does not reimplement any of it
        // (Req 1.3, 7.2, 7.3, 7.5, 7.6, 8.1, 8.3, 8.5, 8.8). A null idempotency key lets the pipeline
        // derive its own per its configured gate (or run without one when idempotency is disabled).
        RetryResult result = await _retryExecutor
            .ExecuteAsync(
                envelope,
                _retryPolicy,
                processAsync: ct => _consumePipeline.DeliverAsync(envelope, ct),
                idempotencyKey: null,
                cancellationToken)
            .ConfigureAwait(false);

        // Step 3: map the terminal outcome onto MassTransit's acknowledge-vs-redeliver contract
        // (Req 1.9, 6.6, 6.7).
        if (result.Outcome == RetryOutcome.Succeeded)
        {
            // Processed (including a short-circuited idempotent duplicate): acknowledge by returning.
            return;
        }

        // Dead-lettered (non-transient or exhausted). The message leaves the pipeline only once it is
        // safely in the sink: a routed-and-acknowledged dead-letter returns (ack), while a
        // retained-unacknowledged dead-letter must be redelivered, never silently dropped (Req 8.6,
        // 8.7).
        if (result.DeadLetterRouting == DeadLetterRoutingResult.RetainedUnacknowledged)
        {
            throw new RetainForRedeliveryException();
        }

        // RoutedAndAcknowledged (or any other terminal non-success that completed routing):
        // acknowledge the delivery by returning normally.
    }
}
