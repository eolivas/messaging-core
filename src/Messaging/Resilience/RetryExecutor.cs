using Messaging.Abstractions;
using Messaging.Configuration;
using Messaging.Contracts;

namespace Messaging.Resilience;

/// <summary>
/// The Application-layer retry policy: it drives a single message's processing attempts, bounds them
/// to the configured maximum, applies the computed backoff delay between consecutive transient
/// attempts, and routes to the dead-letter sink either immediately (non-transient) or on exhaustion
/// (Req 6.2, 6.3, 6.4, 6.5). The core defines this uniform policy and maps it onto the active
/// transport's native retry/redelivery mechanism in the adapter tasks; it is not a bespoke
/// redelivery engine.
/// </summary>
/// <remarks>
/// <para>
/// <b>Delivery semantics are at-least-once (Req 6.7).</b> A processing attempt may succeed while its
/// acknowledgement is lost, so a message can be redelivered and processed again; consumers guard
/// against duplicate side effects with the idempotency key (Req 6.8, 7). The uniform
/// <see cref="RetryPolicyConfig"/> (max attempts, backoff strategy, per-attempt delay) is the policy
/// the core defines and maps onto each transport: MassTransit redelivery for Pub/Sub and Queue,
/// Kafka offset reprocessing for Streaming, and batch checkpoint re-read for Batch.
/// </para>
/// <para>
/// Control flow for a single message:
/// </para>
/// <list type="number">
///   <item><description>Run a processing attempt.</description></item>
///   <item><description>On success → return <see cref="RetryOutcome.Succeeded"/> immediately.</description></item>
///   <item><description>On failure, classify via <see cref="IFailureClassifier"/>:
///     <list type="bullet">
///       <item><description><see cref="FailureKind.NonTransient"/> → route to dead-letter now, zero
///       retries, attempt count 1 (Req 6.3).</description></item>
///       <item><description><see cref="FailureKind.Transient"/> → if attempts remain, wait the
///       computed backoff delay (Req 6.4) and retry; otherwise route to dead-letter on exhaustion
///       (Req 6.5).</description></item>
///     </list>
///   </description></item>
/// </list>
/// <para>
/// The attempt count is bounded by <see cref="RetryPolicyConfig.MaxAttempts"/>, so a transient
/// failure is attempted at most <c>MaxAttempts</c> times before dead-lettering — the message always
/// reaches a terminal state and never loops unbounded (Req 6.2, 6.5). The dead-letter
/// <see cref="DeadLetterMessage"/> preserves the original envelope, correlation id, idempotency key,
/// failure reason, and total attempt count (Req 6.6, 8.2).
/// </para>
/// <para>
/// SRP: this type owns only the bounded-attempt/dead-letter policy. Classification lives in
/// <see cref="IFailureClassifier"/>, delay computation in <see cref="BackoffCalculator"/>, and the
/// dead-letter routing contract (build the record, emit the metric/log, acknowledge or retain) behind
/// <see cref="DeadLetterRouter"/>; the executor composes them. Delegating to the router rather than
/// calling <see cref="IDeadLetterSink"/> directly means exhausted and non-transient failures emit the
/// one metric / one log and honor the ack-vs-retain semantics uniformly (Req 8.1, 8.4, 8.6).
/// </para>
/// </remarks>
public sealed class RetryExecutor
{
    private readonly IFailureClassifier _failureClassifier;
    private readonly BackoffCalculator _backoffCalculator;
    private readonly DeadLetterRouter _deadLetterRouter;
    private readonly Func<TimeSpan, CancellationToken, Task> _delay;

    /// <summary>
    /// Creates the retry executor with real backoff delays (<see cref="Task.Delay(TimeSpan, CancellationToken)"/>).
    /// </summary>
    /// <param name="failureClassifier">Classifies a failure as transient or non-transient (Req 6.2, 6.3).</param>
    /// <param name="backoffCalculator">Computes the per-attempt backoff delay (Req 6.4).</param>
    /// <param name="deadLetterRouter">Routes exhausted/non-transient messages to the dead-letter sink (Req 6.5, 6.6, 8).</param>
    /// <exception cref="ArgumentNullException">If any dependency is null.</exception>
    public RetryExecutor(
        IFailureClassifier failureClassifier,
        BackoffCalculator backoffCalculator,
        DeadLetterRouter deadLetterRouter)
        : this(failureClassifier, backoffCalculator, deadLetterRouter, Task.Delay)
    {
    }

    /// <summary>
    /// Creates the retry executor with an injected delay function, so a test can run without wall-clock
    /// waits while still exercising the backoff computation.
    /// </summary>
    /// <param name="failureClassifier">Classifies a failure as transient or non-transient (Req 6.2, 6.3).</param>
    /// <param name="backoffCalculator">Computes the per-attempt backoff delay (Req 6.4).</param>
    /// <param name="deadLetterRouter">Routes exhausted/non-transient messages to the dead-letter sink (Req 6.5, 6.6, 8).</param>
    /// <param name="delay">The function invoked to wait a computed backoff delay between attempts.</param>
    /// <exception cref="ArgumentNullException">If any dependency is null.</exception>
    public RetryExecutor(
        IFailureClassifier failureClassifier,
        BackoffCalculator backoffCalculator,
        DeadLetterRouter deadLetterRouter,
        Func<TimeSpan, CancellationToken, Task> delay)
    {
        _failureClassifier = failureClassifier ?? throw new ArgumentNullException(nameof(failureClassifier));
        _backoffCalculator = backoffCalculator ?? throw new ArgumentNullException(nameof(backoffCalculator));
        _deadLetterRouter = deadLetterRouter ?? throw new ArgumentNullException(nameof(deadLetterRouter));
        _delay = delay ?? throw new ArgumentNullException(nameof(delay));
    }

    /// <summary>
    /// Processes a message under the retry policy: bounds attempts to <paramref name="policy"/>'s
    /// maximum, retries transient failures with backoff, and routes non-transient or exhausted
    /// failures to the dead-letter sink (Req 6.2, 6.3, 6.4, 6.5).
    /// </summary>
    /// <param name="envelope">The message being processed; preserved into the dead-letter message (Req 6.6, 8.2).</param>
    /// <param name="policy">The retry policy supplying max attempts, backoff strategy, and base delay.</param>
    /// <param name="processAsync">
    /// The processing attempt. It completes normally on success and throws on failure; the thrown
    /// exception is classified to decide retry vs dead-letter.
    /// </param>
    /// <param name="idempotencyKey">The idempotency key to preserve on the dead-letter message, when derived (Req 6.6).</param>
    /// <param name="cancellationToken">Token to cancel processing, backoff waits, and dead-letter routing.</param>
    /// <returns>The terminal <see cref="RetryResult"/> with the total attempt count made.</returns>
    /// <exception cref="ArgumentNullException">If <paramref name="envelope"/>, <paramref name="policy"/>, or <paramref name="processAsync"/> is null.</exception>
    public async Task<RetryResult> ExecuteAsync(
        MessageEnvelope envelope,
        RetryPolicyConfig policy,
        Func<CancellationToken, Task> processAsync,
        string? idempotencyKey,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(envelope);
        ArgumentNullException.ThrowIfNull(policy);
        ArgumentNullException.ThrowIfNull(processAsync);

        // The policy is a validated configuration (max attempts 1..100), but guard against a
        // zero/negative value defensively so the loop always makes at least one attempt.
        var maxAttempts = policy.MaxAttempts < 1 ? 1 : policy.MaxAttempts;

        for (var attempt = 1; attempt <= maxAttempts; attempt++)
        {
            try
            {
                await processAsync(cancellationToken).ConfigureAwait(false);
                return new RetryResult(RetryOutcome.Succeeded, attempt);
            }
            catch (Exception error) when (error is not OperationCanceledException)
            {
                var kind = _failureClassifier.Classify(error);

                if (kind == FailureKind.NonTransient)
                {
                    // Non-transient: dead-letter immediately with zero retries (Req 6.3).
                    var routing = await RouteToDeadLetterAsync(envelope, idempotencyKey, error, attempt, cancellationToken)
                        .ConfigureAwait(false);
                    return new RetryResult(RetryOutcome.DeadLetteredNonTransient, attempt, routing);
                }

                if (attempt >= maxAttempts)
                {
                    // Transient but attempts exhausted: dead-letter on exhaustion (Req 6.5).
                    var routing = await RouteToDeadLetterAsync(envelope, idempotencyKey, error, attempt, cancellationToken)
                        .ConfigureAwait(false);
                    return new RetryResult(RetryOutcome.DeadLetteredExhausted, attempt, routing);
                }

                // Transient with attempts remaining: wait the computed backoff, then retry (Req 6.4).
                var delay = _backoffCalculator.ComputeDelay(policy, attempt);
                await _delay(delay, cancellationToken).ConfigureAwait(false);
            }
        }

        // Unreachable: the loop either returns on success or dead-letters on the final attempt. The
        // throw guards the invariant rather than silently returning a wrong outcome.
        throw new InvalidOperationException(
            "Retry loop completed without reaching a terminal outcome; this indicates a policy invariant was violated.");
    }

    private Task<DeadLetterRoutingResult> RouteToDeadLetterAsync(
        MessageEnvelope envelope,
        string? idempotencyKey,
        Exception error,
        int attemptCount,
        CancellationToken cancellationToken)
        => _deadLetterRouter.RouteAsync(
            envelope,
            failureReason: error.Message,
            idempotencyKey: idempotencyKey,
            attemptCount: attemptCount,
            cancellationToken);
}
