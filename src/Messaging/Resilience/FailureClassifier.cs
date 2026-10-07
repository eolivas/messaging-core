using Messaging.Abstractions;
using Messaging.Consuming;
using Messaging.Contracts;

namespace Messaging.Resilience;

/// <summary>
/// The Application-layer <see cref="IFailureClassifier"/> that decides whether a processing failure
/// is retried or routed straight to the dead-letter sink (Req 6.2, 6.3). Classification is driven by
/// the exception <em>type</em>, never by matching on an exception's message text, so the decision is
/// deterministic and unit-testable.
/// </summary>
/// <remarks>
/// <para>
/// Transient (eligible for retry, Req 6.2) — timeout, connection failure, downstream-unavailable:
/// </para>
/// <list type="bullet">
///   <item><description><see cref="MessageTimeoutException"/> and the BCL <see cref="TimeoutException"/>.</description></item>
///   <item><description><see cref="TransportConnectionException"/>.</description></item>
///   <item><description><see cref="DownstreamUnavailableException"/>.</description></item>
/// </list>
/// <para>
/// Non-transient (routed to dead-letter with zero retries, Req 6.3) — validation failure, malformed
/// payload:
/// </para>
/// <list type="bullet">
///   <item><description><see cref="MessageValidationException"/>.</description></item>
///   <item><description><see cref="MalformedPayloadException"/>.</description></item>
///   <item><description><see cref="InvalidEnvelopeException"/> — a malformed envelope (Req 3.2, 4.3).</description></item>
///   <item><description><see cref="UnknownContractException"/> — an unbindable/poison payload (Req 4.6, 8.3).</description></item>
/// </list>
/// <para>
/// Any <see cref="MessagingFailureException"/> carries its own <see cref="MessagingFailureException.Kind"/>,
/// so new failure categories added to the Domain are classified correctly without changing this type.
/// </para>
/// <para>
/// Default for an unrecognized error: <see cref="FailureKind.Transient"/>. An unanticipated error is
/// treated as potentially recoverable and retried under the bounded policy rather than discarded
/// immediately; the attempt bound guarantees it still terminates at the dead-letter sink if it
/// persists (Req 6.5), so this errs toward not losing a message that might have failed transiently.
/// </para>
/// </remarks>
public sealed class FailureClassifier : IFailureClassifier
{
    /// <inheritdoc />
    /// <exception cref="ArgumentNullException">If <paramref name="error"/> is null.</exception>
    public FailureKind Classify(Exception error)
    {
        ArgumentNullException.ThrowIfNull(error);

        return error switch
        {
            // A domain failure declares its own category — the single source of truth (Req 6.2, 6.3).
            MessagingFailureException failure => failure.Kind,

            // Malformed/unbindable inputs are non-transient: retrying the same bytes cannot succeed
            // (Req 6.3, 8.3).
            InvalidEnvelopeException => FailureKind.NonTransient,
            UnknownContractException => FailureKind.NonTransient,

            // BCL timeout maps to the transient timeout category (Req 6.2).
            TimeoutException => FailureKind.Transient,

            // Unknown errors are treated as potentially transient and retried under the bounded
            // policy; the attempt cap still routes a persistent failure to dead-letter (Req 6.5).
            _ => FailureKind.Transient,
        };
    }
}
