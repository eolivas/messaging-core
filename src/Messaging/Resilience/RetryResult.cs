namespace Messaging.Resilience;

/// <summary>
/// The result of running a message through the <see cref="RetryExecutor"/>: the terminal
/// <see cref="Outcome"/>, the total number of processing attempts actually made, and — when the
/// message was dead-lettered — how the dead-letter routing itself resolved (Req 6.2, 6.3, 6.5, 8.1,
/// 8.6).
/// </summary>
/// <param name="Outcome">The terminal disposition of the message.</param>
/// <param name="AttemptCount">
/// The total number of processing attempts made. For a non-transient failure this is exactly 1 (the
/// single attempt that classified non-transient, with no retry — Req 6.3); for an exhausted
/// transient failure this equals the configured maximum attempts (Req 6.5); for success it is the
/// attempt that succeeded.
/// </param>
/// <param name="DeadLetterRouting">
/// The outcome of routing the message to the dead-letter sink, or <see langword="null"/> when the
/// message succeeded and was never dead-lettered. When the message was dead-lettered this tells the
/// pipeline whether to acknowledge the source
/// (<see cref="DeadLetterRoutingResult.RoutedAndAcknowledged"/>, Req 8.1) or retain it unacknowledged
/// because the sink failed (<see cref="DeadLetterRoutingResult.RetainedUnacknowledged"/>, Req 8.6).
/// </param>
public sealed record RetryResult(
    RetryOutcome Outcome,
    int AttemptCount,
    DeadLetterRoutingResult? DeadLetterRouting = null);
