namespace Messaging.Consuming;

/// <summary>
/// The outcome of running the <see cref="IdempotencyGate"/> ahead of the consumer handler for a
/// single received message (Req 7.2, 7.4, 7.6). The consume pipeline uses the decision to choose
/// between invoking the handler and short-circuiting a duplicate.
/// </summary>
public enum IdempotencyGateOutcome
{
    /// <summary>
    /// Idempotency is disabled; the gate is bypassed and the handler runs with no idempotency key
    /// in context.
    /// </summary>
    Bypassed = 0,

    /// <summary>
    /// A key was derived and is not a known duplicate; the handler should run with the derived key in
    /// context (Req 7.1).
    /// </summary>
    Proceed = 1,

    /// <summary>
    /// The derived key matches a previously processed key; the message is a duplicate and must be
    /// acknowledged WITHOUT re-executing the handler side effects (Req 7.2, 7.4).
    /// </summary>
    Duplicate = 2,
}

/// <summary>
/// The decision produced by <see cref="IdempotencyGate.EvaluateAsync"/>: whether the handler should
/// run and the <see cref="ConsumeContext"/> (carrying the derived idempotency key and duplicate flag)
/// to expose to the consumer (Req 6.8, 7.1, 7.2).
/// </summary>
/// <remarks>
/// The gate never silently swallows an underivable key when idempotency is enabled — it throws a
/// <see cref="MissingIdempotencyKeyException"/> instead (Req 7.6) — so this decision only ever
/// represents a legitimate continue/duplicate/bypass path.
/// </remarks>
/// <param name="Outcome">What the pipeline should do next.</param>
/// <param name="Context">The idempotency context to hand to the consumer.</param>
public sealed record IdempotencyGateDecision(IdempotencyGateOutcome Outcome, ConsumeContext Context)
{
    /// <summary>True when the consumer handler should be invoked for this message.</summary>
    public bool ShouldInvokeHandler => Outcome != IdempotencyGateOutcome.Duplicate;
}
