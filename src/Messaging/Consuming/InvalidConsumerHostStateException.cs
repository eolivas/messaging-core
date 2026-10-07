using Messaging.Abstractions;

namespace Messaging.Consuming;

/// <summary>
/// Domain-specific exception raised when a consumer host is asked to make a lifecycle transition that
/// is not legal from its current <see cref="ConsumerHostState"/> (Req 3.3) — for example starting a
/// host that is already <see cref="ConsumerHostState.Running"/>. Fail-fast: an illegal transition
/// signals a lifecycle misuse rather than a transient condition, so it throws instead of silently
/// no-op'ing.
/// </summary>
public sealed class InvalidConsumerHostStateException : Exception
{
    /// <summary>The state the host was in when the illegal transition was attempted.</summary>
    public ConsumerHostState CurrentState { get; }

    /// <summary>The transition that was attempted.</summary>
    public string AttemptedTransition { get; }

    /// <summary>
    /// Creates the exception describing an illegal lifecycle transition.
    /// </summary>
    /// <param name="currentState">The host's current state.</param>
    /// <param name="attemptedTransition">The attempted transition (e.g. "Start", "Stop").</param>
    public InvalidConsumerHostStateException(ConsumerHostState currentState, string attemptedTransition)
        : base($"Cannot {attemptedTransition} the consumer host from state '{currentState}'.")
    {
        CurrentState = currentState;
        AttemptedTransition = attemptedTransition;
    }
}
