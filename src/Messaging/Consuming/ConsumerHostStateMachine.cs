using Messaging.Abstractions;

namespace Messaging.Consuming;

/// <summary>
/// Encapsulates the <see cref="IMessageConsumerHost"/> lifecycle state transitions
/// Stopped → Starting → Running → Stopping → Stopped (Req 3.3). The host delegates all state changes
/// here so the legal transition graph lives in exactly one place (SRP) and the host itself focuses on
/// the start/stop work.
/// </summary>
/// <remarks>
/// <para>
/// Note on the enum: <see cref="ConsumerHostState"/> has no distinct terminal <c>Stopped</c> member;
/// <see cref="ConsumerHostState.Stopped"/> is reused as both the initial and the terminal resting
/// state. The documented sequence therefore begins and ends on the same member.
/// </para>
/// <para>
/// Legal transitions:
/// </para>
/// <list type="bullet">
///   <item><description><see cref="ConsumerHostState.Stopped"/> → <see cref="ConsumerHostState.Starting"/> (begin start).</description></item>
///   <item><description><see cref="ConsumerHostState.Starting"/> → <see cref="ConsumerHostState.Running"/> (start completed).</description></item>
///   <item><description><see cref="ConsumerHostState.Running"/> → <see cref="ConsumerHostState.Stopping"/> (begin stop/drain).</description></item>
///   <item><description><see cref="ConsumerHostState.Stopping"/> → <see cref="ConsumerHostState.Stopped"/> (drain completed).</description></item>
/// </list>
/// <para>
/// Any other transition throws <see cref="InvalidConsumerHostStateException"/> (fail-fast). The
/// transitions are serialized by a lock so concurrent Start/Stop calls cannot interleave into an
/// inconsistent state.
/// </para>
/// </remarks>
public sealed class ConsumerHostStateMachine
{
    private readonly object _gate = new();
    private ConsumerHostState _state = ConsumerHostState.Stopped;

    /// <summary>The current lifecycle state (Req 3.3). Thread-safe read.</summary>
    public ConsumerHostState State
    {
        get
        {
            lock (_gate)
            {
                return _state;
            }
        }
    }

    /// <summary>
    /// Transitions <see cref="ConsumerHostState.Stopped"/> → <see cref="ConsumerHostState.Starting"/>.
    /// </summary>
    /// <exception cref="InvalidConsumerHostStateException">If the host is not currently Stopped.</exception>
    public void BeginStart() => Transition(ConsumerHostState.Stopped, ConsumerHostState.Starting, "start");

    /// <summary>
    /// Transitions <see cref="ConsumerHostState.Starting"/> → <see cref="ConsumerHostState.Running"/>.
    /// </summary>
    /// <exception cref="InvalidConsumerHostStateException">If the host is not currently Starting.</exception>
    public void CompleteStart() => Transition(ConsumerHostState.Starting, ConsumerHostState.Running, "complete start of");

    /// <summary>
    /// Transitions <see cref="ConsumerHostState.Running"/> → <see cref="ConsumerHostState.Stopping"/>.
    /// </summary>
    /// <exception cref="InvalidConsumerHostStateException">If the host is not currently Running.</exception>
    public void BeginStop() => Transition(ConsumerHostState.Running, ConsumerHostState.Stopping, "stop");

    /// <summary>
    /// Transitions <see cref="ConsumerHostState.Stopping"/> → <see cref="ConsumerHostState.Stopped"/>.
    /// </summary>
    /// <exception cref="InvalidConsumerHostStateException">If the host is not currently Stopping.</exception>
    public void CompleteStop() => Transition(ConsumerHostState.Stopping, ConsumerHostState.Stopped, "complete stop of");

    private void Transition(ConsumerHostState expected, ConsumerHostState next, string transitionName)
    {
        lock (_gate)
        {
            if (_state != expected)
            {
                throw new InvalidConsumerHostStateException(_state, transitionName);
            }

            _state = next;
        }
    }
}
