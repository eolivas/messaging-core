using Messaging.Abstractions;
using Messaging.Contracts;

namespace Messaging.Consuming;

/// <summary>
/// The Application-layer <see cref="IMessageConsumerHost"/> that fronts every incoming message
/// (Req 3.3, 4.5, 4.6, 4.7). It owns the lifecycle state machine and the mode-independent consume
/// policy that runs for each delivered envelope: propagate the correlation id, bind the payload to a
/// known contract, then invoke the registered handler. Application handlers depend only on
/// <see cref="IMessageHandler{TContract}"/>; the transport-specific pull/subscribe loop is a provider
/// concern wired in a later spec and drives this host through <see cref="DeliverAsync"/>.
/// </summary>
/// <remarks>
/// <para>
/// <b>Lifecycle (Req 3.3).</b> <see cref="StartAsync"/> drives Stopped → Starting → Running and
/// <see cref="StopAsync"/> drives Running → Stopping → Stopped via
/// <see cref="ConsumerHostStateMachine"/>. The state is observable through <see cref="State"/> at
/// every step. Full graceful drain/timeout handling (Req 10.6, 10.7) is layered on in task 18; this
/// host establishes the state sequence those steps build on.
/// </para>
/// <para>
/// <b>Per-message pipeline order and failure semantics (<see cref="DeliverAsync"/>):</b>
/// </para>
/// <list type="number">
///   <item>
///     <description>
///     <b>Idempotency gate</b> (<see cref="IdempotencyGate"/>, when configured): runs before the
///     handler. An underivable key is rejected with <see cref="MissingIdempotencyKeyException"/>
///     before any side effects (Req 7.6); a known duplicate is acknowledged without re-invoking the
///     handler (Req 7.2, 7.4); otherwise the derived key is exposed to the consumer via
///     <see cref="ConsumeContext"/> (Req 6.8, 7.1).
///     </description>
///   </item>
///   <item>
///     <description>
///     <b>Bind contract</b> (<see cref="IContractBinder"/>): the untrusted payload is bound to the
///     <em>known</em> contract for its <see cref="MessageEnvelope.MessageType"/>. An unknown or
///     unbindable contract throws <see cref="UnknownContractException"/> and the handler is never
///     invoked, so the payload is never delivered to the consumer (Req 4.5, 4.6). Tolerant
///     deserialization keeps earlier-version consumers compatible with additive fields (Req 4.7).
///     </description>
///   </item>
///   <item>
///     <description>
///     <b>Invoke handler</b> (<see cref="IHandlerInvoker"/>): the bound message and the original
///     envelope are handed to the registered handler. The envelope's correlation id is passed through
///     unchanged end-to-end (Req 9.4).
///     </description>
///   </item>
/// </list>
/// <para>
/// Delivery is only accepted while the host is <see cref="ConsumerHostState.Running"/>; a message
/// delivered when the host is not running is rejected so a stopped/stopping host does not process new
/// work (Req 10.5). Binding runs before the handler by construction, giving task 9.2 a clean seam to
/// assert unknown-contract rejection and task 9.4 a seam to assert correlation-id propagation.
/// </para>
/// <para>
/// SRP: this host orchestrates lifecycle + consume policy only. State transitions live in
/// <see cref="ConsumerHostStateMachine"/>, binding in <see cref="IContractBinder"/>, and handler
/// dispatch in <see cref="IHandlerInvoker"/>; the host composes them.
/// </para>
/// </remarks>
public sealed class ConsumePipeline : IMessageConsumerHost
{
    private readonly ConsumerHostStateMachine _stateMachine;
    private readonly IContractBinder _contractBinder;
    private readonly IHandlerInvoker _handlerInvoker;
    private readonly IdempotencyGate? _idempotencyGate;

    /// <summary>
    /// Creates the consume pipeline / consumer host without an idempotency gate: every bound message
    /// is handed straight to the handler. Used where idempotent consumption is not configured.
    /// </summary>
    /// <param name="stateMachine">The lifecycle state machine (Req 3.3).</param>
    /// <param name="contractBinder">The port that binds payloads to known contracts (Req 4.5, 4.6).</param>
    /// <param name="handlerInvoker">The port that dispatches the bound message to the handler.</param>
    /// <exception cref="ArgumentNullException">If any dependency is null.</exception>
    public ConsumePipeline(
        ConsumerHostStateMachine stateMachine,
        IContractBinder contractBinder,
        IHandlerInvoker handlerInvoker)
        : this(stateMachine, contractBinder, handlerInvoker, idempotencyGate: null)
    {
    }

    /// <summary>
    /// Creates the consume pipeline / consumer host, composing an optional idempotency gate that runs
    /// before the handler (Req 7). When <paramref name="idempotencyGate"/> is non-null the gate
    /// evaluates each message first — rejecting an underivable key (Req 7.6) and short-circuiting a
    /// duplicate without re-invoking the handler (Req 7.2, 7.4) — mirroring how the pipeline already
    /// composes <see cref="IContractBinder"/> and <see cref="IHandlerInvoker"/> (SRP).
    /// </summary>
    /// <param name="stateMachine">The lifecycle state machine (Req 3.3).</param>
    /// <param name="contractBinder">The port that binds payloads to known contracts (Req 4.5, 4.6).</param>
    /// <param name="handlerInvoker">The port that dispatches the bound message to the handler.</param>
    /// <param name="idempotencyGate">
    /// The idempotent-consume gate run before the handler, or <see langword="null"/> to disable the
    /// gate entirely (Req 7).
    /// </param>
    /// <exception cref="ArgumentNullException">If a required dependency is null.</exception>
    public ConsumePipeline(
        ConsumerHostStateMachine stateMachine,
        IContractBinder contractBinder,
        IHandlerInvoker handlerInvoker,
        IdempotencyGate? idempotencyGate)
    {
        _stateMachine = stateMachine ?? throw new ArgumentNullException(nameof(stateMachine));
        _contractBinder = contractBinder ?? throw new ArgumentNullException(nameof(contractBinder));
        _handlerInvoker = handlerInvoker ?? throw new ArgumentNullException(nameof(handlerInvoker));
        _idempotencyGate = idempotencyGate;
    }

    /// <inheritdoc />
    public ConsumerHostState State => _stateMachine.State;

    /// <inheritdoc />
    /// <exception cref="InvalidConsumerHostStateException">If the host is not currently Stopped.</exception>
    public Task StartAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        // Stopped -> Starting: no longer at rest, not yet accepting messages.
        _stateMachine.BeginStart();

        // The transport subscription/consume loop is established by the provider in a later spec; the
        // core host has no loop of its own to start here. Advance to Running once startup completes.
        // Starting -> Running: now accepting delivered messages.
        _stateMachine.CompleteStart();

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    /// <exception cref="InvalidConsumerHostStateException">If the host is not currently Running.</exception>
    public Task StopAsync(CancellationToken cancellationToken)
    {
        // Running -> Stopping: stop accepting new messages; drain in-flight (Req 10.5).
        _stateMachine.BeginStop();

        // Graceful drain within the configured shutdown timeout is layered on in task 18; the core
        // host completes the stop once draining is done.
        // Stopping -> Stopped: back to the terminal resting state.
        _stateMachine.CompleteStop();

        return Task.CompletedTask;
    }

    /// <summary>
    /// Runs the core consume pipeline for a single received envelope: evaluate the idempotency gate
    /// (when configured), bind the payload to a known contract, then invoke the registered handler
    /// with the contract-bound message and the idempotency context (Req 4.5, 4.6, 7). The
    /// transport-specific consume loop (a later spec) calls this for each message it pulls.
    /// </summary>
    /// <param name="envelope">The received envelope.</param>
    /// <param name="cancellationToken">Token to cancel processing.</param>
    /// <exception cref="ArgumentNullException">If <paramref name="envelope"/> is null.</exception>
    /// <exception cref="InvalidConsumerHostStateException">
    /// If the host is not <see cref="ConsumerHostState.Running"/> (not accepting messages — Req 10.5).
    /// </exception>
    /// <exception cref="MissingIdempotencyKeyException">
    /// If idempotency is enabled and the key cannot be derived; the handler is not invoked and no side
    /// effects run (Req 7.6).
    /// </exception>
    /// <exception cref="UnknownContractException">
    /// If the payload cannot be bound to a known contract; the handler is not invoked and the payload
    /// is not delivered to the consumer (Req 4.6).
    /// </exception>
    public async Task DeliverAsync(MessageEnvelope envelope, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(envelope);

        if (_stateMachine.State != ConsumerHostState.Running)
        {
            // A host that is Stopped/Starting/Stopping does not accept new messages (Req 10.5).
            throw new InvalidConsumerHostStateException(_stateMachine.State, "deliver a message to");
        }

        cancellationToken.ThrowIfCancellationRequested();

        // Stage 1: idempotency gate (when configured). An underivable key is rejected here before any
        // side effects (Req 7.6); a known duplicate is acknowledged WITHOUT invoking the handler
        // (Req 7.2, 7.4). Running the gate first keeps the "before side effects" ordering exact.
        var context = new ConsumeContext(IdempotencyKey: null, IsDuplicate: false);
        if (_idempotencyGate is not null)
        {
            var decision = await _idempotencyGate
                .EvaluateAsync(envelope, cancellationToken)
                .ConfigureAwait(false);

            context = decision.Context;

            if (!decision.ShouldInvokeHandler)
            {
                // Duplicate: acknowledge by returning without executing the handler side effects
                // (Req 7.2, 7.4). The transport loop treats a normal return as an acknowledgement.
                return;
            }
        }

        // Stage 2: bind the untrusted payload to its KNOWN contract. Unknown/unbindable => reject
        // before the handler (Req 4.5, 4.6); additive fields are tolerated (Req 4.7).
        var boundMessage = _contractBinder.Bind(envelope);

        // Stage 3: invoke the handler with the bound message, the original envelope (correlation id
        // preserved end-to-end — Req 9.4), and the idempotency context exposing the key (Req 6.8, 7.1).
        await _handlerInvoker
            .InvokeAsync(envelope, boundMessage, context, cancellationToken)
            .ConfigureAwait(false);
    }
}
