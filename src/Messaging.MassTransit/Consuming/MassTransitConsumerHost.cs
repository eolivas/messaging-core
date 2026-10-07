using Messaging.Abstractions;
using Messaging.Consuming;
using Messaging.Contracts;
using Messaging.MassTransit.Transport;
using Microsoft.Extensions.Logging;

namespace Messaging.MassTransit.Consuming;

/// <summary>
/// The MassTransit consumer host shell. It wraps the MassTransit bus start/stop (design:
/// "<c>ConsumerHost</c> wraps the MassTransit bus start/stop") and drives the lifecycle
/// Stopped → Starting → Running → Stopping → Stopped (Req 3.3). Each message MassTransit delivers is
/// run through <see cref="IMassTransitConsumeProcessor"/> — the single seam where the core's
/// correlation / idempotency / classification / dead-letter steps run ahead of the handler, so a
/// message over a MassTransit transport behaves exactly as in the other modes.
/// </summary>
/// <remarks>
/// <para>
/// Lifecycle (Req 3.3) is delegated to the Application-layer <see cref="ConsumerHostStateMachine"/>:
/// the host owns no second state machine (SRP). <see cref="StartAsync"/> starts the bus and advances
/// to Running; <see cref="StopAsync"/> stops accepting new messages (Req 10.5) and stops the bus,
/// which drains MassTransit's in-flight consumers.
/// </para>
/// <para>
/// This is an adapter shell: MassTransit's consumer topology, filter stack, and the concrete
/// <c>IConsumer</c> registrations behind the bus are owned by the provider's own later spec. The shell
/// expresses the "start bus → deliver through core steps → stop bus" shape and keeps
/// <see cref="DeliverAsync"/> directly drivable in a test, standing in for the MassTransit consumer
/// handing a message to the core steps.
/// </para>
/// </remarks>
public sealed class MassTransitConsumerHost : IMessageConsumerHost
{
    private readonly IMassTransitBusAdapter _bus;
    private readonly IMassTransitConsumeProcessor _processor;
    private readonly ILogger<MassTransitConsumerHost> _logger;
    private readonly ConsumerHostStateMachine _stateMachine = new();

    /// <summary>
    /// Creates the MassTransit consumer host (constructor injection only).
    /// </summary>
    /// <param name="bus">The seam over the configured MassTransit bus the host starts and stops.</param>
    /// <param name="processor">The core consume-steps seam each delivered message is run through.</param>
    /// <param name="logger">Structured logger for the consume lifecycle.</param>
    /// <exception cref="ArgumentNullException">If any dependency is null.</exception>
    public MassTransitConsumerHost(
        IMassTransitBusAdapter bus,
        IMassTransitConsumeProcessor processor,
        ILogger<MassTransitConsumerHost> logger)
    {
        _bus = bus ?? throw new ArgumentNullException(nameof(bus));
        _processor = processor ?? throw new ArgumentNullException(nameof(processor));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc />
    public ConsumerHostState State => _stateMachine.State;

    /// <inheritdoc />
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        // Stopped -> Starting: not yet accepting messages.
        _stateMachine.BeginStart();

        try
        {
            await _bus.StartBusAsync(cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            // Startup failed: unwind the state machine back toward a resting state so the host can be
            // retried, then surface the failure fail-fast.
            _stateMachine.CompleteStart();
            _stateMachine.BeginStop();
            _stateMachine.CompleteStop();
            throw;
        }

        // Starting -> Running: the bus is up and MassTransit consumers are accepting messages.
        _stateMachine.CompleteStart();

        _logger.LogInformation("MassTransit consumer host started; bus running and accepting messages.");
    }

    /// <inheritdoc />
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        // Running -> Stopping: stop accepting new messages; MassTransit drains in-flight (Req 10.5).
        _stateMachine.BeginStop();

        await _bus.StopBusAsync(cancellationToken).ConfigureAwait(false);

        // Stopping -> Stopped: back to the terminal resting state.
        _stateMachine.CompleteStop();

        _logger.LogInformation("MassTransit consumer host stopped; bus drained and shut down.");
    }

    /// <summary>
    /// Runs a single message MassTransit delivered through the core consume steps. Accepted only while
    /// the host is <see cref="ConsumerHostState.Running"/>; a message delivered when the host is not
    /// running is rejected so a stopped/stopping host does not process new work (Req 10.5). Exposed so
    /// the MassTransit consumer (and a test) can hand one message to the core steps without the full
    /// bus; the real consumer registration that calls this is owned by a later spec.
    /// </summary>
    /// <param name="envelope">The envelope MassTransit delivered.</param>
    /// <param name="cancellationToken">Token to cancel processing.</param>
    /// <exception cref="ArgumentNullException">If <paramref name="envelope"/> is null.</exception>
    /// <exception cref="InvalidConsumerHostStateException">If the host is not Running (Req 10.5).</exception>
    public async Task DeliverAsync(MessageEnvelope envelope, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(envelope);

        if (_stateMachine.State != ConsumerHostState.Running)
        {
            // A host that is Stopped/Starting/Stopping does not accept new messages (Req 10.5).
            throw new InvalidConsumerHostStateException(_stateMachine.State, "deliver a message to");
        }

        cancellationToken.ThrowIfCancellationRequested();

        await _processor.ProcessAsync(envelope, cancellationToken).ConfigureAwait(false);
    }
}
