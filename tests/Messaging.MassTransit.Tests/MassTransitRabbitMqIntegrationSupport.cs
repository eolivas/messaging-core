using System.Collections.Concurrent;
using System.Text;
using global::MassTransit;
using Messaging.Contracts;
using Messaging.MassTransit.Consuming;
using Messaging.MassTransit.Transport;

namespace Messaging.MassTransit.Tests;

/// <summary>
/// Native MassTransit-over-RabbitMQ wiring for the seam ports, used only by the MassTransit
/// round-trip + dead-letter integration test (task 22.2). The adapter package deliberately leaves
/// <see cref="IMassTransitBusAdapter"/> and <see cref="IMassTransitConsumeProcessor"/> as shallow
/// ports — the concrete bus + consumer filter stack is owned by a later spec — so to exercise the
/// shells (<see cref="Publishing.MassTransitPublisher"/>, <see cref="MassTransitConsumerHost"/>)
/// against a <b>real</b> broker, the test provides those concrete implementations here.
/// </summary>
/// <remarks>
/// <para>
/// <b>Envelope on the wire.</b> The transport-neutral <see cref="MessageEnvelope"/> is carried as a
/// MassTransit message contract (<see cref="EnvelopeMessage"/>): the payload bytes travel verbatim
/// and every other field (message id, correlation id, message type, created-at, contract version,
/// and any custom headers) rides alongside, so the consumer reconstructs the same envelope. Carrying
/// the correlation id is what lets the test assert the round-trip preserves it (Req 4.4).
/// </para>
/// <para>
/// <b>Core steps ahead of the handler.</b> The MassTransit consumer forwards each delivered envelope
/// to the <see cref="IMassTransitConsumeProcessor"/> seam exactly as the design prescribes ("the
/// core's retry classification, idempotency check, and dead-letter routing run inside the MassTransit
/// consumer pipeline"). When the processor throws, the consumer lets MassTransit redeliver; the core
/// <c>RetryExecutor</c> owns the attempt bound and routes to the dead-letter sink on exhaustion, so
/// here the processor is responsible for driving the message to a terminal outcome.
/// </para>
/// </remarks>
public sealed record EnvelopeMessage
{
    public Guid MessageId { get; init; }

    public string CorrelationId { get; init; } = string.Empty;

    public string MessageType { get; init; } = string.Empty;

    public DateTimeOffset CreatedAtUtc { get; init; }

    public int ContractVersion { get; init; } = 1;

    public Dictionary<string, string> Headers { get; init; } = new();

    public byte[] Payload { get; init; } = Array.Empty<byte>();

    public static EnvelopeMessage FromEnvelope(MessageEnvelope envelope) => new()
    {
        MessageId = envelope.MessageId,
        CorrelationId = envelope.CorrelationId,
        MessageType = envelope.MessageType,
        CreatedAtUtc = envelope.CreatedAtUtc,
        ContractVersion = envelope.ContractVersion,
        Headers = new Dictionary<string, string>(envelope.Headers, StringComparer.Ordinal),
        Payload = envelope.Payload.ToArray(),
    };

    public MessageEnvelope ToEnvelope() => new()
    {
        MessageId = MessageId,
        CorrelationId = CorrelationId,
        MessageType = MessageType,
        CreatedAtUtc = CreatedAtUtc,
        ContractVersion = ContractVersion,
        Headers = new Dictionary<string, string>(Headers, StringComparer.Ordinal),
        Payload = new ReadOnlyMemory<byte>(Payload),
    };
}

/// <summary>
/// The MassTransit <see cref="IConsumer{TMessage}"/> that bridges a delivered <see cref="EnvelopeMessage"/>
/// back to the transport-neutral <see cref="MessageEnvelope"/> and runs it through the core
/// consume-steps seam. A processor failure propagates so MassTransit applies its redelivery; the core
/// pipeline inside the processor owns the attempt bound and the eventual dead-letter routing.
/// </summary>
public sealed class EnvelopeConsumer : IConsumer<EnvelopeMessage>
{
    private readonly IMassTransitConsumeProcessor _processor;

    public EnvelopeConsumer(IMassTransitConsumeProcessor processor) =>
        _processor = processor ?? throw new ArgumentNullException(nameof(processor));

    public Task Consume(ConsumeContext<EnvelopeMessage> context) =>
        _processor.ProcessAsync(context.Message.ToEnvelope(), context.CancellationToken);
}

/// <summary>
/// A real <see cref="IMassTransitBusAdapter"/> over a configured MassTransit RabbitMQ
/// <see cref="IBusControl"/>. Start/stop drive the live bus; publish fans the envelope out over
/// RabbitMQ; the reachability probe reflects whether the bus is started. The <see cref="EnvelopeConsumer"/>
/// is registered on a receive endpoint so a published envelope is delivered to the core steps.
/// </summary>
public sealed class RabbitMqBusAdapter : IMassTransitBusAdapter, IAsyncDisposable
{
    private readonly IBusControl _bus;
    private volatile bool _started;

    private RabbitMqBusAdapter(IBusControl bus) => _bus = bus;

    /// <summary>
    /// Builds a MassTransit RabbitMQ bus pointed at <paramref name="connectionString"/> (the live
    /// Testcontainer AMQP endpoint), registering the <see cref="EnvelopeConsumer"/> on a unique
    /// receive endpoint that forwards each envelope to <paramref name="processor"/>.
    /// </summary>
    public static RabbitMqBusAdapter Create(
        string connectionString,
        string receiveQueueName,
        IMassTransitConsumeProcessor processor)
    {
        ArgumentNullException.ThrowIfNull(processor);

        var bus = Bus.Factory.CreateUsingRabbitMq(cfg =>
        {
            cfg.Host(new Uri(connectionString));

            cfg.ReceiveEndpoint(receiveQueueName, endpoint =>
            {
                // No MassTransit-level retry: the core RetryExecutor (driven by the processor) owns
                // the uniform retry/backoff + dead-letter policy. Letting MassTransit redeliver on top
                // would double-count attempts and race the 5s dead-letter window the test asserts.
                endpoint.UseMessageRetry(r => r.None());
                endpoint.Consumer(() => new EnvelopeConsumer(processor));
            });
        });

        return new RabbitMqBusAdapter(bus);
    }

    public async Task StartBusAsync(CancellationToken cancellationToken)
    {
        await _bus.StartAsync(cancellationToken).ConfigureAwait(false);
        _started = true;
    }

    public async Task StopBusAsync(CancellationToken cancellationToken)
    {
        await _bus.StopAsync(cancellationToken).ConfigureAwait(false);
        _started = false;
    }

    public Task PublishAsync(MessageEnvelope envelope, CancellationToken cancellationToken) =>
        _bus.Publish(EnvelopeMessage.FromEnvelope(envelope), cancellationToken);

    public Task<bool> IsTransportReachableAsync(CancellationToken cancellationToken) =>
        Task.FromResult(_started);

    public async ValueTask DisposeAsync()
    {
        if (_started)
        {
            await _bus.StopAsync(CancellationToken.None).ConfigureAwait(false);
        }
    }
}

/// <summary>
/// An in-memory <see cref="IDeadLetterSink"/> that captures every dead-lettered message so the test
/// can assert routing happened, when it happened, and what context was preserved. A monotonic clock
/// (<see cref="DateTimeOffset.UtcNow"/>) stamps each capture so the test can bound the elapsed time
/// from the final failed attempt to the dead-letter (Req 8.1 — within 5s).
/// </summary>
public sealed class CapturingDeadLetterSink : Messaging.Abstractions.IDeadLetterSink
{
    private readonly ConcurrentQueue<(Messaging.Abstractions.DeadLetterMessage Message, DateTimeOffset AtUtc)> _captured = new();
    private readonly TaskCompletionSource _firstCapture =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>Every dead-letter captured, in arrival order, with the UTC instant it was received.</summary>
    public IReadOnlyCollection<(Messaging.Abstractions.DeadLetterMessage Message, DateTimeOffset AtUtc)> Captured =>
        _captured.ToArray();

    /// <summary>Completes once the first message reaches the sink, so a test can await the dead-letter.</summary>
    public Task FirstCapture => _firstCapture.Task;

    public Task SendAsync(Messaging.Abstractions.DeadLetterMessage message, CancellationToken cancellationToken)
    {
        _captured.Enqueue((message, DateTimeOffset.UtcNow));
        _firstCapture.TrySetResult();
        return Task.CompletedTask;
    }
}

/// <summary>A no-op dead-letter metric counter: the metric emission is unit-tested elsewhere (task 13.5).</summary>
internal sealed class NoOpDeadLetterMetrics : Messaging.Observability.IDeadLetterMetrics
{
    public void RecordDeadLetter()
    {
        // Intentionally empty.
    }
}

/// <summary>A no-op structured dead-letter log: structured fields are unit-tested elsewhere (task 13.5).</summary>
internal sealed class NoOpDeadLetterLog : Messaging.Observability.IDeadLetterLog
{
    public void DeadLettered(string correlationId, string failureReason, int attemptCount)
    {
        // Intentionally empty.
    }

    public void DeadLetterRoutingFailed(
        string correlationId,
        string failureReason,
        int attemptCount,
        Exception routingError)
    {
        // Intentionally empty.
    }
}

/// <summary>
/// Builds the test envelope with a stable payload and headers so the round-trip assertion can compare
/// every field end-to-end.
/// </summary>
internal static class EnvelopeFactory
{
    public static MessageEnvelope Create(string correlationId, byte[] payload) => new()
    {
        MessageId = Guid.NewGuid(),
        CorrelationId = correlationId,
        MessageType = "test.contract",
        CreatedAtUtc = DateTimeOffset.UtcNow,
        Headers = new Dictionary<string, string>(StringComparer.Ordinal) { ["tenant"] = "acme" },
        Payload = new ReadOnlyMemory<byte>(payload),
    };

    public static byte[] Utf8(string value) => Encoding.UTF8.GetBytes(value);
}
