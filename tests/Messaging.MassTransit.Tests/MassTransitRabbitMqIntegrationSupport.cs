using System.Collections.Concurrent;
using System.Text;
using Messaging.Abstractions;
using Messaging.Contracts;
using Messaging.MassTransit.Configuration;
using Messaging.MassTransit.Consuming;
using Messaging.MassTransit.Transport;
using Messaging.Observability;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Client;

namespace Messaging.MassTransit.Tests;

/// <summary>
/// Test-local wiring for the MassTransit-over-RabbitMQ round-trip + dead-letter integration test
/// (task 22.2 / spec task 9). The production <see cref="Messaging.MassTransit"/> package now owns the
/// wire contract (<see cref="EnvelopeMessage"/>), the consume bridge (<see cref="EnvelopeConsumer"/>),
/// and the bus seam (<see cref="RabbitMqBusAdapter"/>), so the integration test exercises the real
/// adapter rather than a duplicated test copy.
/// </summary>
/// <remarks>
/// <para>
/// This file keeps only what is genuinely test scaffolding: the capturing dead-letter sink the test
/// asserts against, the no-op observability sinks the real adapter needs but whose emission is unit
/// tested elsewhere (task 13.5), and the envelope factory that builds a stable, fully-populated
/// envelope so the round-trip assertion can compare every field end to end.
/// </para>
/// <para>
/// <see cref="IntegrationBus.Create"/> adapts the production
/// <see cref="RabbitMqBusAdapter.Create(MassTransitProviderSettings, MassTransitRole, MessagingMode, IMassTransitConsumeProcessor?, ObservabilityContext, Microsoft.Extensions.Logging.ILogger{RabbitMqBusAdapter}, string?)"/>
/// factory to the shape the test needs: a combined publish/consume role in Publish-Subscribe mode,
/// pointed at the live Testcontainer broker, hosting the real <see cref="EnvelopeConsumer"/> on a
/// unique receive endpoint that forwards each envelope to the supplied core consume-steps seam.
/// </para>
/// </remarks>
internal static class IntegrationBus
{
    /// <summary>
    /// Builds the production <see cref="RabbitMqBusAdapter"/> against the live broker at
    /// <paramref name="connectionString"/>, registering the real <see cref="EnvelopeConsumer"/> on the
    /// <paramref name="receiveQueueName"/> receive endpoint and forwarding each delivered envelope to
    /// <paramref name="processor"/>. The role is <see cref="MassTransitRole.Both"/> (publish and
    /// consume) in <see cref="MessagingMode.PublishSubscribe"/> mode so a published envelope fans out to
    /// the receive queue, matching the round-trip the test asserts.
    /// </summary>
    public static RabbitMqBusAdapter Create(
        string connectionString,
        string receiveQueueName,
        IMassTransitConsumeProcessor processor)
    {
        var settings = new MassTransitProviderSettings(
            MassTransitTransport.RabbitMq,
            connectionString,
            host: "testcontainer");

        return RabbitMqBusAdapter.Create(
            settings,
            MassTransitRole.Both,
            MessagingMode.PublishSubscribe,
            processor,
            NoOpObservability(),
            NullLogger<RabbitMqBusAdapter>.Instance,
            receiveQueueName);
    }

    /// <summary>
    /// Builds an <see cref="ObservabilityContext"/> wired to no-op sinks: the adapter requires the
    /// facade, but observability emission is unit tested elsewhere (task 13.5), so here it must not
    /// affect the message outcome the integration test asserts.
    /// </summary>
    private static ObservabilityContext NoOpObservability() => new(
        new NoOpMessagingLog(),
        new NoOpMessagingMetrics(),
        new MessagingTraceSource(tracingEnabled: false),
        new PayloadRedactor());
}

/// <summary>
/// Reads broker-side queue state directly over AMQP so a test can assert, against the <b>real</b>
/// broker, that a dead-lettered poison message no longer sits in the receive queue — i.e. it is not
/// reported in-flight (Req 11.5). A passive queue declare returns the broker's own count of messages
/// in the ready state for that queue without creating or mutating it; a drained queue reports zero.
/// </summary>
/// <remarks>
/// Uses a one-off connection and channel per query (the RabbitMQ guidance for passive declares, since
/// a failed passive declare faults the channel). The broker is reached only through the live,
/// throwaway Testcontainer connection string the fixture already surfaces — no secret is committed.
/// </remarks>
internal static class BrokerQueueInspector
{
    /// <summary>
    /// Returns the number of messages in the ready state in <paramref name="queueName"/> on the broker
    /// at <paramref name="connectionString"/>, as reported by a passive queue declare. A ready count of
    /// zero after a dead-letter-and-acknowledge means the poison message has left the queue and is not
    /// awaiting (re)delivery.
    /// </summary>
    /// <param name="connectionString">The live AMQP connection string for the Testcontainer broker.</param>
    /// <param name="queueName">The receive queue the consumer endpoint is bound to.</param>
    /// <param name="cancellationToken">Token to cancel the broker round trip.</param>
    public static async Task<uint> GetReadyMessageCountAsync(
        string connectionString,
        string queueName,
        CancellationToken cancellationToken)
    {
        var factory = new ConnectionFactory { Uri = new Uri(connectionString) };

        await using var connection = await factory.CreateConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken).ConfigureAwait(false);

        var declareOk = await channel.QueueDeclarePassiveAsync(queueName, cancellationToken).ConfigureAwait(false);
        return declareOk.MessageCount;
    }
}

/// <summary>
/// An in-memory <see cref="IDeadLetterSink"/> that captures every dead-lettered message so the test
/// can assert routing happened, when it happened, and what context was preserved. A monotonic clock
/// (<see cref="DateTimeOffset.UtcNow"/>) stamps each capture so the test can bound the elapsed time
/// from the final failed attempt to the dead-letter (Req 8.1 — within 5s).
/// </summary>
public sealed class CapturingDeadLetterSink : IDeadLetterSink
{
    private readonly ConcurrentQueue<(DeadLetterMessage Message, DateTimeOffset AtUtc)> _captured = new();
    private readonly TaskCompletionSource _firstCapture =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>Every dead-letter captured, in arrival order, with the UTC instant it was received.</summary>
    public IReadOnlyCollection<(DeadLetterMessage Message, DateTimeOffset AtUtc)> Captured =>
        _captured.ToArray();

    /// <summary>Completes once the first message reaches the sink, so a test can await the dead-letter.</summary>
    public Task FirstCapture => _firstCapture.Task;

    public Task SendAsync(DeadLetterMessage message, CancellationToken cancellationToken)
    {
        _captured.Enqueue((message, DateTimeOffset.UtcNow));
        _firstCapture.TrySetResult();
        return Task.CompletedTask;
    }
}

/// <summary>A no-op structured message-event log: the structured fields are unit-tested elsewhere (task 13.5).</summary>
internal sealed class NoOpMessagingLog : IMessagingLog
{
    public void MessageEvent(MessagingEventType eventType, string correlationId, DateTimeOffset timestampUtc)
    {
        // Intentionally empty.
    }
}

/// <summary>A no-op message-lifecycle metric counter: the counters are unit-tested elsewhere (task 13.5).</summary>
internal sealed class NoOpMessagingMetrics : IMessagingMetrics
{
    public void RecordPublished()
    {
        // Intentionally empty.
    }

    public void RecordConsumed()
    {
        // Intentionally empty.
    }

    public void RecordRetry()
    {
        // Intentionally empty.
    }

    public void RecordDeadLetter()
    {
        // Intentionally empty.
    }
}

/// <summary>A no-op dead-letter metric counter: the metric emission is unit-tested elsewhere (task 13.5).</summary>
internal sealed class NoOpDeadLetterMetrics : IDeadLetterMetrics
{
    public void RecordDeadLetter()
    {
        // Intentionally empty.
    }
}

/// <summary>A no-op structured dead-letter log: structured fields are unit-tested elsewhere (task 13.5).</summary>
internal sealed class NoOpDeadLetterLog : IDeadLetterLog
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

        // Multiple, distinct headers so the round-trip's full-collection assertion is meaningful:
        // it must prove count, keys, and values all survive — not just that a single known key does.
        Headers = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["tenant"] = "acme",
            ["trace-id"] = Guid.NewGuid().ToString("N"),
            ["origin"] = "integration-test",
        },
        Payload = new ReadOnlyMemory<byte>(payload),
    };

    public static byte[] Utf8(string value) => Encoding.UTF8.GetBytes(value);
}
