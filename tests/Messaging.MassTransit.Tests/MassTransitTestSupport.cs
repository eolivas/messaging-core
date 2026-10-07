using Messaging.Contracts;
using Messaging.MassTransit.Configuration;
using Messaging.MassTransit.Consuming;
using Messaging.MassTransit.Transport;

namespace Messaging.MassTransit.Tests;

/// <summary>
/// Shared test helpers for the MassTransit adapter shell tests: valid settings/envelope factories and
/// in-memory fakes for the seam ports (<see cref="IMassTransitBusAdapter"/>,
/// <see cref="IMassTransitConsumeProcessor"/>). The fakes record what they were asked to do so a test
/// can assert the publish/start/stop/deliver behavior without a live broker.
/// </summary>
internal static class MassTransitTestSupport
{
    public static MassTransitProviderSettings ValidSettings(
        MassTransitTransport transport = MassTransitTransport.RabbitMq) =>
        new(transport, connectionString: "amqp://localhost", host: "localhost");

    public static MessageEnvelope ValidEnvelope(string correlationId = "corr-1") => new()
    {
        MessageId = Guid.NewGuid(),
        CorrelationId = correlationId,
        MessageType = "test.contract",
        CreatedAtUtc = DateTimeOffset.UtcNow,
        Headers = new Dictionary<string, string>(),
        Payload = new ReadOnlyMemory<byte>(new byte[] { 1, 2, 3 }),
    };
}

/// <summary>
/// An in-memory stand-in for the MassTransit bus seam. Records start/stop and published envelopes,
/// and exposes a test-controlled reachability and optional failures so both the happy and the
/// transport-failure paths are reachable.
/// </summary>
internal sealed class FakeMassTransitBusAdapter : IMassTransitBusAdapter
{
    private readonly bool _reachable;
    private readonly Exception? _throwOnPublish;
    private readonly Exception? _throwOnStart;
    private readonly Exception? _throwOnProbe;

    public FakeMassTransitBusAdapter(
        bool reachable = true,
        Exception? throwOnPublish = null,
        Exception? throwOnStart = null,
        Exception? throwOnProbe = null)
    {
        _reachable = reachable;
        _throwOnPublish = throwOnPublish;
        _throwOnStart = throwOnStart;
        _throwOnProbe = throwOnProbe;
    }

    public int StartCount { get; private set; }
    public int StopCount { get; private set; }
    public List<MessageEnvelope> Published { get; } = new();

    public Task StartBusAsync(CancellationToken cancellationToken)
    {
        if (_throwOnStart is not null)
        {
            throw _throwOnStart;
        }

        StartCount++;
        return Task.CompletedTask;
    }

    public Task StopBusAsync(CancellationToken cancellationToken)
    {
        StopCount++;
        return Task.CompletedTask;
    }

    public Task PublishAsync(MessageEnvelope envelope, CancellationToken cancellationToken)
    {
        if (_throwOnPublish is not null)
        {
            throw _throwOnPublish;
        }

        Published.Add(envelope);
        return Task.CompletedTask;
    }

    public Task<bool> IsTransportReachableAsync(CancellationToken cancellationToken)
    {
        if (_throwOnProbe is not null)
        {
            throw _throwOnProbe;
        }

        return Task.FromResult(_reachable);
    }
}

/// <summary>Records processed envelopes; optionally throws to simulate a core-pipeline failure.</summary>
internal sealed class FakeMassTransitConsumeProcessor : IMassTransitConsumeProcessor
{
    private readonly Exception? _throwOnProcess;

    public FakeMassTransitConsumeProcessor(Exception? throwOnProcess = null) => _throwOnProcess = throwOnProcess;

    public List<MessageEnvelope> Processed { get; } = new();

    public Task ProcessAsync(MessageEnvelope envelope, CancellationToken cancellationToken)
    {
        if (_throwOnProcess is not null)
        {
            throw _throwOnProcess;
        }

        Processed.Add(envelope);
        return Task.CompletedTask;
    }
}

/// <summary>A transport probe returning a fixed, test-controlled health.</summary>
internal sealed class FakeMassTransitTransportProbe : IMassTransitTransportProbe
{
    private readonly Messaging.Abstractions.TransportHealth _health;

    public FakeMassTransitTransportProbe(Messaging.Abstractions.TransportHealth health) => _health = health;

    public Task<Messaging.Abstractions.TransportHealth> ProbeAsync(CancellationToken cancellationToken) =>
        Task.FromResult(_health);
}
