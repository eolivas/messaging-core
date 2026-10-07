using Messaging.Abstractions;
using Messaging.Contracts;
using Messaging.Kafka.Streaming;

namespace Messaging.Kafka.Tests;

/// <summary>
/// Shared test helpers for the Kafka adapter shell tests: a valid settings factory, a valid envelope
/// builder, and in-memory fakes for the seam ports (<see cref="IKafkaProducer"/>,
/// <see cref="IKafkaConsumer"/>, <see cref="IStreamMessageProcessor"/>, <see cref="IKafkaTransportProbe"/>).
/// The fakes record what they were asked to do so a test can assert the consume → process → commit
/// ordering without a live broker.
/// </summary>
internal static class KafkaTestSupport
{
    public static KafkaProviderSettings ValidSettings() => new()
    {
        BootstrapServers = "localhost:9092",
        ConsumerGroupId = "test-group",
        Topic = "test-topic",
    };

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

/// <summary>Records produced envelopes; optionally throws to simulate a produce failure.</summary>
internal sealed class FakeKafkaProducer : IKafkaProducer
{
    private readonly Exception? _throwOnProduce;

    public FakeKafkaProducer(Exception? throwOnProduce = null) => _throwOnProduce = throwOnProduce;

    public List<(string Topic, MessageEnvelope Envelope)> Produced { get; } = new();

    public Task ProduceAsync(string topic, MessageEnvelope envelope, CancellationToken cancellationToken)
    {
        if (_throwOnProduce is not null)
        {
            throw _throwOnProduce;
        }

        Produced.Add((topic, envelope));
        return Task.CompletedTask;
    }
}

/// <summary>
/// Serves a queued sequence of records, then empty polls. Records every subscribe/commit/close and
/// the order in which process-vs-commit happened (via an external action set by the test).
/// </summary>
internal sealed class FakeKafkaConsumer : IKafkaConsumer
{
    private readonly Queue<KafkaConsumeResult> _records;

    public FakeKafkaConsumer(params KafkaConsumeResult[] records) => _records = new Queue<KafkaConsumeResult>(records);

    public string? SubscribedTopic { get; private set; }
    public List<KafkaConsumeResult> Committed { get; } = new();
    public bool Closed { get; private set; }

    /// <summary>Invoked on each commit so a test can record relative ordering against processing.</summary>
    public Action<KafkaConsumeResult>? OnCommit { get; set; }

    public void Subscribe(string topic) => SubscribedTopic = topic;

    public Task<KafkaConsumeResult?> ConsumeAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(_records.Count > 0 ? _records.Dequeue() : null);
    }

    public Task CommitAsync(KafkaConsumeResult result, CancellationToken cancellationToken)
    {
        Committed.Add(result);
        OnCommit?.Invoke(result);
        return Task.CompletedTask;
    }

    public void Close() => Closed = true;
}

/// <summary>Records processed envelopes; optionally throws to simulate a pipeline failure.</summary>
internal sealed class FakeStreamMessageProcessor : IStreamMessageProcessor
{
    private readonly Exception? _throwOnProcess;

    public FakeStreamMessageProcessor(Exception? throwOnProcess = null) => _throwOnProcess = throwOnProcess;

    public List<MessageEnvelope> Processed { get; } = new();

    /// <summary>Invoked on each process so a test can record relative ordering against commit.</summary>
    public Action<MessageEnvelope>? OnProcess { get; set; }

    public Task ProcessAsync(MessageEnvelope envelope, CancellationToken cancellationToken)
    {
        if (_throwOnProcess is not null)
        {
            throw _throwOnProcess;
        }

        Processed.Add(envelope);
        OnProcess?.Invoke(envelope);
        return Task.CompletedTask;
    }
}

/// <summary>A transport probe returning a fixed, test-controlled health.</summary>
internal sealed class FakeKafkaTransportProbe : IKafkaTransportProbe
{
    private readonly TransportHealth _health;

    public FakeKafkaTransportProbe(TransportHealth health) => _health = health;

    public Task<TransportHealth> ProbeAsync(CancellationToken cancellationToken) => Task.FromResult(_health);
}
