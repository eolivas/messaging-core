using System.Collections.Concurrent;
using System.Text;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Messaging.Contracts;
using Messaging.Kafka.Streaming;

namespace Messaging.Kafka.Tests;

/// <summary>
/// Native Confluent.Kafka adapters for the seam ports, used only by the Kafka integration test
/// (task 22.3). The adapter package deliberately leaves <see cref="IKafkaProducer"/> and
/// <see cref="IKafkaConsumer"/> as shallow ports — the concrete Confluent.Kafka wiring is owned by a
/// later spec — so to exercise the shells (<see cref="KafkaMessagePublisher"/>,
/// <see cref="KafkaConsumerHost"/>) against a <b>real</b> broker, the test provides those concrete
/// implementations here.
/// </summary>
/// <remarks>
/// <para>
/// <b>Envelope on the wire.</b> The payload bytes become the Kafka message value; every other
/// envelope field (message id, correlation id, message type, created-at, contract version, and any
/// custom headers) travels as Kafka record headers so the consumer reconstructs the same envelope.
/// Carrying the correlation id as a header is what lets the test assert it propagates unchanged from
/// producer to consumer (Req 9.4).
/// </para>
/// <para>
/// <b>Manual commit.</b> The consumer sets <c>EnableAutoCommit = false</c>, so the broker only
/// advances the committed offset when the host calls <see cref="IKafkaConsumer.CommitAsync"/> — which
/// the host does strictly after the pipeline completes (Req 6.7). A record consumed but not committed
/// is therefore redelivered to the next member of the group.
/// </para>
/// </remarks>
internal static class KafkaWireFormat
{
    public const string MessageIdHeader = "x-message-id";
    public const string CorrelationIdHeader = "x-correlation-id";
    public const string MessageTypeHeader = "x-message-type";
    public const string CreatedAtHeader = "x-created-at-utc";
    public const string ContractVersionHeader = "x-contract-version";

    /// <summary>Reserved header names that carry envelope metadata rather than user headers.</summary>
    private static readonly HashSet<string> ReservedHeaders = new(StringComparer.Ordinal)
    {
        MessageIdHeader,
        CorrelationIdHeader,
        MessageTypeHeader,
        CreatedAtHeader,
        ContractVersionHeader,
    };

    /// <summary>Serializes an envelope into a Kafka message (payload as value, metadata as headers).</summary>
    public static Message<Null, byte[]> ToMessage(MessageEnvelope envelope)
    {
        var headers = new Headers
        {
            { MessageIdHeader, Encoding.UTF8.GetBytes(envelope.MessageId.ToString("D")) },
            { CorrelationIdHeader, Encoding.UTF8.GetBytes(envelope.CorrelationId) },
            { MessageTypeHeader, Encoding.UTF8.GetBytes(envelope.MessageType) },
            { CreatedAtHeader, Encoding.UTF8.GetBytes(envelope.CreatedAtUtc.ToString("O")) },
            { ContractVersionHeader, Encoding.UTF8.GetBytes(envelope.ContractVersion.ToString()) },
        };

        foreach (var (key, value) in envelope.Headers)
        {
            headers.Add(key, Encoding.UTF8.GetBytes(value));
        }

        return new Message<Null, byte[]>
        {
            Value = envelope.Payload.ToArray(),
            Headers = headers,
        };
    }

    /// <summary>Reconstructs an envelope from a consumed Kafka message (inverse of <see cref="ToMessage"/>).</summary>
    public static MessageEnvelope ToEnvelope(Message<Ignore, byte[]> message)
    {
        var userHeaders = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var header in message.Headers)
        {
            if (!ReservedHeaders.Contains(header.Key))
            {
                userHeaders[header.Key] = Encoding.UTF8.GetString(header.GetValueBytes());
            }
        }

        return new MessageEnvelope
        {
            MessageId = Guid.Parse(ReadHeader(message.Headers, MessageIdHeader)),
            CorrelationId = ReadHeader(message.Headers, CorrelationIdHeader),
            MessageType = ReadHeader(message.Headers, MessageTypeHeader),
            CreatedAtUtc = DateTimeOffset.Parse(
                ReadHeader(message.Headers, CreatedAtHeader),
                null,
                System.Globalization.DateTimeStyles.RoundtripKind),
            ContractVersion = int.Parse(ReadHeader(message.Headers, ContractVersionHeader)),
            Headers = userHeaders,
            Payload = new ReadOnlyMemory<byte>(message.Value ?? Array.Empty<byte>()),
        };
    }

    private static string ReadHeader(Headers headers, string key)
    {
        if (!headers.TryGetLastBytes(key, out var bytes))
        {
            throw new InvalidOperationException($"Required Kafka header '{key}' was missing from the record.");
        }

        return Encoding.UTF8.GetString(bytes);
    }
}

/// <summary>
/// A native Confluent.Kafka producer behind the <see cref="IKafkaProducer"/> seam. Produces to the
/// topic and awaits the broker's acknowledgement, so a failed produce surfaces as a thrown exception
/// (the publisher reports it as an undelivered message, Req 3.2).
/// </summary>
internal sealed class ConfluentKafkaProducer : IKafkaProducer, IDisposable
{
    private readonly IProducer<Null, byte[]> _producer;

    public ConfluentKafkaProducer(string bootstrapServers)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = bootstrapServers,
            Acks = Acks.All,
            EnableIdempotence = true,
        };

        _producer = new ProducerBuilder<Null, byte[]>(config).Build();
    }

    public async Task ProduceAsync(string topic, MessageEnvelope envelope, CancellationToken cancellationToken)
    {
        var message = KafkaWireFormat.ToMessage(envelope);
        await _producer.ProduceAsync(topic, message, cancellationToken).ConfigureAwait(false);
    }

    public void Dispose()
    {
        _producer.Flush(TimeSpan.FromSeconds(5));
        _producer.Dispose();
    }
}

/// <summary>
/// A native Confluent.Kafka consumer behind the <see cref="IKafkaConsumer"/> seam, with auto-commit
/// disabled so the host owns commit timing (manual commit after the pipeline, Req 6.7). Each consumed
/// record's underlying <see cref="ConsumeResult{TKey,TValue}"/> is retained so
/// <see cref="CommitAsync"/> commits the exact offset the host asks for.
/// </summary>
internal sealed class ConfluentKafkaConsumer : IKafkaConsumer, IDisposable
{
    private readonly IConsumer<Ignore, byte[]> _consumer;
    private readonly TimeSpan _pollTimeout;
    private readonly ConcurrentDictionary<KafkaConsumeResult, ConsumeResult<Ignore, byte[]>> _inFlight = new();

    public ConfluentKafkaConsumer(string bootstrapServers, string consumerGroupId, TimeSpan? pollTimeout = null)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = consumerGroupId,
            // Manual commit: the host advances the offset only after the pipeline completes (Req 6.7).
            EnableAutoCommit = false,
            // Start at the beginning so a fresh group sees records produced before it joined.
            AutoOffsetReset = AutoOffsetReset.Earliest,
        };

        _consumer = new ConsumerBuilder<Ignore, byte[]>(config).Build();
        _pollTimeout = pollTimeout ?? TimeSpan.FromSeconds(10);
    }

    public void Subscribe(string topic) => _consumer.Subscribe(topic);

    public Task<KafkaConsumeResult?> ConsumeAsync(CancellationToken cancellationToken)
    {
        var consumeResult = _consumer.Consume(_pollTimeout);
        if (consumeResult is null || consumeResult.IsPartitionEOF)
        {
            return Task.FromResult<KafkaConsumeResult?>(null);
        }

        var envelope = KafkaWireFormat.ToEnvelope(consumeResult.Message);
        var result = new KafkaConsumeResult(
            envelope,
            consumeResult.Partition.Value,
            consumeResult.Offset.Value);

        _inFlight[result] = consumeResult;
        return Task.FromResult<KafkaConsumeResult?>(result);
    }

    public Task CommitAsync(KafkaConsumeResult result, CancellationToken cancellationToken)
    {
        if (!_inFlight.TryRemove(result, out var consumeResult))
        {
            throw new InvalidOperationException(
                "CommitAsync was called for a record that was not consumed by this consumer.");
        }

        // Commit the consumed record's offset. Confluent.Kafka stores offset + 1 as the resume point.
        _consumer.Commit(consumeResult);
        return Task.CompletedTask;
    }

    public void Close() => _consumer.Close();

    public void Dispose() => _consumer.Dispose();
}

/// <summary>
/// Minimal admin helper that creates the test topic before producing, so the first produce/consume
/// does not race topic auto-creation.
/// </summary>
internal static class KafkaTopicAdmin
{
    public static async Task CreateTopicAsync(
        string bootstrapServers,
        string topic,
        int partitions = 1,
        CancellationToken cancellationToken = default)
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig
        {
            BootstrapServers = bootstrapServers,
        }).Build();

        try
        {
            await admin.CreateTopicsAsync(new[]
            {
                new TopicSpecification
                {
                    Name = topic,
                    NumPartitions = partitions,
                    ReplicationFactor = 1,
                },
            }).ConfigureAwait(false);
        }
        catch (CreateTopicsException ex) when (ex.Results.All(r => r.Error.Code == ErrorCode.TopicAlreadyExists))
        {
            // Idempotent: a prior test already created the topic.
        }
    }
}
