using System.Collections.Concurrent;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace Messaging.Kafka.Streaming;

/// <summary>
/// Production native Confluent.Kafka consumer behind <see cref="IKafkaConsumer"/>. Auto-commit is
/// disabled (via <see cref="KafkaClientConfigFactory.CreateConsumerConfig"/>), so the host owns commit
/// timing. Each polled record's <see cref="ConsumeResult{TKey,TValue}"/> is retained so
/// <see cref="CommitAsync"/> commits the exact offset (resume point offset + 1) the host asks for
/// (Req 1.3, 1.4, 4.1, 4.6). Logs and exceptions reference partition/offset only, never payloads or
/// credentials (Req 10.3, 12.10).
/// </summary>
public sealed class ConfluentKafkaConsumer : IKafkaConsumer, IDisposable
{
    /// <summary>Default poll timeout (Req 4.4).</summary>
    public static readonly TimeSpan DefaultPollTimeout = TimeSpan.FromSeconds(5);

    /// <summary>Minimum permitted poll timeout (Req 4.4).</summary>
    public static readonly TimeSpan MinPollTimeout = TimeSpan.FromSeconds(1);

    /// <summary>Maximum permitted poll timeout (Req 4.4).</summary>
    public static readonly TimeSpan MaxPollTimeout = TimeSpan.FromSeconds(300);

    private readonly IConsumer<Ignore, byte[]> _consumer;
    private readonly TimeSpan _pollTimeout;
    private readonly ILogger<ConfluentKafkaConsumer> _logger;

    // Reference-keyed: the same KafkaConsumeResult instance handed to the host is the commit handle.
    private readonly ConcurrentDictionary<KafkaConsumeResult, ConsumeResult<Ignore, byte[]>> _inFlight =
        new(ReferenceEqualityComparer.Instance);

    /// <summary>Builds the consumer from the factory's manual-commit configuration.</summary>
    /// <param name="configFactory">Builds the consumer config (group, manual commit, security).</param>
    /// <param name="logger">Logger; receives partition/offset only.</param>
    /// <param name="pollTimeout">Poll timeout, 1 to 300 seconds inclusive; default 5 seconds.</param>
    /// <exception cref="ArgumentOutOfRangeException">If <paramref name="pollTimeout"/> is outside 1..300s.</exception>
    public ConfluentKafkaConsumer(
        KafkaClientConfigFactory configFactory,
        ILogger<ConfluentKafkaConsumer> logger,
        TimeSpan? pollTimeout = null)
    {
        ArgumentNullException.ThrowIfNull(configFactory);
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        var timeout = pollTimeout ?? DefaultPollTimeout;
        if (timeout < MinPollTimeout || timeout > MaxPollTimeout)
        {
            throw new ArgumentOutOfRangeException(
                nameof(pollTimeout),
                "Poll timeout must be between 1 and 300 seconds inclusive.");
        }

        _pollTimeout = timeout;
        _consumer = new ConsumerBuilder<Ignore, byte[]>(configFactory.CreateConsumerConfig()).Build();
    }

    /// <inheritdoc />
    public void Subscribe(string topic)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        _consumer.Subscribe(topic);
    }

    /// <inheritdoc />
    public Task<KafkaConsumeResult?> ConsumeAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var consumeResult = _consumer.Consume(_pollTimeout);
        cancellationToken.ThrowIfCancellationRequested();

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

    /// <inheritdoc />
    public Task CommitAsync(KafkaConsumeResult result, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(result);
        cancellationToken.ThrowIfCancellationRequested();

        if (!_inFlight.TryRemove(result, out var consumeResult))
        {
            throw new KafkaOffsetCommitException(result.Partition, result.Offset);
        }

        try
        {
            // Stores offset + 1 as the partition's resume point (Req 4.6).
            _consumer.Commit(consumeResult);
        }
        catch (Exception ex) when (ex is KafkaException or TopicPartitionOffsetException)
        {
            _logger.LogWarning(
                "Kafka offset commit failed for partition {Partition}, offset {Offset} ({ExceptionType}).",
                result.Partition,
                result.Offset,
                ex.GetType().Name);
            throw new KafkaOffsetCommitException(result.Partition, result.Offset, ex);
        }

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public void Close() => _consumer.Close();

    /// <summary>Releases the underlying consumer.</summary>
    public void Dispose() => _consumer.Dispose();
}
