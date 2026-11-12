using Confluent.Kafka;
using Messaging.Contracts;
using Microsoft.Extensions.Logging;

namespace Messaging.Kafka.Streaming;

/// <summary>
/// Native Confluent.Kafka producer behind the <see cref="IKafkaProducer"/> seam. Built from
/// <see cref="KafkaClientConfigFactory.CreateProducerConfig"/> (<c>Acks.All</c> + idempotence, Req 1.2).
/// Produces and awaits the delivery report; a produce that is not persisted by all in-sync replicas
/// throws <see cref="KafkaProduceException"/> so the publisher reports an undelivered message.
/// Logs only the correlation id and topic, never payloads or credentials (Req 10.3, 12.10).
/// </summary>
public sealed class ConfluentKafkaProducer : IKafkaProducer, IDisposable
{
    private static readonly TimeSpan FlushTimeout = TimeSpan.FromSeconds(10);

    private readonly IProducer<Null, byte[]> _producer;
    private readonly ILogger<ConfluentKafkaProducer> _logger;
    private int _disposed;

    /// <summary>Creates the producer.</summary>
    /// <param name="configFactory">Builds the producer configuration.</param>
    /// <param name="settings">Provider settings, validated fail-fast.</param>
    /// <param name="logger">Structured logger.</param>
    public ConfluentKafkaProducer(
        KafkaClientConfigFactory configFactory,
        KafkaProviderSettings settings,
        ILogger<ConfluentKafkaProducer> logger)
    {
        ArgumentNullException.ThrowIfNull(configFactory);
        ArgumentNullException.ThrowIfNull(settings);
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        settings.Validate();
        _producer = new ProducerBuilder<Null, byte[]>(configFactory.CreateProducerConfig()).Build();
    }

    /// <inheritdoc />
    public async Task ProduceAsync(string topic, MessageEnvelope envelope, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ArgumentNullException.ThrowIfNull(envelope);

        var message = KafkaWireFormat.ToMessage(envelope);

        DeliveryResult<Null, byte[]> report;
        try
        {
            report = await _producer.ProduceAsync(topic, message, cancellationToken).ConfigureAwait(false);
        }
        catch (ProduceException<Null, byte[]> ex)
        {
            _logger.LogError(
                "Kafka produce failed for correlation id {CorrelationId} on topic {Topic}: {ErrorCode}",
                envelope.CorrelationId, topic, ex.Error.Code);
            throw new KafkaProduceException(topic, $"error code {ex.Error.Code}");
        }

        if (report.Status != PersistenceStatus.Persisted)
        {
            _logger.LogError(
                "Kafka produce not fully acknowledged for correlation id {CorrelationId} on topic {Topic}: {Status}",
                envelope.CorrelationId, topic, report.Status);
            throw new KafkaProduceException(topic, $"persistence status {report.Status}");
        }

        _logger.LogDebug(
            "Kafka produce acknowledged for correlation id {CorrelationId} on topic {Topic}",
            envelope.CorrelationId, topic);
    }

    /// <summary>Flushes outstanding messages, then disposes the native producer.</summary>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        try
        {
            _producer.Flush(FlushTimeout);
        }
        finally
        {
            _producer.Dispose();
        }
    }
}
