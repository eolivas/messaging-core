using Messaging.Abstractions;
using Messaging.Contracts;
using Microsoft.Extensions.Logging;

namespace Messaging.Kafka.Streaming;

/// <summary>
/// The Streaming (Kafka) publisher shell: validates the envelope's invariants, then produces it to
/// the configured topic through <see cref="IKafkaProducer"/> (Req 3.1, 3.2, 4.4). Returns a failed
/// <see cref="PublishResult"/> and leaves the envelope undelivered when the envelope is invalid or the
/// produce fails. The concrete produce mechanics live behind the shallow producer port and are owned
/// by a later spec.
/// </summary>
public sealed class KafkaMessagePublisher : IMessagePublisher
{
    private readonly IKafkaProducer _producer;
    private readonly KafkaProviderSettings _settings;
    private readonly ILogger<KafkaMessagePublisher> _logger;

    /// <summary>
    /// Creates the publisher over the Kafka producer seam (constructor injection only).
    /// </summary>
    /// <param name="producer">The seam that produces records to a topic.</param>
    /// <param name="settings">The Kafka provider settings (topic to produce to).</param>
    /// <param name="logger">Structured logger for publish lifecycle events.</param>
    /// <exception cref="ArgumentNullException">If any dependency is null.</exception>
    public KafkaMessagePublisher(
        IKafkaProducer producer,
        KafkaProviderSettings settings,
        ILogger<KafkaMessagePublisher> logger)
    {
        _producer = producer ?? throw new ArgumentNullException(nameof(producer));
        _settings = settings ?? throw new ArgumentNullException(nameof(settings));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        _settings.Validate();
    }

    /// <inheritdoc />
    public async Task<PublishResult> PublishAsync(MessageEnvelope envelope, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(envelope);

        try
        {
            envelope.Validate();
        }
        catch (InvalidEnvelopeException ex)
        {
            // Invalid envelope: leave it undelivered and report the failure (Req 3.2, 4.3). The
            // correlation id is a non-sensitive identifier and safe to log; the payload never is.
            _logger.LogWarning(
                ex,
                "Kafka publish rejected an invalid envelope for correlation id {CorrelationId}.",
                envelope.CorrelationId);
            return PublishResult.Fail(ex.Message);
        }

        try
        {
            await _producer.ProduceAsync(_settings.Topic, envelope, cancellationToken).ConfigureAwait(false);
            _logger.LogDebug(
                "Kafka publish produced envelope for correlation id {CorrelationId} to topic {Topic}.",
                envelope.CorrelationId,
                _settings.Topic);
            return PublishResult.Ok();
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            // Produce failed: the message is undelivered (Req 3.2).
            _logger.LogError(
                ex,
                "Kafka publish failed to produce envelope for correlation id {CorrelationId} to topic {Topic}.",
                envelope.CorrelationId,
                _settings.Topic);
            return PublishResult.Fail($"Kafka produce failed: {ex.Message}");
        }
    }
}
