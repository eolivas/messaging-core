using Messaging.Abstractions;
using Messaging.Contracts;
using Microsoft.Extensions.Logging;

namespace Messaging.Batch.BatchProcessing;

/// <summary>
/// The Batch Processing publisher shell: validates the envelope's invariants, then writes it to the
/// configured batch destination through <see cref="IBatchDestinationWriter"/> (Req 3.1, 3.2, 4.4).
/// Returns a failed <see cref="PublishResult"/> and leaves the envelope undelivered when the envelope
/// is invalid or the destination write fails. The concrete write mechanics live behind the shallow
/// writer port and are owned by a later spec.
/// </summary>
public sealed class BatchMessagePublisher : IMessagePublisher
{
    private readonly IBatchDestinationWriter _destinationWriter;
    private readonly ILogger<BatchMessagePublisher> _logger;

    /// <summary>
    /// Creates the publisher over the batch destination writer (constructor injection only).
    /// </summary>
    /// <param name="destinationWriter">The seam that writes to the batch destination.</param>
    /// <param name="logger">Structured logger for publish lifecycle events.</param>
    /// <exception cref="ArgumentNullException">If any dependency is null.</exception>
    public BatchMessagePublisher(
        IBatchDestinationWriter destinationWriter,
        ILogger<BatchMessagePublisher> logger)
    {
        _destinationWriter = destinationWriter ?? throw new ArgumentNullException(nameof(destinationWriter));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
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
                "Batch publish rejected an invalid envelope for correlation id {CorrelationId}.",
                envelope.CorrelationId);
            return PublishResult.Fail(ex.Message);
        }

        try
        {
            await _destinationWriter.WriteAsync(envelope, cancellationToken).ConfigureAwait(false);
            _logger.LogDebug(
                "Batch publish wrote envelope for correlation id {CorrelationId} to the batch destination.",
                envelope.CorrelationId);
            return PublishResult.Ok();
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            // Transport write failed: the message is undelivered (Req 3.2).
            _logger.LogError(
                ex,
                "Batch publish failed to write envelope for correlation id {CorrelationId} to the batch destination.",
                envelope.CorrelationId);
            return PublishResult.Fail($"Batch destination write failed: {ex.Message}");
        }
    }
}
