using Messaging.Abstractions;
using Messaging.Contracts;
using Messaging.MassTransit.Transport;
using Microsoft.Extensions.Logging;

namespace Messaging.MassTransit.Publishing;

/// <summary>
/// The MassTransit publish side of the adapter: an <see cref="IMessagePublisher"/> that delegates to
/// MassTransit's publish endpoint (Publish-Subscribe fan-out) or send endpoint (Message Queue
/// point-to-point) via <see cref="IMassTransitBusAdapter"/> (Req 3.4). Envelope invariants are already
/// enforced by the Application-layer publish pipeline before an envelope reaches here, so this adapter
/// only hands the envelope to the transport and maps the outcome to a <see cref="PublishResult"/>.
/// </summary>
/// <remarks>
/// A transport-level failure is surfaced as a failed <see cref="PublishResult"/> (not an exception) so
/// the message is reported undelivered without leaking MassTransit internals to the caller. The
/// correlation id is logged for traceability; the payload is never logged (code conventions: never log
/// raw payloads).
/// </remarks>
public sealed class MassTransitPublisher : IMessagePublisher
{
    private readonly IMassTransitBusAdapter _bus;
    private readonly ILogger<MassTransitPublisher> _logger;

    /// <summary>
    /// Creates the MassTransit publisher.
    /// </summary>
    /// <param name="bus">The seam over the configured MassTransit bus the publisher delegates to.</param>
    /// <param name="logger">The structured logger (code conventions: structured logging only).</param>
    /// <exception cref="ArgumentNullException">If any dependency is null.</exception>
    public MassTransitPublisher(IMassTransitBusAdapter bus, ILogger<MassTransitPublisher> logger)
    {
        _bus = bus ?? throw new ArgumentNullException(nameof(bus));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc />
    /// <exception cref="ArgumentNullException">If <paramref name="envelope"/> is null.</exception>
    public async Task<PublishResult> PublishAsync(MessageEnvelope envelope, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(envelope);
        cancellationToken.ThrowIfCancellationRequested();

        try
        {
            await _bus.PublishAsync(envelope, cancellationToken).ConfigureAwait(false);
            return PublishResult.Ok();
        }
        catch (OperationCanceledException)
        {
            // Cancellation is a caller signal, not a transport failure — let it propagate.
            throw;
        }
        catch (Exception ex)
        {
            // Transport rejected or could not accept the message: leave it undelivered (Req 3.2).
            _logger.LogWarning(
                ex,
                "MassTransit publish failed for message {MessageId} (correlation {CorrelationId}).",
                envelope.MessageId,
                envelope.CorrelationId);

            return PublishResult.Fail($"MassTransit publish failed: {ex.Message}");
        }
    }
}
