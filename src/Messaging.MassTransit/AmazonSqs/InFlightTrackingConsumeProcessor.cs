using Messaging.Contracts;
using Messaging.Lifecycle;
using Messaging.MassTransit.Consuming;

namespace Messaging.MassTransit.AmazonSqs;

/// <summary>
/// Decorates the inner consume processor (in production <see cref="CoreConsumeProcessor"/>) so each
/// delivery is counted as in-flight and is rejected once the consumer stops accepting work (Req 12.4,
/// 12.6). MassTransit delivers straight to the consumer, bypassing the shell's state-machine gate, so
/// this decorator is where the shutdown drain sees these deliveries.
/// </summary>
public sealed class InFlightTrackingConsumeProcessor : IMassTransitConsumeProcessor
{
    private readonly IMassTransitConsumeProcessor _inner;
    private readonly InFlightTracker _tracker;

    /// <summary>Creates the decorator.</summary>
    /// <param name="inner">The processor that runs the core consume steps.</param>
    /// <param name="tracker">The in-flight tracker the shutdown coordinator drains against.</param>
    /// <exception cref="ArgumentNullException">Thrown when an argument is <c>null</c>.</exception>
    public InFlightTrackingConsumeProcessor(IMassTransitConsumeProcessor inner, InFlightTracker tracker)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        _tracker = tracker ?? throw new ArgumentNullException(nameof(tracker));
    }

    /// <inheritdoc />
    /// <exception cref="MessageDeliveryRejectedException">
    /// Thrown when the tracker is no longer accepting; the inner processor is not called.
    /// </exception>
    public async Task ProcessAsync(MessageEnvelope envelope, CancellationToken cancellationToken)
    {
        if (!_tracker.TryBeginProcessing())
        {
            throw new MessageDeliveryRejectedException();
        }

        try
        {
            await _inner.ProcessAsync(envelope, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _tracker.CompleteProcessing();
        }
    }
}
