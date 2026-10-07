namespace Messaging.Abstractions;

/// <summary>
/// Reads pending outbox entries and publishes them through the <see cref="IMessagePublisher"/>,
/// providing reliable at-least-once delivery of committed entries (Req 5.2–5.6).
/// </summary>
public interface IOutboxDispatcher
{
    /// <summary>
    /// Dispatches every pending outbox entry, marking each as dispatched only after the publisher
    /// confirms publication (Req 5.2, 5.3, 5.6).
    /// </summary>
    /// <param name="cancellationToken">Token to cancel the dispatch pass.</param>
    Task DispatchPendingAsync(CancellationToken cancellationToken);
}
