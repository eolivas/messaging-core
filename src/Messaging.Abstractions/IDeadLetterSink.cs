namespace Messaging.Abstractions;

/// <summary>
/// The destination for messages that exhaust the retry policy or are classified as poison
/// (Req 6.6, 8).
/// </summary>
public interface IDeadLetterSink
{
    /// <summary>
    /// Sends a dead-lettered message to the sink, preserving the original envelope and failure
    /// context (Req 8.2).
    /// </summary>
    /// <param name="message">The dead-letter message to send.</param>
    /// <param name="cancellationToken">Token to cancel the send.</param>
    Task SendAsync(DeadLetterMessage message, CancellationToken cancellationToken);
}
