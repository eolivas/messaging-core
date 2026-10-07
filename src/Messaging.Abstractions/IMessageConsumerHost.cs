namespace Messaging.Abstractions;

/// <summary>
/// Manages the consumption lifecycle for the active mode independently of the underlying
/// transport (Req 3.3, 10.5–10.7).
/// </summary>
public interface IMessageConsumerHost
{
    /// <summary>The current lifecycle state (Req 3.3).</summary>
    ConsumerHostState State { get; }

    /// <summary>Starts consuming messages, advancing the host toward <see cref="ConsumerHostState.Running"/>.</summary>
    /// <param name="cancellationToken">Token to cancel startup.</param>
    Task StartAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Stops accepting new messages and drains in-flight messages within the configured shutdown
    /// timeout (Req 10.5, 10.6).
    /// </summary>
    /// <param name="cancellationToken">Token to cancel the drain.</param>
    Task StopAsync(CancellationToken cancellationToken);
}
