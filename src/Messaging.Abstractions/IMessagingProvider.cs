namespace Messaging.Abstractions;

/// <summary>
/// A pluggable adapter that implements the core messaging contracts for exactly one
/// <see cref="MessagingMode"/>. The single plug point of the ports-and-adapters design (Req 3.4).
/// </summary>
public interface IMessagingProvider
{
    /// <summary>The mode this provider serves.</summary>
    MessagingMode Mode { get; }

    /// <summary>The publisher the provider exposes for the active mode.</summary>
    IMessagePublisher Publisher { get; }

    /// <summary>The consumer host the provider exposes for the active mode.</summary>
    IMessageConsumerHost ConsumerHost { get; }

    /// <summary>
    /// Declares provider-specific required settings so the configuration validator can check them
    /// during startup validation (Req 2.5).
    /// </summary>
    IReadOnlyCollection<ProviderSettingDescriptor> RequiredSettings { get; }

    /// <summary>
    /// Reports whether the provider can reach its underlying transport, backing the readiness
    /// probe (Req 10.3, 10.4).
    /// </summary>
    /// <param name="cancellationToken">Token to cancel the check.</param>
    Task<TransportHealth> CheckTransportAsync(CancellationToken cancellationToken);
}
