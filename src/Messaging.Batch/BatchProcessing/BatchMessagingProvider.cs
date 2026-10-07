using Messaging.Abstractions;

namespace Messaging.Batch.BatchProcessing;

/// <summary>
/// The Batch Processing adapter: the single <see cref="IMessagingProvider"/> plug point for
/// <see cref="MessagingMode.BatchProcessing"/> (Req 3.4). It composes the batch
/// <see cref="BatchMessagePublisher"/> and <see cref="BatchConsumerHost"/>, declares the
/// provider-specific required settings for the configuration validator (Req 2.5), and reports
/// transport reachability to back the readiness probe (Req 10.3, 10.4).
/// </summary>
/// <remarks>
/// This is an adapter shell: it wires the ports and exposes the pull → chunk → process → checkpoint
/// consumer host, but the concrete batch mechanics behind the shallow ports are owned by a later
/// spec. Nothing in Domain/Application depends on this package; the registry resolves it by
/// <see cref="Mode"/>.
/// </remarks>
public sealed class BatchMessagingProvider : IMessagingProvider
{
    private readonly BatchMessagePublisher _publisher;
    private readonly BatchConsumerHost _consumerHost;
    private readonly IBatchTransportProbe _transportProbe;

    /// <summary>
    /// Creates the batch provider over its publisher, consumer host, and transport probe
    /// (constructor injection only).
    /// </summary>
    /// <param name="publisher">The batch publisher.</param>
    /// <param name="consumerHost">The batch consumer host (pull → chunk → process → checkpoint).</param>
    /// <param name="transportProbe">The reachability probe backing the readiness check.</param>
    /// <exception cref="ArgumentNullException">If any dependency is null.</exception>
    public BatchMessagingProvider(
        BatchMessagePublisher publisher,
        BatchConsumerHost consumerHost,
        IBatchTransportProbe transportProbe)
    {
        _publisher = publisher ?? throw new ArgumentNullException(nameof(publisher));
        _consumerHost = consumerHost ?? throw new ArgumentNullException(nameof(consumerHost));
        _transportProbe = transportProbe ?? throw new ArgumentNullException(nameof(transportProbe));
    }

    /// <inheritdoc />
    public MessagingMode Mode => MessagingMode.BatchProcessing;

    /// <inheritdoc />
    public IMessagePublisher Publisher => _publisher;

    /// <inheritdoc />
    public IMessageConsumerHost ConsumerHost => _consumerHost;

    /// <inheritdoc />
    public IReadOnlyCollection<ProviderSettingDescriptor> RequiredSettings => BatchProviderSettings.Describe();

    /// <inheritdoc />
    public Task<TransportHealth> CheckTransportAsync(CancellationToken cancellationToken) =>
        _transportProbe.ProbeAsync(cancellationToken);
}
