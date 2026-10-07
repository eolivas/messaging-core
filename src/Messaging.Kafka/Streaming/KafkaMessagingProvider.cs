using Messaging.Abstractions;

namespace Messaging.Kafka.Streaming;

/// <summary>
/// The Streaming (Kafka) adapter: the single <see cref="IMessagingProvider"/> plug point for
/// <see cref="MessagingMode.Streaming"/> (Req 3.4). It composes the Kafka
/// <see cref="KafkaMessagePublisher"/> and <see cref="KafkaConsumerHost"/>, declares the
/// provider-specific required settings for the configuration validator (Req 2.5), and reports Kafka
/// cluster reachability to back the readiness probe (Req 10.3, 10.4).
/// </summary>
/// <remarks>
/// This is an adapter shell over the <b>native</b> Confluent.Kafka client rather than the MassTransit
/// Kafka Rider: streaming's defining concern is explicit consumer-group/partition assignment and
/// manual offset-commit control, which the Rider abstracts away (see the design's MassTransit
/// Tradeoff Analysis). The deep Kafka mechanics behind the shallow seam ports are owned by a later
/// spec. Nothing in Domain/Application depends on this package; the registry resolves it by
/// <see cref="Mode"/>.
/// </remarks>
public sealed class KafkaMessagingProvider : IMessagingProvider
{
    private readonly KafkaMessagePublisher _publisher;
    private readonly KafkaConsumerHost _consumerHost;
    private readonly IKafkaTransportProbe _transportProbe;

    /// <summary>
    /// Creates the Kafka provider over its publisher, consumer host, and transport probe
    /// (constructor injection only).
    /// </summary>
    /// <param name="publisher">The Kafka publisher.</param>
    /// <param name="consumerHost">The Kafka consumer host (consume → process → commit).</param>
    /// <param name="transportProbe">The reachability probe backing the readiness check.</param>
    /// <exception cref="ArgumentNullException">If any dependency is null.</exception>
    public KafkaMessagingProvider(
        KafkaMessagePublisher publisher,
        KafkaConsumerHost consumerHost,
        IKafkaTransportProbe transportProbe)
    {
        _publisher = publisher ?? throw new ArgumentNullException(nameof(publisher));
        _consumerHost = consumerHost ?? throw new ArgumentNullException(nameof(consumerHost));
        _transportProbe = transportProbe ?? throw new ArgumentNullException(nameof(transportProbe));
    }

    /// <inheritdoc />
    public MessagingMode Mode => MessagingMode.Streaming;

    /// <inheritdoc />
    public IMessagePublisher Publisher => _publisher;

    /// <inheritdoc />
    public IMessageConsumerHost ConsumerHost => _consumerHost;

    /// <inheritdoc />
    public IReadOnlyCollection<ProviderSettingDescriptor> RequiredSettings => KafkaProviderSettings.Describe();

    /// <inheritdoc />
    public Task<TransportHealth> CheckTransportAsync(CancellationToken cancellationToken) =>
        _transportProbe.ProbeAsync(cancellationToken);
}
