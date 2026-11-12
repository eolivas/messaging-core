using Confluent.Kafka;

namespace Messaging.Kafka.Streaming;

/// <summary>
/// Pure factory that builds the Confluent producer, consumer, and admin client configurations from
/// <see cref="KafkaProviderSettings"/> and the injected <see cref="KafkaSecuritySettings"/>.
/// Performs no I/O and no logging, so secrets can never leak through it (Req 12.9, 12.10).
/// </summary>
public sealed class KafkaClientConfigFactory
{
    private readonly KafkaProviderSettings _settings;
    private readonly KafkaSecuritySettings _security;

    /// <summary>Creates the factory.</summary>
    /// <param name="settings">Non-secret provider settings.</param>
    /// <param name="security">Injected security material.</param>
    public KafkaClientConfigFactory(KafkaProviderSettings settings, KafkaSecuritySettings security)
    {
        _settings = settings ?? throw new ArgumentNullException(nameof(settings));
        _security = security ?? throw new ArgumentNullException(nameof(security));
    }

    /// <summary>Durable, idempotent producer: <c>Acks.All</c> + <c>EnableIdempotence</c> (Req 1.2).</summary>
    public ProducerConfig CreateProducerConfig()
    {
        var config = new ProducerConfig
        {
            BootstrapServers = _settings.BootstrapServers,
            Acks = Acks.All,
            EnableIdempotence = true,
        };
        ApplySecurity(config);
        return config;
    }

    /// <summary>
    /// Manual-commit consumer starting at the earliest offset for a new group (Req 1.3, 1.4, 4.1).
    /// No client-side redelivery or retry policy is configured beyond client defaults (Req 7.6).
    /// </summary>
    public ConsumerConfig CreateConsumerConfig()
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _settings.BootstrapServers,
            GroupId = _settings.ConsumerGroupId,
            EnableAutoCommit = false,
            AutoOffsetReset = AutoOffsetReset.Earliest,
        };
        ApplySecurity(config);
        return config;
    }

    /// <summary>Admin client config used by the reachability probe.</summary>
    public AdminClientConfig CreateAdminClientConfig()
    {
        var config = new AdminClientConfig
        {
            BootstrapServers = _settings.BootstrapServers,
        };
        ApplySecurity(config);
        return config;
    }

    private void ApplySecurity(ClientConfig config)
    {
        config.SecurityProtocol = _security.SecurityProtocol switch
        {
            KafkaSecurityProtocol.Ssl => Confluent.Kafka.SecurityProtocol.Ssl,
            KafkaSecurityProtocol.SaslPlaintext => Confluent.Kafka.SecurityProtocol.SaslPlaintext,
            KafkaSecurityProtocol.SaslSsl => Confluent.Kafka.SecurityProtocol.SaslSsl,
            _ => Confluent.Kafka.SecurityProtocol.Plaintext,
        };

        var usesSasl = _security.SecurityProtocol is KafkaSecurityProtocol.SaslPlaintext
            or KafkaSecurityProtocol.SaslSsl;
        if (usesSasl)
        {
            config.SaslMechanism = _security.SaslMechanism switch
            {
                KafkaSaslMechanism.Plain => Confluent.Kafka.SaslMechanism.Plain,
                KafkaSaslMechanism.ScramSha256 => Confluent.Kafka.SaslMechanism.ScramSha256,
                KafkaSaslMechanism.ScramSha512 => Confluent.Kafka.SaslMechanism.ScramSha512,
                _ => (Confluent.Kafka.SaslMechanism?)null,
            };
            config.SaslUsername = _security.SaslUsername;
            config.SaslPassword = _security.SaslPassword;
        }

        var usesTls = _security.SecurityProtocol is KafkaSecurityProtocol.Ssl
            or KafkaSecurityProtocol.SaslSsl;
        if (usesTls && !string.IsNullOrWhiteSpace(_security.TlsCaLocation))
        {
            config.SslCaLocation = _security.TlsCaLocation;
        }
    }
}
