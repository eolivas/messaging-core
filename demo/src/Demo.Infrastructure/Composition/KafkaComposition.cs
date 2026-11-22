using Demo.Application.Providers;
using Demo.Contracts;
using Messaging;
using Messaging.Abstractions;
using Messaging.Configuration;
using Messaging.Kafka.Streaming;
using Microsoft.Extensions.DependencyInjection;

namespace Demo.Infrastructure.Composition;

/// <summary>
/// Kafka in <see cref="MessagingMode.Streaming"/>. One topic, one consumer group per consuming
/// microservice, so each receives its own copy. Group ids and the topic are fixed so committed
/// offsets prevent replay of old events across Sessions. The Sales Leads host publishes only, so
/// the library's <see cref="KafkaConsumerHostedService"/> descriptor is removed there (finding F8).
/// </summary>
public sealed class KafkaComposition : IProviderComposition
{
    public const string DefaultBootstrapServers = "localhost:9092";
    public const string Topic = "demo.quote-accepted";
    public const string PublisherGroupId = "sales-leads-ms";
    public const string DefaultSecurityProtocol = nameof(KafkaSecurityProtocol.Plaintext);
    public const string CustomerGroupSettingName = "Kafka:CustomerConsumerGroupId";
    public const string CommunicationsGroupSettingName = "Kafka:CommunicationsConsumerGroupId";
    private const string DeadLetterTargetName = "in-memory";

    public ProviderId Id => ProviderId.Kafka;

    public MessagingMode Mode => MessagingMode.Streaming;

    public ProviderSettingsSnapshot Defaults()
    {
        var entries = new List<SettingEntry>
        {
            new("Kafka", "BootstrapServers", DefaultBootstrapServers, false, false, "host:port[,host:port...]"),
            new("Kafka", "Topic", Topic, false, false, null),
            new("Kafka", "SecurityProtocol", DefaultSecurityProtocol, false, false,
                string.Join(", ", Enum.GetNames<KafkaSecurityProtocol>())),
            new("Kafka", "CustomerConsumerGroupId", ConsumerIdentity.Customer.ConsumerGroupId, false, false, null),
            new("Kafka", "CommunicationsConsumerGroupId", ConsumerIdentity.Communications.ConsumerGroupId,
                false, false, null),
        };
        entries.AddRange(CommonMessagingSettings.Defaults(DeadLetterTargetName));
        return new ProviderSettingsSnapshot(Id, Mode.ToString(), entries);
    }

    public IReadOnlyList<SettingError> Validate(IReadOnlyDictionary<string, string> effective)
    {
        ArgumentNullException.ThrowIfNull(effective);
        return Build(
            MicroserviceId.SalesLeads,
            PublisherGroupId,
            effective,
            out _,
            out _);
    }

    public void ConfigurePublisher(IServiceCollection services, CompositionContext context)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(context);
        var (settings, security, config) = Prepare(context, PublisherGroupId);

        var before = services.Count;
        services.AddKafkaMessagingProvider(settings, security);
        // F8: the publisher never consumes. The only descriptors the call adds for hosted
        // services are the consumer one, so drop those added by it.
        for (var i = services.Count - 1; i >= before; i--)
        {
            var type = services[i].ServiceType;
            if (type == typeof(KafkaConsumerHostedService)
                || type == typeof(Microsoft.Extensions.Hosting.IHostedService))
            {
                services.RemoveAt(i);
            }
        }

        services.AddMessagingPublisher(config);
    }

    public void ConfigureConsumer(IServiceCollection services, ConsumerIdentity identity, CompositionContext context)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(identity);
        ArgumentNullException.ThrowIfNull(context);
        var (settings, security, config) = Prepare(context, identity.ConsumerGroupId);

        services.AddKafkaMessagingProvider(settings, security);
        services.AddMessagingConsumer(config);
    }

    private (KafkaProviderSettings Settings, KafkaSecuritySettings Security, MessagingConfiguration Config)
        Prepare(CompositionContext context, string groupId)
    {
        var errors = Build(context.Service, groupId, context.EffectiveSettings, out var config, out var merged);
        if (errors.Count > 0)
        {
            var fields = string.Join(", ", errors.Select(e => e.Field));
            throw new InvalidOperationException($"Invalid Kafka settings: {fields}.");
        }

        var settings = new KafkaProviderSettings
        {
            BootstrapServers = merged[KafkaProviderSettings.BootstrapServersSettingName],
            ConsumerGroupId = groupId,
            Topic = merged[KafkaProviderSettings.TopicSettingName],
        };
        var security = new KafkaSecuritySettings
        {
            SecurityProtocol = Enum.Parse<KafkaSecurityProtocol>(
                merged[KafkaProviderSettings.SecurityProtocolSettingName], ignoreCase: true),
        };
        return (settings, security, config!);
    }

    private IReadOnlyList<SettingError> Build(
        MicroserviceId service,
        string groupId,
        IReadOnlyDictionary<string, string> effective,
        out MessagingConfiguration? config,
        out Dictionary<string, string> merged)
    {
        merged = new Dictionary<string, string>(effective, StringComparer.Ordinal)
        {
            [KafkaProviderSettings.BootstrapServersSettingName] =
                Read(effective, KafkaProviderSettings.BootstrapServersSettingName, DefaultBootstrapServers),
            [KafkaProviderSettings.TopicSettingName] =
                Read(effective, KafkaProviderSettings.TopicSettingName, Topic),
            [KafkaProviderSettings.SecurityProtocolSettingName] =
                Read(effective, KafkaProviderSettings.SecurityProtocolSettingName, DefaultSecurityProtocol),
            [KafkaProviderSettings.ConsumerGroupIdSettingName] = groupId,
        };

        var errors = new List<SettingError>(CommonMessagingSettings.TryBuild(
            Mode,
            CommonMessagingSettings.ServiceName(service),
            merged,
            DeadLetterTargetName,
            out config));
        if (config is null)
        {
            return errors;
        }

        errors.AddRange(KafkaStartupValidator.Validate(config)
            .Select(e => new SettingError(e.SettingName, e.Message, e.PermittedValuesOrRange)));
        if (errors.Count > 0)
        {
            config = null;
        }

        return errors;
    }

    private static string Read(IReadOnlyDictionary<string, string> effective, string key, string fallback) =>
        effective.TryGetValue(key, out var value) && !string.IsNullOrWhiteSpace(value) ? value : fallback;
}
