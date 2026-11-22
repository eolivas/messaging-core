using Amazon;
using Amazon.Runtime;
using Amazon.SimpleNotificationService;
using Amazon.SQS;
using Demo.Application.Providers;
using Demo.Contracts;
using Demo.Infrastructure.Messaging;
using Messaging;
using Messaging.Abstractions;
using Messaging.Configuration;
using Messaging.Consuming;
using Messaging.Lifecycle;
using Messaging.MassTransit;
using Messaging.MassTransit.AmazonSqs;
using Messaging.MassTransit.Configuration;
using Messaging.MassTransit.Consuming;
using Messaging.MassTransit.Transport;
using Messaging.Observability;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Demo.Infrastructure.Composition;

/// <summary>
/// Amazon SNS/SQS (LocalStack) through MassTransit in <see cref="MessagingMode.PublishSubscribe"/>.
/// The library registers no Amazon seams in DI (finding F1), so this class composes them by hand
/// and registers <see cref="IMassTransitBusAdapter"/> before the library extension, so the
/// library's TryAdd does not replace it. Each consumer gets its own queue from its endpoint name
/// and every queue subscribes to the same topic.
/// </summary>
public sealed class AmazonSnsSqsComposition : IProviderComposition
{
    public const string DefaultRegion = "us-east-1";
    public const string DefaultServiceUrl = "http://localhost:4566";

    // Throwaway LocalStack credentials. LocalStack accepts any value.
    public const string DefaultAccessKeyId = "test";
    public const string DefaultSecretAccessKey = "test";

    private const string DeadLetterTargetName = "amazon-sqs-dead-letter-queue";
    private const string ValidationEndpointName = "customer-ms";
    private const string PublisherEndpointName = "sales-leads-ms";

    public ProviderId Id => ProviderId.AmazonSnsSqs;

    public MessagingMode Mode => MessagingMode.PublishSubscribe;

    public ProviderSettingsSnapshot Defaults()
    {
        var entries = new List<SettingEntry>
        {
            new("MassTransit", "Transport", nameof(MassTransitTransport.AmazonSqs), false, false,
                nameof(MassTransitTransport.AmazonSqs)),
            new("AmazonSqs", "Region", DefaultRegion, false, false, null),
            new("AmazonSqs", "ServiceUrl", DefaultServiceUrl, false, false, null),
            new("AmazonSqs", "EndpointName", "customer-ms, communications-ms", false, false, null),
            new("AmazonSqs", "AccessKeyId", DefaultAccessKeyId, true, false, null),
            new("AmazonSqs", "SecretAccessKey", DefaultSecretAccessKey, true, false, null),
            new("AmazonSqs", "AutoCreateTopology", "true", false, false, "true, false"),
            new("AmazonSqs", "RawMessageDelivery", "true", false, false, "true, false"),
            new("AmazonSqs", "Fifo", "false", false, false, "true, false"),
            new("AmazonSqs", "MaxReceiveCount",
                AmazonSqsProviderSettings.DefaultMaxReceiveCount.ToString(System.Globalization.CultureInfo.InvariantCulture),
                false, false, "1..1000"),
            new("AmazonSqs", "VisibilityTimeoutSeconds",
                AmazonSqsProviderSettings.DefaultVisibilityTimeoutSeconds.ToString(System.Globalization.CultureInfo.InvariantCulture),
                false, false, "1..43200"),
            new("AmazonSqs", "WaitTimeSeconds",
                AmazonSqsProviderSettings.DefaultWaitTimeSeconds.ToString(System.Globalization.CultureInfo.InvariantCulture),
                false, false, "0..20"),
        };
        entries.AddRange(CommonMessagingSettings.Defaults(DeadLetterTargetName));
        return new ProviderSettingsSnapshot(Id, Mode.ToString(), entries);
    }

    public IReadOnlyList<SettingError> Validate(IReadOnlyDictionary<string, string> effective)
    {
        ArgumentNullException.ThrowIfNull(effective);
        var bag = Bag(effective, ValidationEndpointName);
        var errors = new List<SettingError>(CommonMessagingSettings.TryBuild(
            Mode,
            CommonMessagingSettings.ServiceName(MicroserviceId.SalesLeads),
            bag,
            DeadLetterTargetName,
            out var config));
        if (config is not null)
        {
            // The library validator names the setting and never echoes its value.
            errors.AddRange(AmazonSqsStartupValidator.Validate(config)
                .Select(e => new SettingError(e.SettingName, e.Message, e.PermittedValuesOrRange)));
        }

        return errors;
    }

    public void ConfigurePublisher(IServiceCollection services, CompositionContext context)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(context);
        var (settings, config) = Prepare(context, PublisherEndpointName);
        RegisterSeams(services, settings, config, MassTransitRole.Publisher);

        // F12: before the library call, so its TryAdd keeps this host.
        services.AddSingleton<IMessageConsumerHost, PublisherOnlyConsumerHost>();
        services.AddMassTransitMessagingProvider(Mode, ProviderSettings(settings), MassTransitRole.Publisher);
        services.AddMessagingPublisher(config);
        services.AddSingleton<IHostedService>(sp => new AmazonSqsBusHostedService(
            MassTransitRole.Publisher,
            sp.GetRequiredService<IMassTransitBusAdapter>(),
            consumerHost: null,
            sp.GetRequiredService<ILogger<AmazonSqsBusHostedService>>()));
    }

    public void ConfigureConsumer(IServiceCollection services, ConsumerIdentity identity, CompositionContext context)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(identity);
        ArgumentNullException.ThrowIfNull(context);
        var (settings, config) = Prepare(context, identity.EndpointName);
        RegisterSeams(services, settings, config, MassTransitRole.Consumer);
        RegisterDeadLetterSink(services, settings);

        // Before AddMessagingConsumer, so MassTransitConsumerHost is the registered IMessageConsumerHost.
        services.AddMassTransitMessagingProvider(Mode, ProviderSettings(settings), MassTransitRole.Consumer);
        services.AddMessagingConsumer(config);
        services.AddSingleton<IHostedService>(sp => new AmazonSqsBusHostedService(
            MassTransitRole.Consumer,
            sp.GetRequiredService<IMassTransitBusAdapter>(),
            sp.GetRequiredService<IMessageConsumerHost>(),
            sp.GetRequiredService<ILogger<AmazonSqsBusHostedService>>()));
    }

    private static MassTransitProviderSettings ProviderSettings(AmazonSqsProviderSettings settings) =>
        new(MassTransitTransport.AmazonSqs, connectionString: null, host: null, amazonSqs: settings);

    private static void RegisterSeams(
        IServiceCollection services,
        AmazonSqsProviderSettings settings,
        MessagingConfiguration config,
        MassTransitRole role)
    {
        services.AddSingleton(settings);
        services.AddSingleton<IAmazonSQS>(_ => CreateSqsClient(settings));
        services.AddSingleton<IAmazonSimpleNotificationService>(_ => CreateSnsClient(settings));
        services.AddSingleton<ISnsSqsTopologyClient>(sp => new AwsSnsSqsTopologyClient(
            sp.GetRequiredService<IAmazonSQS>(),
            sp.GetRequiredService<IAmazonSimpleNotificationService>()));
        services.AddSingleton(sp => new SnsSqsTopologyManager(
            sp.GetRequiredService<ISnsSqsTopologyClient>(),
            settings,
            sp.GetRequiredService<ILogger<SnsSqsTopologyManager>>()));
        services.AddSingleton(sp => new AmazonSqsMetrics(sp.GetRequiredService<ILogger<AmazonSqsMetrics>>()));

        // F1: registered first so the library TryAdd of IMassTransitBusAdapter does not replace it.
        var consumes = role is MassTransitRole.Consumer or MassTransitRole.Both;
        services.AddSingleton(sp => AmazonSqsBusAdapter.Create(
            settings,
            role,
            MessagingMode.PublishSubscribe,
            sp.GetServices<ContractRegistration>().ToList(),
            consumes ? sp.GetRequiredService<IMassTransitConsumeProcessor>() : null,
            sp.GetRequiredService<SnsSqsTopologyManager>(),
            sp.GetRequiredService<ISnsSqsTopologyClient>(),
            sp.GetRequiredService<ObservabilityContext>(),
            sp.GetRequiredService<AmazonSqsMetrics>(),
            sp.GetRequiredService<InFlightTracker>(),
            config.Lifecycle.ShutdownTimeoutSeconds,
            sp.GetRequiredService<ILogger<AmazonSqsBusAdapter>>()));
        services.AddSingleton<IMassTransitBusAdapter>(sp => sp.GetRequiredService<AmazonSqsBusAdapter>());
    }

    private static void RegisterDeadLetterSink(IServiceCollection services, AmazonSqsProviderSettings settings)
    {
        var queueName = SnsSqsNamingConvention.QueueName(settings.EndpointName, settings);
        var deadLetterQueue = SnsSqsNamingConvention.DeadLetterQueueName(queueName, settings.DeadLetterQueueSuffix);

        services.TryAddSingleton<InMemoryDeadLetterSink>();
        services.AddSingleton<IDeadLetterSink>(sp => new RecordingDeadLetterSink(
            new SqsDeadLetterSink(
                new LazyDeadLetterQueueSender(sp),
                deadLetterQueue,
                settings.Fifo,
                settings.FifoGroupSelection),
            sp.GetRequiredService<InMemoryDeadLetterSink>()));
    }

    private static AmazonSQSClient CreateSqsClient(AmazonSqsProviderSettings settings)
    {
        var config = new AmazonSQSConfig();
        Apply(config, settings);
        return Credentials(settings) is { } credentials
            ? new AmazonSQSClient(credentials, config)
            : new AmazonSQSClient(config);
    }

    private static AmazonSimpleNotificationServiceClient CreateSnsClient(AmazonSqsProviderSettings settings)
    {
        var config = new AmazonSimpleNotificationServiceConfig();
        Apply(config, settings);
        return Credentials(settings) is { } credentials
            ? new AmazonSimpleNotificationServiceClient(credentials, config)
            : new AmazonSimpleNotificationServiceClient(config);
    }

    private static void Apply(ClientConfig config, AmazonSqsProviderSettings settings)
    {
        if (!string.IsNullOrWhiteSpace(settings.ServiceUrl))
        {
            // The SDK ignores the region endpoint once ServiceURL is set; AuthenticationRegion signs requests.
            config.ServiceURL = settings.ServiceUrl;
            config.AuthenticationRegion = settings.Region;
        }
        else
        {
            config.RegionEndpoint = RegionEndpoint.GetBySystemName(settings.Region);
        }
    }

    private static AWSCredentials? Credentials(AmazonSqsProviderSettings settings)
    {
        if (string.IsNullOrWhiteSpace(settings.AccessKeyId) || string.IsNullOrWhiteSpace(settings.SecretAccessKey))
        {
            return null;
        }

        return string.IsNullOrWhiteSpace(settings.SessionToken)
            ? new BasicAWSCredentials(settings.AccessKeyId, settings.SecretAccessKey)
            : new SessionAWSCredentials(settings.AccessKeyId, settings.SecretAccessKey, settings.SessionToken);
    }

    private (AmazonSqsProviderSettings Settings, MessagingConfiguration Config) Prepare(
        CompositionContext context,
        string endpointName)
    {
        var bag = Bag(context.EffectiveSettings, endpointName);
        var errors = CommonMessagingSettings.TryBuild(
            Mode,
            CommonMessagingSettings.ServiceName(context.Service),
            bag,
            DeadLetterTargetName,
            out var config);
        if (config is not null)
        {
            AmazonSqsStartupValidator.ValidateOrThrow(config);
        }
        else
        {
            var fields = string.Join(", ", errors.Select(e => e.Field));
            throw new InvalidOperationException($"Invalid Amazon SNS/SQS settings: {fields}.");
        }

        return (AmazonSqsProviderSettings.FromProviderSettings(bag), config);
    }

    /// <summary>Effective settings with the demo defaults filled in and the endpoint name fixed per host.</summary>
    private static Dictionary<string, string> Bag(IReadOnlyDictionary<string, string> effective, string endpointName)
    {
        var bag = new Dictionary<string, string>(effective, StringComparer.Ordinal);
        SetDefault(bag, AmazonSqsProviderSettings.RegionSettingName, DefaultRegion);
        SetDefault(bag, AmazonSqsProviderSettings.ServiceUrlSettingName, DefaultServiceUrl);
        SetDefault(bag, AmazonSqsProviderSettings.AccessKeyIdSettingName, DefaultAccessKeyId);
        SetDefault(bag, AmazonSqsProviderSettings.SecretAccessKeySettingName, DefaultSecretAccessKey);
        bag[AmazonSqsProviderSettings.EndpointNameSettingName] = endpointName;
        return bag;
    }

    private static void SetDefault(Dictionary<string, string> bag, string key, string value)
    {
        if (!bag.TryGetValue(key, out var current) || string.IsNullOrWhiteSpace(current))
        {
            bag[key] = value;
        }
    }
}
