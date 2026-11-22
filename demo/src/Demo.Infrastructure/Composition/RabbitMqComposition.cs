using Demo.Application.Providers;
using Demo.Contracts;
using Messaging;
using Messaging.Abstractions;
using Messaging.Configuration;
using Messaging.MassTransit;
using Messaging.MassTransit.Configuration;
using Messaging.MassTransit.Consuming;
using Messaging.MassTransit.Transport;
using Messaging.Observability;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Demo.Infrastructure.Composition;

/// <summary>
/// RabbitMQ through MassTransit in <see cref="MessagingMode.PublishSubscribe"/>. The bus adapter
/// is registered here, before the library extension, so each consumer binds its own queue
/// (finding F2) and the library's TryAdd does not replace it. The handler chain
/// (IHandlerInvoker, contract registrations), the idempotency store, the dead-letter sink and
/// the log adapters are registered by the microservice host, not here.
/// </summary>
public sealed class RabbitMqComposition : IProviderComposition
{
    public const string DefaultConnectionString = "amqp://guest:guest@localhost:5672/";
    public const string DefaultHost = "localhost:5672";
    private const string DeadLetterTargetName = "in-memory";

    public ProviderId Id => ProviderId.RabbitMq;

    public MessagingMode Mode => MessagingMode.PublishSubscribe;

    public ProviderSettingsSnapshot Defaults()
    {
        var entries = new List<SettingEntry>
        {
            new("MassTransit", "Transport", nameof(MassTransitTransport.RabbitMq), false, false,
                nameof(MassTransitTransport.RabbitMq)),
            new("MassTransit", "ConnectionString", DefaultConnectionString, true, false,
                MassTransitProviderSettings.RabbitMqConnectionFormat),
            new("MassTransit", "Host", DefaultHost, false, false, null),
        };
        entries.AddRange(CommonMessagingSettings.Defaults(DeadLetterTargetName));
        return new ProviderSettingsSnapshot(Id, Mode.ToString(), entries);
    }

    public IReadOnlyList<SettingError> Validate(IReadOnlyDictionary<string, string> effective)
    {
        ArgumentNullException.ThrowIfNull(effective);
        var errors = new List<SettingError>();
        var connection = Read(effective, MassTransitProviderSettings.ConnectionSettingName, DefaultConnectionString);
        if (!MassTransitProviderSettings.IsRabbitMqConnectionFormatValid(connection))
        {
            // Name the setting only; the value is secret-bearing.
            errors.Add(new SettingError(
                MassTransitProviderSettings.ConnectionSettingName,
                "Must be a parseable AMQP(S) URI.",
                MassTransitProviderSettings.RabbitMqConnectionFormat));
        }

        errors.AddRange(CommonMessagingSettings.TryBuild(
            Mode,
            CommonMessagingSettings.ServiceName(MicroserviceId.SalesLeads),
            effective,
            DeadLetterTargetName,
            out _));
        return errors;
    }

    public void ConfigurePublisher(IServiceCollection services, CompositionContext context)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(context);
        var (settings, config) = Prepare(context);

        services.AddSingleton<IMassTransitBusAdapter>(sp => RabbitMqBusAdapter.Create(
            settings,
            MassTransitRole.Publisher,
            Mode,
            processor: null,
            sp.GetRequiredService<ObservabilityContext>(),
            sp.GetRequiredService<ILogger<RabbitMqBusAdapter>>()));
        // F12: before the library call, so its TryAdd keeps this host.
        services.AddSingleton<IMessageConsumerHost, PublisherOnlyConsumerHost>();
        services.AddMassTransitMessagingProvider(Mode, settings, MassTransitRole.Publisher);
        services.AddMessagingPublisher(config);
        services.AddSingleton<IHostedService>(sp => new TransportLifecycleHostedService(
            sp.GetRequiredService<IMassTransitBusAdapter>(),
            consumerHost: null,
            sp.GetRequiredService<ILogger<TransportLifecycleHostedService>>()));
    }

    public void ConfigureConsumer(IServiceCollection services, ConsumerIdentity identity, CompositionContext context)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(identity);
        ArgumentNullException.ThrowIfNull(context);
        var (settings, config) = Prepare(context);

        // F2: a queue per consumer, so both receive every event instead of competing.
        services.AddSingleton<IMassTransitBusAdapter>(sp => RabbitMqBusAdapter.Create(
            settings,
            MassTransitRole.Consumer,
            Mode,
            sp.GetRequiredService<IMassTransitConsumeProcessor>(),
            sp.GetRequiredService<ObservabilityContext>(),
            sp.GetRequiredService<ILogger<RabbitMqBusAdapter>>(),
            identity.EndpointName));
        // Before AddMessagingConsumer, so MassTransitConsumerHost is the registered IMessageConsumerHost.
        services.AddMassTransitMessagingProvider(Mode, settings, MassTransitRole.Consumer);
        services.AddMessagingConsumer(config);
        services.AddSingleton<IHostedService>(sp => new TransportLifecycleHostedService(
            sp.GetRequiredService<IMassTransitBusAdapter>(),
            sp.GetRequiredService<IMessageConsumerHost>(),
            sp.GetRequiredService<ILogger<TransportLifecycleHostedService>>()));
    }

    private (MassTransitProviderSettings Settings, MessagingConfiguration Config) Prepare(CompositionContext context)
    {
        var errors = Validate(context.EffectiveSettings);
        if (errors.Count > 0)
        {
            var fields = string.Join(", ", errors.Select(e => e.Field));
            throw new InvalidOperationException($"Invalid RabbitMQ settings: {fields}.");
        }

        var effective = context.EffectiveSettings;
        CommonMessagingSettings.TryBuild(
            Mode,
            CommonMessagingSettings.ServiceName(context.Service),
            effective,
            DeadLetterTargetName,
            out var config);
        var settings = new MassTransitProviderSettings(
            MassTransitTransport.RabbitMq,
            Read(effective, MassTransitProviderSettings.ConnectionSettingName, DefaultConnectionString),
            Read(effective, MassTransitProviderSettings.HostSettingName, DefaultHost));
        return (settings, config!);
    }

    private static string Read(IReadOnlyDictionary<string, string> effective, string key, string fallback) =>
        effective.TryGetValue(key, out var value) && !string.IsNullOrWhiteSpace(value) ? value : fallback;
}
