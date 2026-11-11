using Messaging.Abstractions;
using Messaging.Configuration;
using Messaging.Consuming;
using Messaging.Observability;
using Messaging.Resilience;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Messaging.Kafka.Streaming;

/// <summary>
/// Minimal, additive DI registration for the Streaming (Kafka) <see cref="IMessagingProvider"/>, the
/// extension point of the ports-and-adapters design for <see cref="MessagingMode.Streaming"/>
/// (Req 3.6). Calling <see cref="AddKafkaMessagingProvider"/> makes the Kafka provider selectable
/// through configuration without touching Domain (<c>Messaging.Abstractions</c>) or Application
/// (<c>Messaging</c>): the registration lives entirely in this Infrastructure adapter package, so a
/// new mode is a new registration rather than a change inward (Req 3.6).
/// </summary>
/// <remarks>
/// <para>
/// The provider is registered as <see cref="IMessagingProvider"/> with a plain
/// <see cref="ServiceCollectionServiceExtensions.AddSingleton{TService}(IServiceCollection, System.Func{System.IServiceProvider, TService})"/>
/// (not <c>TryAdd</c>) so each adapter package's registration <em>accumulates</em> into the
/// <see cref="IEnumerable{T}"/> of providers the <c>ProviderRegistry</c> is built from; the registry
/// resolves the active one by <see cref="MessagingMode"/>. Singleton is the correct lifetime: the
/// resolved provider must be stable for the instance lifetime (Req 3.7), and the provider, publisher,
/// and consumer host here are config-bound and hold a single long-lived producer/consumer.
/// </para>
/// <para>
/// This package owns the <see cref="KafkaMessagePublisher"/> and <see cref="KafkaConsumerHost"/>
/// shells and registers them, together with the production Confluent.Kafka seams:
/// <see cref="IKafkaProducer"/>, <see cref="IKafkaConsumer"/>, <see cref="IKafkaTransportProbe"/>,
/// <see cref="IStreamMessageProcessor"/> (the core consume pipeline), <see cref="KafkaClientConfigFactory"/>,
/// and the <see cref="KafkaConsumerHostedService"/> exposed as an <see cref="IHostedService"/>. All of
/// these use <c>TryAdd</c>, so a seam registered beforehand (for example a test fake) wins.
/// The core collaborators the processor composes (<see cref="ObservabilityContext"/>,
/// <see cref="RetryExecutor"/>, <see cref="DeadLetterRouter"/>, <see cref="RetryPolicyConfig"/>,
/// <see cref="LifecycleConfig"/>, <see cref="ConsumerHostStateMachine"/>, <see cref="IContractBinder"/>,
/// <see cref="IHandlerInvoker"/>, and optionally <see cref="IdempotencyGate"/>) must be registered by
/// the core messaging setup.
/// </para>
/// </remarks>
public static class KafkaMessagingProviderServiceCollectionExtensions
{
    /// <summary>
    /// Registers the Kafka <see cref="IMessagingProvider"/> for
    /// <see cref="MessagingMode.Streaming"/> together with the publisher and consumer host shells it
    /// composes (Req 3.4, 3.6).
    /// </summary>
    /// <param name="services">The service collection to add to.</param>
    /// <param name="settings">
    /// The provider-specific settings (bootstrap servers, consumer group, topic); also backs the
    /// required-settings the validator checks at startup (Req 2.5).
    /// </param>
    /// <param name="security">
    /// Injected-only security material (SASL credentials, TLS CA path) sourced from the environment or a
    /// secret store by the caller; defaults to Plaintext with no credentials (Req 12.1).
    /// </param>
    /// <returns>The same <paramref name="services"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException">If <paramref name="services"/> or <paramref name="settings"/> is null.</exception>
    public static IServiceCollection AddKafkaMessagingProvider(
        this IServiceCollection services,
        KafkaProviderSettings settings,
        KafkaSecuritySettings? security = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(settings);

        services.AddSingleton(settings);
        services.TryAddSingleton(security ?? new KafkaSecuritySettings());
        services.TryAddSingleton<KafkaClientConfigFactory>();

        // Production seams; TryAdd so a pre-registered fake or caller-supplied seam wins.
        services.TryAddSingleton<IKafkaProducer, ConfluentKafkaProducer>();
        services.TryAddSingleton<IKafkaConsumer>(sp => new ConfluentKafkaConsumer(
            sp.GetRequiredService<KafkaClientConfigFactory>(),
            sp.GetRequiredService<ILogger<ConfluentKafkaConsumer>>()));
        services.TryAddSingleton<IKafkaTransportProbe>(sp => new ConfluentKafkaTransportProbe(
            sp.GetRequiredService<KafkaClientConfigFactory>()));
        services.TryAddSingleton<IStreamMessageProcessor>(sp => new KafkaStreamMessageProcessor(
            sp.GetRequiredService<ObservabilityContext>(),
            sp.GetRequiredService<RetryExecutor>(),
            BuildConsumePipeline(sp),
            sp.GetRequiredService<DeadLetterRouter>(),
            sp.GetRequiredService<RetryPolicyConfig>()));

        services.AddSingleton<KafkaMessagePublisher>();
        services.AddSingleton<KafkaConsumerHost>();

        services.TryAddSingleton<KafkaConsumerHostedService>(sp => new KafkaConsumerHostedService(
            sp.GetRequiredService<KafkaConsumerHost>(),
            sp.GetRequiredService<LifecycleConfig>(),
            sp.GetRequiredService<ILogger<KafkaConsumerHostedService>>()));
        services.AddSingleton<IHostedService>(sp => sp.GetRequiredService<KafkaConsumerHostedService>());

        services.AddSingleton<IMessagingProvider>(sp =>
        {
            // Startup path: when the core configuration is registered and selects Streaming, validate
            // the Kafka settings in one pass so every error is raised together (Req 3.1-3.6, 12.6).
            var configuration = sp.GetService<MessagingConfiguration>();
            if (configuration is { Mode: MessagingMode.Streaming })
            {
                KafkaStartupValidator.ValidateOrThrow(configuration);
            }

            return new KafkaMessagingProvider(
                sp.GetRequiredService<KafkaMessagePublisher>(),
                sp.GetRequiredService<KafkaConsumerHost>(),
                sp.GetRequiredService<IKafkaTransportProbe>());
        });

        return services;
    }

    private static ConsumePipeline BuildConsumePipeline(IServiceProvider sp) => new(
        sp.GetRequiredService<ConsumerHostStateMachine>(),
        sp.GetRequiredService<IContractBinder>(),
        sp.GetRequiredService<IHandlerInvoker>(),
        sp.GetService<IdempotencyGate>());
}
