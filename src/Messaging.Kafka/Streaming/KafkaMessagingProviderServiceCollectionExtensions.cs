using Messaging.Abstractions;
using Microsoft.Extensions.DependencyInjection;

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
/// shells and registers them. The deeper native-client seam ports they compose —
/// <see cref="IKafkaProducer"/>, <see cref="IKafkaConsumer"/>, <see cref="IStreamMessageProcessor"/>
/// (the core consume pipeline), and <see cref="IKafkaTransportProbe"/> — are owned by the provider's
/// own later spec and must be registered by the caller. This keeps the shell buildable and resolvable
/// while leaving the live Confluent.Kafka mechanics to be plugged in later.
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
    /// <returns>The same <paramref name="services"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException">If <paramref name="services"/> or <paramref name="settings"/> is null.</exception>
    public static IServiceCollection AddKafkaMessagingProvider(
        this IServiceCollection services,
        KafkaProviderSettings settings)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(settings);

        services.AddSingleton(settings);
        services.AddSingleton<KafkaMessagePublisher>();
        services.AddSingleton<KafkaConsumerHost>();

        services.AddSingleton<IMessagingProvider>(sp => new KafkaMessagingProvider(
            sp.GetRequiredService<KafkaMessagePublisher>(),
            sp.GetRequiredService<KafkaConsumerHost>(),
            sp.GetRequiredService<IKafkaTransportProbe>()));

        return services;
    }
}
