using Messaging.Abstractions;
using Messaging.MassTransit.Configuration;
using Messaging.MassTransit.Consuming;
using Messaging.MassTransit.Publishing;
using Messaging.MassTransit.Transport;
using Microsoft.Extensions.DependencyInjection;

namespace Messaging.MassTransit;

/// <summary>
/// Minimal, additive DI registration for the MassTransit <see cref="IMessagingProvider"/>, the
/// extension point of the ports-and-adapters design for the broker-shaped modes (Req 3.6). Calling
/// <see cref="AddMassTransitMessagingProvider"/> makes the MassTransit provider selectable through
/// configuration without touching Domain (<c>Messaging.Abstractions</c>) or Application
/// (<c>Messaging</c>): the registration lives entirely in this Infrastructure adapter package, so a
/// new mode is a new registration rather than a change inward (Req 3.6).
/// </summary>
/// <remarks>
/// <para>
/// The provider is registered as <see cref="IMessagingProvider"/> with a plain
/// <see cref="ServiceCollectionServiceExtensions.AddSingleton{TService}(IServiceCollection, System.Func{System.IServiceProvider, TService})"/>
/// (not <c>TryAdd</c>) so that each adapter package's registration <em>accumulates</em> into the
/// <see cref="IEnumerable{T}"/> of providers the <c>ProviderRegistry</c> is built from — the registry
/// resolves the active one by <see cref="MessagingMode"/>. Singleton is the correct lifetime: the
/// resolved provider must be stable for the instance lifetime (Req 3.7), and the provider and its
/// dependencies here are stateless/config-bound.
/// </para>
/// <para>
/// This package owns the publisher, consumer host, and transport probe shells and registers them. The
/// deeper seam ports they compose — <see cref="IMassTransitBusAdapter"/> (bus start/stop,
/// publish/send) and <see cref="IMassTransitConsumeProcessor"/> (the core consume steps) — are owned
/// by the provider's own later spec and must be registered by the caller. This keeps the shell
/// buildable and resolvable while leaving the live-broker mechanics to be plugged in later.
/// </para>
/// </remarks>
public static class MassTransitMessagingProviderServiceCollectionExtensions
{
    /// <summary>
    /// Registers the MassTransit <see cref="IMessagingProvider"/> for a single broker-shaped
    /// <see cref="MessagingMode"/> (Publish-Subscribe or Message Queue) together with the
    /// publisher, consumer host, and transport probe shells it composes (Req 3.4, 3.6).
    /// </summary>
    /// <param name="services">The service collection to add to.</param>
    /// <param name="mode">
    /// The mode this provider serves. Must be <see cref="MessagingMode.PublishSubscribe"/> or
    /// <see cref="MessagingMode.MessageQueue"/> — the modes MassTransit backs. The provider ctor
    /// rejects any other mode (Streaming / Batch) fail-fast at resolution.
    /// </param>
    /// <param name="settings">
    /// The provider-specific settings (transport, connection, host) bound from the open
    /// <c>ProviderSettings</c> bag; also backs the required-settings the validator checks (Req 2.5).
    /// </param>
    /// <returns>The same <paramref name="services"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException">If <paramref name="services"/> or <paramref name="settings"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// If <paramref name="mode"/> is a mode MassTransit does not back (Streaming / Batch).
    /// </exception>
    public static IServiceCollection AddMassTransitMessagingProvider(
        this IServiceCollection services,
        MessagingMode mode,
        MassTransitProviderSettings settings)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(settings);

        if (mode is not (MessagingMode.PublishSubscribe or MessagingMode.MessageQueue))
        {
            throw new ArgumentOutOfRangeException(
                nameof(mode),
                mode,
                "The MassTransit provider backs only the Publish-Subscribe and Message Queue modes.");
        }

        services.AddSingleton(settings);
        services.AddSingleton<IMassTransitTransportProbe, MassTransitTransportProbe>();
        services.AddSingleton<IMessagePublisher, MassTransitPublisher>();
        services.AddSingleton<IMessageConsumerHost, MassTransitConsumerHost>();

        services.AddSingleton<IMessagingProvider>(sp => new MassTransitMessagingProvider(
            mode,
            sp.GetRequiredService<MassTransitProviderSettings>(),
            sp.GetRequiredService<IMessagePublisher>(),
            sp.GetRequiredService<IMessageConsumerHost>(),
            sp.GetRequiredService<IMassTransitTransportProbe>()));

        return services;
    }
}
