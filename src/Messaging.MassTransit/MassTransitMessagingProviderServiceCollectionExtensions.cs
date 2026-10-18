using Messaging.Abstractions;
using Messaging.Configuration;
using Messaging.Consuming;
using Messaging.MassTransit.Configuration;
using Messaging.MassTransit.Consuming;
using Messaging.MassTransit.Publishing;
using Messaging.MassTransit.Transport;
using Messaging.Observability;
using Messaging.Resilience;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;

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
/// resolved provider must be stable for the instance lifetime (Req 3.7, 2.4), and the provider and its
/// dependencies here are stateless/config-bound.
/// </para>
/// <para>
/// This package owns the publisher, consumer host, and transport probe shells and registers them,
/// together with the two live seam ports they compose: the bus adapter
/// (<see cref="IMassTransitBusAdapter"/> → <see cref="RabbitMqBusAdapter"/>, built by its factory as a
/// singleton so the one RabbitMQ bus is stable for the instance lifetime) and, for a consuming role,
/// the consume processor (<see cref="IMassTransitConsumeProcessor"/> → <see cref="CoreConsumeProcessor"/>,
/// which drives the transport-neutral core consume pipeline). The seam registrations are additive via
/// <c>TryAdd</c> so a caller that has already supplied its own seam (for example a test fake, or a
/// different broker binding) keeps control (Req 2.1, 4).
/// </para>
/// </remarks>
public static class MassTransitMessagingProviderServiceCollectionExtensions
{
    /// <summary>
    /// Registers the MassTransit <see cref="IMessagingProvider"/> for a single broker-shaped
    /// <see cref="MessagingMode"/> (Publish-Subscribe or Message Queue) together with the publisher,
    /// consumer host, and transport probe shells it composes and the live RabbitMQ seam ports, scoped
    /// to the embedding application's <paramref name="role"/> (Req 3.4, 3.6, 4).
    /// </summary>
    /// <param name="services">The service collection to add to.</param>
    /// <param name="mode">
    /// The mode this provider serves. Must be <see cref="MessagingMode.PublishSubscribe"/> or
    /// <see cref="MessagingMode.MessageQueue"/> — the modes MassTransit backs. Any other mode
    /// (Streaming / Batch) is rejected fail-fast <em>before</em> any seam is registered, mirroring the
    /// provider constructor guard (Req 2.3).
    /// </param>
    /// <param name="settings">
    /// The provider-specific settings (transport, connection, host) bound from the open
    /// <c>ProviderSettings</c> bag; also backs the required-settings the validator checks (Req 2.5).
    /// </param>
    /// <param name="role">
    /// The role the embedding application takes on, driving role-scoped composition (Req 4):
    /// <see cref="MassTransitRole.Publisher"/> builds a bus with no receive endpoint and does not
    /// register the consume processor; <see cref="MassTransitRole.Consumer"/> and
    /// <see cref="MassTransitRole.Both"/> build exactly one receive endpoint and register the consume
    /// processor so each delivery runs through the core consume steps. Defaults to
    /// <see cref="MassTransitRole.Both"/>, the most permissive role.
    /// </param>
    /// <returns>The same <paramref name="services"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException">If <paramref name="services"/> or <paramref name="settings"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// If <paramref name="mode"/> is a mode MassTransit does not back (Streaming / Batch).
    /// </exception>
    public static IServiceCollection AddMassTransitMessagingProvider(
        this IServiceCollection services,
        MessagingMode mode,
        MassTransitProviderSettings settings,
        MassTransitRole role = MassTransitRole.Both)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(settings);

        // Fail fast on an unsupported mode BEFORE any seam is registered, so a misconfigured transport
        // never leaves a partial provider behind (Req 2.3). This mirrors the MassTransitMessagingProvider
        // constructor guard: RabbitMQ (and every other MassTransit transport) backs only the
        // Publish-Subscribe and Message Queue modes.
        if (mode is not (MessagingMode.PublishSubscribe or MessagingMode.MessageQueue))
        {
            throw new ArgumentOutOfRangeException(
                nameof(mode),
                mode,
                "The MassTransit provider backs only the Publish-Subscribe and Message Queue modes; " +
                $"RabbitMQ cannot serve '{mode}'.");
        }

        services.AddSingleton(settings);

        var registersConsumer = role is MassTransitRole.Consumer or MassTransitRole.Both;

        RegisterConsumeSeam(services, registersConsumer);
        RegisterBusSeam(services, mode, role, registersConsumer);

        services.TryAddSingleton<IMassTransitTransportProbe, MassTransitTransportProbe>();
        services.TryAddSingleton<IMessagePublisher, MassTransitPublisher>();
        services.TryAddSingleton<IMessageConsumerHost, MassTransitConsumerHost>();

        services.AddSingleton<IMessagingProvider>(sp => new MassTransitMessagingProvider(
            mode,
            sp.GetRequiredService<MassTransitProviderSettings>(),
            sp.GetRequiredService<IMessagePublisher>(),
            sp.GetRequiredService<IMessageConsumerHost>(),
            sp.GetRequiredService<IMassTransitTransportProbe>()));

        return services;
    }

    /// <summary>
    /// Registers the live consume seam (<see cref="IMassTransitConsumeProcessor"/> →
    /// <see cref="CoreConsumeProcessor"/>) for a consuming role only (Req 4.3, 4.5). A publisher-only
    /// role registers no processor, so no delivery path and no receive endpoint are stood up (Req 4.2).
    /// </summary>
    private static void RegisterConsumeSeam(IServiceCollection services, bool registersConsumer)
    {
        if (!registersConsumer)
        {
            // Publisher-only: the consume steps are never invoked (Req 4.2). Leave the processor
            // unregistered so nothing can drive a consume path for this composition.
            return;
        }

        // The processor drives the transport-neutral core consume pipeline; it does not reimplement it.
        // It needs the concrete ConsumePipeline (idempotency gate → contract bind → handler invoke),
        // which the core registers only behind IMessageConsumerHost — and that registration is shadowed
        // here by MassTransitConsumerHost. So compose the pipeline from its own already-registered
        // parts, exactly as the core's AddConsumePipeline does, rather than resolving the ambiguous
        // IMessageConsumerHost.
        services.TryAddSingleton<IMassTransitConsumeProcessor>(sp => new CoreConsumeProcessor(
            sp.GetRequiredService<ObservabilityContext>(),
            sp.GetRequiredService<RetryExecutor>(),
            BuildConsumePipeline(sp),
            sp.GetRequiredService<DeadLetterRouter>(),
            sp.GetRequiredService<RetryPolicyConfig>()));
    }

    /// <summary>
    /// Registers the live bus seam (<see cref="IMassTransitBusAdapter"/> → <see cref="RabbitMqBusAdapter"/>)
    /// as a singleton built by the adapter factory, so the one RabbitMQ bus is stable for the instance
    /// lifetime (Req 2.4). The <paramref name="role"/> drives endpoint topology: publisher-only builds a
    /// bus with no receive endpoint, consumer/both build exactly one bound to the consume processor
    /// (Req 4.1–4.5). The processor is resolved only for a consuming role; a publisher-only bus passes
    /// <c>null</c>, which the factory accepts because it registers no receive endpoint.
    /// </summary>
    private static void RegisterBusSeam(
        IServiceCollection services,
        MessagingMode mode,
        MassTransitRole role,
        bool registersConsumer)
    {
        services.TryAddSingleton<IMassTransitBusAdapter>(sp => RabbitMqBusAdapter.Create(
            sp.GetRequiredService<MassTransitProviderSettings>(),
            role,
            mode,
            processor: registersConsumer
                ? sp.GetRequiredService<IMassTransitConsumeProcessor>()
                : null,
            sp.GetRequiredService<ObservabilityContext>(),
            sp.GetRequiredService<ILogger<RabbitMqBusAdapter>>()));
    }

    /// <summary>
    /// Composes the core <see cref="ConsumePipeline"/> from its already-registered parts, mirroring the
    /// core consume-pipeline registration: the idempotency gate is included only when the configuration
    /// enabled it (resolved as optional), so a disabled configuration runs the pipeline without a gate.
    /// </summary>
    private static ConsumePipeline BuildConsumePipeline(IServiceProvider sp) => new(
        sp.GetRequiredService<ConsumerHostStateMachine>(),
        sp.GetRequiredService<IContractBinder>(),
        sp.GetRequiredService<IHandlerInvoker>(),
        sp.GetService<IdempotencyGate>());
}
