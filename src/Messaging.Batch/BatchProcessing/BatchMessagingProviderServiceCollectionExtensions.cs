using Messaging.Abstractions;
using Microsoft.Extensions.DependencyInjection;

namespace Messaging.Batch.BatchProcessing;

/// <summary>
/// Minimal, additive DI registration for the Batch Processing <see cref="IMessagingProvider"/>, the
/// extension point of the ports-and-adapters design for <see cref="MessagingMode.BatchProcessing"/>
/// (Req 3.6). Calling <see cref="AddBatchMessagingProvider"/> makes the batch provider selectable
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
/// and consumer host here are config-bound.
/// </para>
/// <para>
/// This package owns the <see cref="BatchMessagePublisher"/> and <see cref="BatchConsumerHost"/>
/// shells and registers them. The deeper seam ports they compose — <see cref="IBatchSource"/>,
/// <see cref="IBatchItemProcessor"/> (the core consume pipeline), <see cref="IBatchCheckpointStore"/>,
/// <see cref="IBatchDestinationWriter"/>, and <see cref="IBatchTransportProbe"/> — are owned by the
/// provider's own later spec and must be registered by the caller. This keeps the shell buildable and
/// resolvable while leaving the concrete batch mechanics to be plugged in later.
/// </para>
/// </remarks>
public static class BatchMessagingProviderServiceCollectionExtensions
{
    /// <summary>
    /// Registers the batch <see cref="IMessagingProvider"/> for
    /// <see cref="MessagingMode.BatchProcessing"/> together with the publisher and consumer host
    /// shells it composes (Req 3.4, 3.6).
    /// </summary>
    /// <param name="services">The service collection to add to.</param>
    /// <param name="settings">
    /// The provider-specific settings (source, destination, window size, trigger); also backs the
    /// required-settings the validator checks at startup (Req 2.5).
    /// </param>
    /// <returns>The same <paramref name="services"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException">If <paramref name="services"/> or <paramref name="settings"/> is null.</exception>
    public static IServiceCollection AddBatchMessagingProvider(
        this IServiceCollection services,
        BatchProviderSettings settings)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(settings);

        services.AddSingleton(settings);
        services.AddSingleton<BatchMessagePublisher>();
        services.AddSingleton<BatchConsumerHost>();

        services.AddSingleton<IMessagingProvider>(sp => new BatchMessagingProvider(
            sp.GetRequiredService<BatchMessagePublisher>(),
            sp.GetRequiredService<BatchConsumerHost>(),
            sp.GetRequiredService<IBatchTransportProbe>()));

        return services;
    }
}
