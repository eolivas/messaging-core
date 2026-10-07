using Messaging.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Messaging.Persistence.Idempotency;

/// <summary>
/// Minimal DI registration for the EF-backed idempotency store and its background retention sweep
/// (Req 7). Kept small and additive: the embedding application registers
/// <see cref="MessagingDbContext"/> (connection string injected, never committed) and calls this to
/// wire the store and the sweep without the Domain/Application layers taking any EF or hosting
/// dependency.
/// </summary>
public static class IdempotencyPersistenceServiceCollectionExtensions
{
    /// <summary>
    /// Registers <see cref="EfIdempotencyStore"/> as the <see cref="IIdempotencyStore"/> adapter and
    /// hosts the <see cref="IdempotencySweepService"/> that prunes expired rows (Req 7.2–7.5).
    /// </summary>
    /// <param name="services">The service collection to add to.</param>
    /// <param name="sweepOptions">
    /// Optional sweep settings; when omitted the default interval
    /// (<see cref="IdempotencySweepOptions.DefaultInterval"/>) is used.
    /// </param>
    /// <returns>The same <paramref name="services"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException">If <paramref name="services"/> is null.</exception>
    /// <remarks>
    /// Assumes <see cref="MessagingDbContext"/> is already registered (typically scoped, via
    /// <c>AddDbContext</c>). The store is scoped to match the context's lifetime; the sweep is a
    /// hosted singleton that creates its own scope per pass.
    /// </remarks>
    public static IServiceCollection AddMessagingIdempotencyStore(
        this IServiceCollection services,
        IdempotencySweepOptions? sweepOptions = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddScoped<IIdempotencyStore, EfIdempotencyStore>();
        services.AddSingleton(sweepOptions ?? new IdempotencySweepOptions());
        services.AddHostedService<IdempotencySweepService>();

        return services;
    }
}
