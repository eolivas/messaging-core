using Messaging.Abstractions;
using Messaging.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Messaging.Persistence.Outbox;

/// <summary>
/// Minimal, additive DI registration for the EF-backed transactional outbox: the recorder that
/// stages entries into the caller's unit-of-work (Req 5.1) and the dispatcher that publishes
/// pending entries at-least-once (Req 5.2–5.6).
/// </summary>
/// <remarks>
/// Assumes <see cref="MessagingDbContext"/> and <see cref="IMessagePublisher"/> are registered
/// elsewhere (the context typically scoped via <c>AddDbContext</c>, the publisher by the active
/// provider's wiring). The recorder and dispatcher are scoped to match the context's lifetime so
/// the recorder enlists in the caller's unit-of-work and the dispatcher runs against a per-scope
/// context. The Domain/Application layers take no EF or hosting dependency from this helper.
/// </remarks>
public static class OutboxPersistenceServiceCollectionExtensions
{
    /// <summary>
    /// Registers <see cref="OutboxRecorder"/> as the <see cref="IOutboxRecorder"/> and
    /// <see cref="EfOutboxDispatcher"/> as the <see cref="IOutboxDispatcher"/> (Req 5.1–5.6).
    /// </summary>
    /// <param name="services">The service collection to add to.</param>
    /// <param name="outboxConfig">
    /// Optional outbox settings supplying the dispatch-attempt cap; when omitted a default
    /// <see cref="OutboxConfig"/> (<see cref="OutboxConfig.MaxDispatchAttempts"/> = 10) is used
    /// (Req 5.5).
    /// </param>
    /// <returns>The same <paramref name="services"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException">If <paramref name="services"/> is null.</exception>
    public static IServiceCollection AddMessagingOutboxDispatcher(
        this IServiceCollection services,
        OutboxConfig? outboxConfig = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddSingleton(outboxConfig ?? new OutboxConfig());
        services.AddScoped<IOutboxRecorder, OutboxRecorder>();
        services.AddScoped<IOutboxDispatcher, EfOutboxDispatcher>();

        return services;
    }
}
