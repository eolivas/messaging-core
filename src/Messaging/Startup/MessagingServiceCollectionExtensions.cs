using Messaging.Abstractions;
using Messaging.Configuration;
using Messaging.Consuming;
using Messaging.Lifecycle;
using Messaging.Observability;
using Messaging.Providers;
using Messaging.Publishing;
using Messaging.Resilience;
using Messaging.Startup;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>
/// The "use-what-you-need" DI registration surface for the Messaging Core (Application layer),
/// composing whatever <see cref="IMessagingProvider"/> adapter packages were registered into the
/// publish and/or consume pipeline with the cross-cutting policies wrapped around the active provider
/// (Req 1.1, 1.4–1.7, 2.1, 3.6). The registration surface makes the active role explicit so a
/// publisher-only application never spins up a consume loop and a consumer-only application never
/// stands up unused publish machinery.
/// </summary>
/// <remarks>
/// <para>
/// <b>Layering.</b> These helpers live in the Application package but take only the
/// <c>Microsoft.Extensions.DependencyInjection.Abstractions</c>/<c>Hosting.Abstractions</c>
/// dependencies; they compose Application types and <em>ports</em> only and never reference a
/// concrete provider assembly, so the inward-dependency rule holds (Req 3.8). The concrete
/// <see cref="IMessagingProvider"/>(s) are contributed by the adapter packages'
/// <c>AddXyzMessagingProvider(...)</c> calls (task 21.1); the outbox dispatcher, idempotency store,
/// dead-letter sink, and structured-log/metric adapters are contributed by the persistence /
/// observability packages. These core helpers wire the pure Application pieces that compose them.
/// </para>
/// <para>
/// <b>Shared foundation.</b> All three surfaces register the configuration, the
/// <see cref="IProviderRegistry"/> built from the registered providers, the startup
/// validate-then-resolve gate (fail-fast — Req 2.1) and its hosted runner, the resolved active
/// provider, the readiness state exposing the active mode (Req 1.7), and the observability stack.
/// They differ only in whether the publish pipeline, the consume pipeline, or both are added.
/// </para>
/// </remarks>
public static class MessagingServiceCollectionExtensions
{
    /// <summary>
    /// Registers the <b>publish-only</b> surface: the <see cref="IMessagePublisher"/> pipeline plus the
    /// shared startup/validation/observability foundation, <em>without</em> standing up a consumer host
    /// or consume loop (Req 3.1). A publisher-only application uses this so no consume machinery it does
    /// not need is started.
    /// </summary>
    /// <param name="services">The service collection to add to.</param>
    /// <param name="configuration">The messaging configuration selecting the active mode (Req 1.1, 2.1).</param>
    /// <returns>The same <paramref name="services"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException">If <paramref name="services"/> or <paramref name="configuration"/> is null.</exception>
    public static IServiceCollection AddMessagingPublisher(
        this IServiceCollection services,
        MessagingConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configuration);

        AddMessagingFoundation(services, configuration);
        AddPublishPipeline(services);

        return services;
    }

    /// <summary>
    /// Registers the <b>consume-only</b> surface: the <see cref="IMessageConsumerHost"/> consume
    /// pipeline (idempotency gate, contract binding, retry/dead-letter, observability) plus the shared
    /// startup/validation/observability foundation, <em>without</em> the publish machinery a
    /// consumer-only application does not use. The application supplies its contract registrations and
    /// its <see cref="IHandlerInvoker"/>/handlers (and the idempotency store / dead-letter sink
    /// adapters) separately.
    /// </summary>
    /// <param name="services">The service collection to add to.</param>
    /// <param name="configuration">The messaging configuration selecting the active mode (Req 1.1, 2.1).</param>
    /// <returns>The same <paramref name="services"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException">If <paramref name="services"/> or <paramref name="configuration"/> is null.</exception>
    public static IServiceCollection AddMessagingConsumer(
        this IServiceCollection services,
        MessagingConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configuration);

        AddMessagingFoundation(services, configuration);
        AddConsumePipeline(services, configuration);

        return services;
    }

    /// <summary>
    /// Registers <b>both</b> the publish and consume surfaces together, for an application that
    /// consumes, does work, and publishes a result (Req 3.1, 3.3). Equivalent to calling
    /// <see cref="AddMessagingPublisher"/> and <see cref="AddMessagingConsumer"/>; the shared
    /// foundation is registered once (the registrations are idempotent).
    /// </summary>
    /// <param name="services">The service collection to add to.</param>
    /// <param name="configuration">The messaging configuration selecting the active mode (Req 1.1, 2.1).</param>
    /// <returns>The same <paramref name="services"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException">If <paramref name="services"/> or <paramref name="configuration"/> is null.</exception>
    public static IServiceCollection AddMessagingCore(
        this IServiceCollection services,
        MessagingConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configuration);

        AddMessagingFoundation(services, configuration);
        AddPublishPipeline(services);
        AddConsumePipeline(services, configuration);

        return services;
    }

    /// <summary>
    /// Registers the pieces every surface shares: configuration, the provider registry built from the
    /// registered providers, the fail-fast startup validate-then-resolve gate and its hosted runner,
    /// the resolved active provider, the readiness state (Req 1.7), the health-check logic, and the
    /// observability stack. All registrations use <c>TryAdd</c> so calling more than one surface (or a
    /// surface twice) does not double-register the foundation.
    /// </summary>
    private static void AddMessagingFoundation(IServiceCollection services, MessagingConfiguration configuration)
    {
        // Configuration and its sub-sections, so policy components can take the specific section they
        // need via constructor injection rather than the whole configuration (ISP).
        services.TryAddSingleton(configuration);
        services.TryAddSingleton(configuration.Retry);
        services.TryAddSingleton(configuration.Outbox);
        services.TryAddSingleton(configuration.Idempotency);
        services.TryAddSingleton(configuration.DeadLetter);
        services.TryAddSingleton(configuration.Observability);
        services.TryAddSingleton(configuration.Lifecycle);

        // Provider resolution (Req 1.5, 3.4): the registry is built from the IMessagingProvider set the
        // adapter packages accumulated. The active provider is the single one the configured mode
        // resolves to, exposed as IMessagingProvider for the health checks and any consumer of "the
        // active provider" (Req 1.4 — resolution is stable for the instance lifetime).
        services.TryAddSingleton<IProviderRegistry, ProviderRegistry>();
        services.TryAddSingleton<IConfigurationValidator>(_ => new ConfigurationValidator());
        services.TryAddSingleton(sp =>
        {
            var registry = sp.GetRequiredService<IProviderRegistry>();
            var config = sp.GetRequiredService<MessagingConfiguration>();
            return new MessagingStartupValidator(config, registry);
        });
        services.TryAddSingleton<IMessagingProvider>(sp =>
        {
            // Resolve the active provider through the shared startup gate so the single
            // validate-and-resolve decision (and its fail-fast diagnostics) is reused rather than
            // duplicated here (Req 1.5, 1.6, 2.1).
            var startupValidator = sp.GetRequiredService<MessagingStartupValidator>();
            return startupValidator.Validate();
        });

        // Startup: expose the active mode on readiness (Req 1.7) and run the fail-fast gate on startup
        // before any message is processed (Req 2.1).
        services.TryAddSingleton<MessagingReadinessState>();
        services.AddHostedService<MessagingStartupHostedService>();

        // Health/readiness logic (Req 10.1–10.4, 1.7) over the active provider.
        services.TryAddSingleton<MessagingHealthChecks>();

        // Lifecycle: in-flight tracking + graceful drain coordinator (Req 10.5–10.7).
        services.TryAddSingleton<InFlightTracker>();
        services.TryAddSingleton<GracefulShutdownCoordinator>();

        AddObservability(services);
    }

    /// <summary>
    /// Registers the observability stack shared by the publish and consume paths (Req 9): the
    /// monotonic counters, the trace source, the redactor, and the <see cref="ObservabilityContext"/>
    /// facade. <see cref="MessagingMetrics"/> satisfies both <see cref="IMessagingMetrics"/> and the
    /// dead-letter counter seam <see cref="IDeadLetterMetrics"/> so there is one counter per event with
    /// no double count. The structured-log ports (<see cref="IMessagingLog"/>, <see cref="IDeadLetterLog"/>)
    /// are satisfied by an Infrastructure adapter the caller registers.
    /// </summary>
    private static void AddObservability(IServiceCollection services)
    {
        services.TryAddSingleton<MessagingMetrics>();
        services.TryAddSingleton<IMessagingMetrics>(sp => sp.GetRequiredService<MessagingMetrics>());
        services.TryAddSingleton<IDeadLetterMetrics>(sp => sp.GetRequiredService<MessagingMetrics>());

        services.TryAddSingleton(sp =>
        {
            var observability = sp.GetRequiredService<ObservabilityConfig>();
            return new MessagingTraceSource(observability.TracingEnabled, observability.ServiceName);
        });
        services.TryAddSingleton<PayloadRedactor>();
        services.TryAddSingleton<ObservabilityContext>();
    }

    /// <summary>
    /// Adds the publish side: the correlation-id gate and the <see cref="PublishPipeline"/> bound to
    /// the configured active mode. Resolution of the active provider stays behind the
    /// <see cref="IProviderRegistry"/> so the pipeline keeps the fail-fast unresolved/ambiguous
    /// diagnostics (Req 3.1, 3.2, 4.4). The transactional outbox dispatcher, when used, is registered
    /// by the persistence package's <c>AddMessagingOutboxDispatcher</c> helper (Req 5).
    /// </summary>
    private static void AddPublishPipeline(IServiceCollection services)
    {
        services.TryAddSingleton<CorrelationIdGate>();
        services.TryAddSingleton<IMessagePublisher>(sp =>
        {
            var correlationIdGate = sp.GetRequiredService<CorrelationIdGate>();
            var registry = sp.GetRequiredService<IProviderRegistry>();
            var configuration = sp.GetRequiredService<MessagingConfiguration>();

            // The mode is guaranteed present once the startup gate has validated it; resolve the active
            // mode here for the pipeline to bind to (Req 1.4 — fixed for the instance lifetime).
            var activeMode = configuration.Mode
                ?? throw new InvalidOperationException(
                    "Cannot build the publish pipeline: the messaging mode is not configured.");

            return new PublishPipeline(correlationIdGate, registry, activeMode);
        });
    }

    /// <summary>
    /// Adds the consume side: the contract binder, idempotency key provider + gate (when enabled),
    /// failure classification + backoff + dead-letter routing + retry executor, and the
    /// <see cref="IMessageConsumerHost"/> consume pipeline (Req 3.3, 4.5–4.7, 6, 7, 8). The idempotency
    /// gate is only composed into the host when idempotency is enabled in configuration, so a disabled
    /// configuration starts no gate (Req 7.2). The application supplies the contract registrations, the
    /// <see cref="IHandlerInvoker"/>/handlers, and the idempotency-store / dead-letter-sink adapters.
    /// </summary>
    private static void AddConsumePipeline(IServiceCollection services, MessagingConfiguration configuration)
    {
        services.TryAddSingleton<ConsumerHostStateMachine>();
        services.TryAddSingleton<IContractBinder>(sp =>
            new JsonContractBinder(sp.GetServices<ContractRegistration>()));
        services.TryAddSingleton<IIdempotencyKeyProvider, DefaultIdempotencyKeyProvider>();

        // Resilience policy (Req 6, 8): classification + backoff + dead-letter routing + retry executor.
        services.TryAddSingleton<IFailureClassifier, FailureClassifier>();
        services.TryAddSingleton<BackoffCalculator>();
        services.TryAddSingleton<DeadLetterRouter>();
        services.TryAddSingleton<RetryExecutor>();

        if (configuration.Idempotency.Enabled)
        {
            services.TryAddSingleton<IdempotencyGate>();
        }

        services.TryAddSingleton<IMessageConsumerHost>(sp =>
        {
            var stateMachine = sp.GetRequiredService<ConsumerHostStateMachine>();
            var binder = sp.GetRequiredService<IContractBinder>();
            var handlerInvoker = sp.GetRequiredService<IHandlerInvoker>();

            // Compose the idempotency gate only when enabled, so a disabled configuration runs the
            // pipeline without a gate exactly as before idempotency existed (Req 7.2).
            var idempotencyGate = sp.GetService<IdempotencyGate>();
            return new ConsumePipeline(stateMachine, binder, handlerInvoker, idempotencyGate);
        });
    }
}
