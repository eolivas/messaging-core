using Messaging.Abstractions;
using Messaging.MassTransit.Configuration;
using Messaging.MassTransit.Consuming;
using Messaging.MassTransit.Transport;
using Messaging.Observability;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Messaging.MassTransit.Tests;

/// <summary>
/// Unit tests for the role-scoped composition and fail-fast guard of
/// <c>AddMassTransitMessagingProvider</c> (spec task 7.3). These cover the role-scoped effect table
/// from the design — publisher stands up no consume path/receive endpoint while keeping the publish
/// path, consumer/both stand up exactly one receive endpoint and the consume path (Req 4.1–4.5) — the
/// fail-fast-with-no-partial-registration guard for an unsupported mode (Req 2.3), and the
/// single-registration / stable-instance resolution guarantee (Req 2.1, 2.2, 2.4).
/// </summary>
/// <remarks>
/// <para>
/// The composition is exercised without a live broker. Two observation points are used together:
/// </para>
/// <list type="bullet">
///   <item><description><b>Registration level.</b> The DI extension registers the consume seam
///   (<see cref="IMassTransitConsumeProcessor"/>) only for a consuming role, so whether that seam is
///   present after registration is the composition-level proxy for "one receive endpoint and the
///   consume path" (consumer/both) versus "no receive endpoint and no consume path" (publisher). A
///   fake bus adapter seam is pre-supplied so the real RabbitMQ bus factory is not invoked, keeping
///   these tests broker-free; the consume seam is deliberately <em>not</em> pre-supplied so the
///   extension's own role decision is the only thing that can register it.</description></item>
///   <item><description><b>Adapter level.</b> The production
///   <see cref="RabbitMqBusAdapter.Create(MassTransitProviderSettings, MassTransitRole, MessagingMode, IMassTransitConsumeProcessor?, ObservabilityContext, ILogger{RabbitMqBusAdapter}, string?)"/>
///   factory builds (but does not start) a bus, so it runs without a broker. It requires a processor
///   for a consuming role (one receive endpoint) and permits a null processor for a publisher-only
///   role (no receive endpoint), which is the direct "0 vs 1 receive endpoint" guarantee behind the
///   registration-level proxy.</description></item>
/// </list>
/// </remarks>
public class MassTransitRoleScopedCompositionTests
{
    private const string ReceiveQueueName = "role-scoped-tests";

    // ----- Registration-level role scoping (Req 4.1–4.5) ---------------------------------------

    [Fact]
    public void AddMassTransitMessagingProvider_PublisherRole_RegistersNoConsumeSeam()
    {
        var services = ServicesWithBusSeam();

        services.AddMassTransitMessagingProvider(
            MessagingMode.PublishSubscribe,
            MassTransitTestSupport.ValidSettings(),
            MassTransitRole.Publisher);

        // Publisher-only: the consume path (the receive endpoint's processor) is never stood up, so no
        // delivery can run the core consume steps (Req 4.2). The publish path shell stays registered
        // (Req 4.1).
        Assert.DoesNotContain(services, d => d.ServiceType == typeof(IMassTransitConsumeProcessor));
        Assert.Contains(services, d => d.ServiceType == typeof(IMessagePublisher));
    }

    [Fact]
    public void AddMassTransitMessagingProvider_ConsumerRole_RegistersConsumeSeamAndDeadLetterSendPath()
    {
        var services = ServicesWithBusSeam();

        services.AddMassTransitMessagingProvider(
            MessagingMode.MessageQueue,
            MassTransitTestSupport.ValidSettings(),
            MassTransitRole.Consumer);

        // Consumer-only: exactly one receive endpoint is stood up, so each delivery runs the core
        // consume steps — observed here as the consume seam being registered (Req 4.3). The core
        // dead-letter send path that routes a poison message still flows through that same consume
        // seam (Req 4.4).
        Assert.Contains(services, d => d.ServiceType == typeof(IMassTransitConsumeProcessor));
    }

    [Fact]
    public void AddMassTransitMessagingProvider_BothRole_RegistersConsumeSeamAndPublishPath()
    {
        var services = ServicesWithBusSeam();

        services.AddMassTransitMessagingProvider(
            MessagingMode.PublishSubscribe,
            MassTransitTestSupport.ValidSettings(),
            MassTransitRole.Both);

        // Both: exactly one receive endpoint (the consume seam, Req 4.5) and the publish path shell
        // (Req 4.5) are stood up together.
        Assert.Contains(services, d => d.ServiceType == typeof(IMassTransitConsumeProcessor));
        Assert.Contains(services, d => d.ServiceType == typeof(IMessagePublisher));
    }

    // ----- Adapter-level "0 vs 1 receive endpoint" (Req 4.2, 4.3, 4.5) -------------------------

    [Fact]
    public void Create_PublisherRole_BuildsBusWithNoReceiveEndpointAndNullProcessor()
    {
        // A publisher-only bus registers no receive endpoint, so the factory must build successfully
        // with a null processor — the direct "0 receive endpoints" guarantee (Req 4.2).
        var adapter = RabbitMqBusAdapter.Create(
            MassTransitTestSupport.ValidSettings(),
            MassTransitRole.Publisher,
            MessagingMode.PublishSubscribe,
            processor: null,
            BuildObservability(),
            NullLogger<RabbitMqBusAdapter>.Instance,
            ReceiveQueueName);

        Assert.NotNull(adapter);
    }

    [Theory]
    [InlineData(MassTransitRole.Consumer)]
    [InlineData(MassTransitRole.Both)]
    public void Create_ConsumingRole_RequiresProcessorForItsOneReceiveEndpoint(MassTransitRole role)
    {
        // A consuming role registers exactly one receive endpoint bound to the consume processor, so
        // the factory must reject a null processor rather than build a bus whose receive endpoint has
        // no consume path (Req 4.3, 4.5).
        var ex = Assert.Throws<ArgumentNullException>(() => RabbitMqBusAdapter.Create(
            MassTransitTestSupport.ValidSettings(),
            role,
            MessagingMode.PublishSubscribe,
            processor: null,
            BuildObservability(),
            NullLogger<RabbitMqBusAdapter>.Instance,
            ReceiveQueueName));

        Assert.Equal("processor", ex.ParamName);
    }

    [Theory]
    [InlineData(MassTransitRole.Consumer)]
    [InlineData(MassTransitRole.Both)]
    public void Create_ConsumingRole_BuildsBusWithOneReceiveEndpointWhenProcessorSupplied(MassTransitRole role)
    {
        // With the processor supplied, a consuming role builds exactly one receive endpoint bound to
        // it (Req 4.3, 4.5). The bus is constructed, not started, so this runs without a broker.
        var adapter = RabbitMqBusAdapter.Create(
            MassTransitTestSupport.ValidSettings(),
            role,
            MessagingMode.PublishSubscribe,
            processor: new FakeMassTransitConsumeProcessor(),
            BuildObservability(),
            NullLogger<RabbitMqBusAdapter>.Instance,
            ReceiveQueueName);

        Assert.NotNull(adapter);
    }

    // ----- Fail-fast on unsupported mode, no partial registration (Req 2.3) --------------------

    [Theory]
    [InlineData(MessagingMode.Streaming)]
    [InlineData(MessagingMode.BatchProcessing)]
    public void AddMassTransitMessagingProvider_UnsupportedMode_FailsFastWithNoPartialRegistration(MessagingMode mode)
    {
        var services = ServicesWithBusSeam();
        var descriptorsBefore = services.Count;

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            services.AddMassTransitMessagingProvider(
                mode,
                MassTransitTestSupport.ValidSettings(),
                MassTransitRole.Both));

        // No partial provider: the guard throws before any seam, settings, or provider registration,
        // so the collection is exactly as it was before the call (Req 2.3).
        Assert.Equal(descriptorsBefore, services.Count);
        Assert.DoesNotContain(services, d => d.ServiceType == typeof(IMessagingProvider));
        Assert.DoesNotContain(services, d => d.ServiceType == typeof(MassTransitProviderSettings));
        Assert.DoesNotContain(services, d => d.ServiceType == typeof(IMassTransitConsumeProcessor));
    }

    // ----- Single registration, stable instance (Req 2.1, 2.2, 2.4) ---------------------------

    [Theory]
    [InlineData(MessagingMode.PublishSubscribe)]
    [InlineData(MessagingMode.MessageQueue)]
    public void AddMassTransitMessagingProvider_RegistersExactlyOneProviderForTheMode(MessagingMode mode)
    {
        var services = ServicesWithSeams();

        services.AddMassTransitMessagingProvider(mode, MassTransitTestSupport.ValidSettings());

        using var provider = services.BuildServiceProvider();
        var providers = provider.GetServices<IMessagingProvider>().ToArray();

        // Exactly one MassTransit provider is registered for the configured mode, so the registry
        // resolves it unambiguously at startup (Req 2.1).
        Assert.Single(providers);
        Assert.Equal(mode, providers[0].Mode);
    }

    [Fact]
    public void AddMassTransitMessagingProvider_ResolvedProvider_IsSameInstanceOnRepeatedResolution()
    {
        var services = ServicesWithSeams();

        services.AddMassTransitMessagingProvider(
            MessagingMode.PublishSubscribe,
            MassTransitTestSupport.ValidSettings());

        using var provider = services.BuildServiceProvider();
        var first = provider.GetRequiredService<IMessagingProvider>();
        var second = provider.GetRequiredService<IMessagingProvider>();

        // The resolved provider is stable for the instance lifetime: every resolution returns the same
        // instance (Req 2.2, 2.4).
        Assert.Same(first, second);
    }

    [Fact]
    public void AddMassTransitMessagingProvider_ResolvedBusAdapter_IsSameInstanceOnRepeatedResolution()
    {
        var services = ServicesWithSeams();

        services.AddMassTransitMessagingProvider(
            MessagingMode.MessageQueue,
            MassTransitTestSupport.ValidSettings());

        using var provider = services.BuildServiceProvider();
        var first = provider.GetRequiredService<IMassTransitBusAdapter>();
        var second = provider.GetRequiredService<IMassTransitBusAdapter>();

        // The one bus seam is a singleton, stable for the instance lifetime, so repeated resolution
        // returns the same instance (Req 2.4).
        Assert.Same(first, second);
    }

    // ----- Helpers -----------------------------------------------------------------------------

    /// <summary>
    /// A service collection carrying only the logging the shells take and a fake bus adapter seam, so
    /// the extension's real RabbitMQ bus factory is not invoked (broker-free). The consume seam is
    /// deliberately left unregistered so the extension's own role decision is the only thing that can
    /// register it — letting a test observe role scoping at the registration level.
    /// </summary>
    private static ServiceCollection ServicesWithBusSeam()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.TryAddSingleton<IMassTransitBusAdapter>(new FakeMassTransitBusAdapter());
        return services;
    }

    /// <summary>
    /// A service collection carrying the logging and both seam fakes, so the provider graph resolves
    /// end to end without a live broker — used for the single-registration and stable-instance
    /// resolution assertions.
    /// </summary>
    private static ServiceCollection ServicesWithSeams()
    {
        var services = ServicesWithBusSeam();
        services.TryAddSingleton<IMassTransitConsumeProcessor>(new FakeMassTransitConsumeProcessor());
        return services;
    }

    private static ObservabilityContext BuildObservability() => new(
        new NoOpMessagingLog(),
        new NoOpMessagingMetrics(),
        new MessagingTraceSource(tracingEnabled: false),
        new PayloadRedactor());
}
