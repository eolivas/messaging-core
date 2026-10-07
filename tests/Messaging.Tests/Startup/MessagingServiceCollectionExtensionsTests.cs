using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Messaging.Abstractions;
using Messaging.Configuration;
using Messaging.Consuming;
using Messaging.Contracts;
using Messaging.Lifecycle;
using Messaging.Publishing;
using Messaging.Startup;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;

namespace Messaging.Tests.Startup;

/// <summary>
/// Unit tests for the use-what-you-need DI registration surfaces and the startup validate-and-resolve
/// wiring (task 21.2, Req 1.1, 1.4–1.7, 2.1, 3.6). The tests build a real
/// <see cref="IServiceProvider"/> through the public extension methods so the wiring is exercised as
/// an embedding application would call it.
/// </summary>
public sealed class MessagingServiceCollectionExtensionsTests
{
    private static MessagingConfiguration ValidConfig(
        MessagingMode mode = MessagingMode.PublishSubscribe) =>
        new() { Mode = mode };

    private static IServiceCollection ServicesWithProvider(
        MessagingMode providerMode = MessagingMode.PublishSubscribe,
        IMessagingProvider? provider = null)
    {
        var services = new ServiceCollection();
        services.AddSingleton(NullLoggerFactory.Instance);
        services.AddSingleton(typeof(Microsoft.Extensions.Logging.ILogger<>), typeof(NullLogger<>));
        services.AddSingleton<IMessagingProvider>(provider ?? new TestMessagingProvider(providerMode));
        return services;
    }

    [Fact]
    public void AddMessagingPublisher_PublishOnly_DoesNotRegisterConsumerHost()
    {
        // Arrange
        var services = ServicesWithProvider();

        // Act
        services.AddMessagingPublisher(ValidConfig());

        // Assert: the publish side is present, the consume host is not (Req 3.1 — no consume loop).
        Assert.Contains(services, d => d.ServiceType == typeof(IMessagePublisher) && IsPipeline(d));
        Assert.DoesNotContain(services, d => d.ServiceType == typeof(IMessageConsumerHost) && IsPipeline(d));
    }

    [Fact]
    public void AddMessagingConsumer_ConsumeOnly_RegistersConsumerHost()
    {
        // Arrange
        var services = ServicesWithProvider();

        // Act
        services.AddMessagingConsumer(ValidConfig());

        // Assert: the consumer host is registered; no publish pipeline is wired for a consumer-only app.
        Assert.Contains(services, d => d.ServiceType == typeof(IMessageConsumerHost) && IsPipeline(d));
        Assert.DoesNotContain(services, d => d.ServiceType == typeof(IMessagePublisher) && IsPipeline(d));
    }

    [Fact]
    public void AddMessagingCore_Both_RegistersPublisherAndConsumerHost()
    {
        // Arrange
        var services = ServicesWithProvider();

        // Act
        services.AddMessagingCore(ValidConfig());

        // Assert: both sides are wired for a consume-then-publish application.
        Assert.Contains(services, d => d.ServiceType == typeof(IMessagePublisher) && IsPipeline(d));
        Assert.Contains(services, d => d.ServiceType == typeof(IMessageConsumerHost) && IsPipeline(d));
    }

    [Fact]
    public void AddMessagingPublisher_ValidConfig_ResolvesPublishPipeline()
    {
        // Arrange
        var services = ServicesWithProvider();
        services.AddMessagingPublisher(ValidConfig());

        // Act
        using var provider = services.BuildServiceProvider();
        var publisher = provider.GetRequiredService<IMessagePublisher>();

        // Assert
        Assert.IsType<PublishPipeline>(publisher);
    }

    [Fact]
    public void AddMessagingConsumer_IdempotencyEnabled_ComposesIdempotencyGate()
    {
        // Arrange: idempotency enabled (the default) requires the gate's store/key-provider wiring.
        var services = ServicesWithProvider();
        services.AddSingleton<IHandlerInvoker, NoOpHandlerInvoker>();
        services.AddSingleton<IIdempotencyStore, AlwaysNewIdempotencyStore>();
        services.AddMessagingConsumer(ValidConfig());

        // Act
        using var provider = services.BuildServiceProvider();
        var host = provider.GetRequiredService<IMessageConsumerHost>();

        // Assert
        Assert.IsType<ConsumePipeline>(host);
        Assert.NotNull(provider.GetService<IdempotencyGate>());
    }

    [Fact]
    public void AddMessagingConsumer_IdempotencyDisabled_DoesNotRegisterIdempotencyGate()
    {
        // Arrange
        var services = ServicesWithProvider();
        services.AddSingleton<IHandlerInvoker, NoOpHandlerInvoker>();
        var config = ValidConfig() with { Idempotency = new IdempotencyConfig { Enabled = false } };
        services.AddMessagingConsumer(config);

        // Act
        using var provider = services.BuildServiceProvider();
        var host = provider.GetRequiredService<IMessageConsumerHost>();

        // Assert: a disabled configuration starts no gate (Req 7.2).
        Assert.IsType<ConsumePipeline>(host);
        Assert.Null(provider.GetService<IdempotencyGate>());
    }

    [Fact]
    public async Task StartupHostedService_InvalidConfig_FailsFast()
    {
        // Arrange: a missing mode is invalid (Req 1.2) and must abort startup.
        var services = ServicesWithProvider();
        var invalid = new MessagingConfiguration { Mode = null };
        services.AddMessagingCore(invalid);

        using var provider = services.BuildServiceProvider();
        var hostedService = provider.GetServices<IHostedService>()
            .OfType<MessagingStartupHostedService>()
            .Single();

        // Act + Assert: validation throws fail-fast before any processing-ready state (Req 2.1, 2.2).
        var ex = await Assert.ThrowsAsync<MessagingConfigurationException>(
            () => hostedService.StartAsync(CancellationToken.None));
        Assert.Contains(ex.Errors, e => e.SettingName == nameof(MessagingConfiguration.Mode));
    }

    [Fact]
    public async Task StartupHostedService_UnresolvedMode_FailsFast()
    {
        // Arrange: config asks for a mode no registered provider serves (Req 1.6).
        var services = ServicesWithProvider(providerMode: MessagingMode.PublishSubscribe);
        services.AddMessagingCore(ValidConfig(MessagingMode.Streaming));

        using var provider = services.BuildServiceProvider();
        var hostedService = provider.GetServices<IHostedService>()
            .OfType<MessagingStartupHostedService>()
            .Single();

        // Act + Assert
        await Assert.ThrowsAsync<Messaging.Providers.ProviderResolutionException>(
            () => hostedService.StartAsync(CancellationToken.None));
    }

    [Fact]
    public async Task StartupHostedService_MissingProviderSetting_FailsFast()
    {
        // Arrange: the provider declares a required setting the configuration does not supply (Req 2.5).
        var requiredSettings = new[] { new ProviderSettingDescriptor("broker.host", Required: true, null) };
        var declaringProvider = new TestMessagingProvider(MessagingMode.PublishSubscribe, requiredSettings);
        var services = ServicesWithProvider(provider: declaringProvider);
        services.AddMessagingCore(ValidConfig());

        using var provider = services.BuildServiceProvider();
        var hostedService = provider.GetServices<IHostedService>()
            .OfType<MessagingStartupHostedService>()
            .Single();

        // Act + Assert
        var ex = await Assert.ThrowsAsync<MessagingConfigurationException>(
            () => hostedService.StartAsync(CancellationToken.None));
        Assert.Contains(ex.Errors, e => e.SettingName == "broker.host");
    }

    [Fact]
    public async Task StartupHostedService_ValidConfig_ExposesActiveModeOnReadiness()
    {
        // Arrange
        var services = ServicesWithProvider(providerMode: MessagingMode.MessageQueue);
        services.AddMessagingCore(ValidConfig(MessagingMode.MessageQueue));

        using var provider = services.BuildServiceProvider();
        var hostedService = provider.GetServices<IHostedService>()
            .OfType<MessagingStartupHostedService>()
            .Single();
        var readinessState = provider.GetRequiredService<MessagingReadinessState>();

        // Act
        await hostedService.StartAsync(CancellationToken.None);

        // Assert: the active mode is exposed on the readiness state after a successful startup (Req 1.7).
        Assert.True(readinessState.IsStarted);
        Assert.Equal(MessagingMode.MessageQueue, readinessState.ActiveMode);
    }

    [Fact]
    public async Task HealthChecks_ValidConfig_ReportsActiveModeOnReadiness()
    {
        // Arrange: a reachable provider for the Message Queue mode.
        var services = ServicesWithProvider(providerMode: MessagingMode.MessageQueue);
        services.AddMessagingCore(ValidConfig(MessagingMode.MessageQueue));

        using var provider = services.BuildServiceProvider();
        var healthChecks = provider.GetRequiredService<MessagingHealthChecks>();

        // Act
        var readiness = await healthChecks.ReadinessAsync(CancellationToken.None);

        // Assert: readiness is ready and reports the active mode (Req 1.7, 10.3).
        Assert.True(readiness.IsReady);
        Assert.Equal(MessagingMode.MessageQueue, readiness.ActiveMode);
    }

    [Fact]
    public void AddMessagingCore_ResolvesSameActiveProviderEveryTime()
    {
        // Arrange
        var services = ServicesWithProvider();
        services.AddMessagingCore(ValidConfig());

        // Act
        using var provider = services.BuildServiceProvider();
        var first = provider.GetRequiredService<IMessagingProvider>();
        var second = provider.GetRequiredService<IMessagingProvider>();

        // Assert: resolution is stable for the instance lifetime (Req 1.4).
        Assert.Same(first, second);
    }

    // A registered pipeline is one added by the core helpers (an implementation-factory or concrete
    // type), as opposed to the raw provider instance the test registered as IMessagingProvider.
    private static bool IsPipeline(ServiceDescriptor descriptor) =>
        descriptor.ImplementationFactory is not null || descriptor.ImplementationType is not null;

    private sealed class NoOpHandlerInvoker : IHandlerInvoker
    {
        public Task InvokeAsync(
            MessageEnvelope envelope,
            object boundMessage,
            ConsumeContext context,
            CancellationToken cancellationToken) => Task.CompletedTask;
    }

    private sealed class AlwaysNewIdempotencyStore : IIdempotencyStore
    {
        public Task<bool> IsProcessedAsync(string idempotencyKey, CancellationToken cancellationToken) =>
            Task.FromResult(false);

        public Task RecordProcessedAsync(
            string idempotencyKey,
            TimeSpan retention,
            CancellationToken cancellationToken) => Task.CompletedTask;
    }
}
