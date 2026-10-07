using Messaging.Abstractions;
using Messaging.MassTransit.Configuration;
using Messaging.MassTransit.Consuming;
using Messaging.MassTransit.Transport;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Messaging.MassTransit.Tests;

/// <summary>
/// Verifies the <c>AddMassTransitMessagingProvider</c> registration extension (Req 3.6): it registers
/// a resolvable <see cref="IMessagingProvider"/> for the configured broker-shaped mode, accumulates
/// into the provider enumerable the registry consumes, keeps the provider a stable singleton, and
/// rejects an unsupported mode or null arguments fail-fast.
/// </summary>
public class MassTransitMessagingProviderServiceCollectionExtensionsTests
{
    private static ServiceCollection ServicesWithSeams(bool reachable = true)
    {
        var services = new ServiceCollection();

        // Logging the shells (publisher, consumer host, probe) take via ILogger<T>.
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));

        // Deep seam ports owned by the provider's later spec; the caller supplies them.
        services.AddSingleton<IMassTransitBusAdapter>(new FakeMassTransitBusAdapter(reachable));
        services.AddSingleton<IMassTransitConsumeProcessor>(new FakeMassTransitConsumeProcessor());

        return services;
    }

    [Theory]
    [InlineData(MessagingMode.PublishSubscribe)]
    [InlineData(MessagingMode.MessageQueue)]
    public void AddMassTransitMessagingProvider_RegistersResolvableProviderForTheMode(MessagingMode mode)
    {
        var services = ServicesWithSeams();

        services.AddMassTransitMessagingProvider(mode, MassTransitTestSupport.ValidSettings());

        using var provider = services.BuildServiceProvider();
        var messagingProvider = provider.GetRequiredService<IMessagingProvider>();

        Assert.IsType<MassTransitMessagingProvider>(messagingProvider);
        Assert.Equal(mode, messagingProvider.Mode);
    }

    [Fact]
    public void AddMassTransitMessagingProvider_RegistersProviderIntoTheEnumerableTheRegistryConsumes()
    {
        var services = ServicesWithSeams();

        services.AddMassTransitMessagingProvider(
            MessagingMode.PublishSubscribe,
            MassTransitTestSupport.ValidSettings());

        using var provider = services.BuildServiceProvider();
        var providers = provider.GetServices<IMessagingProvider>().ToArray();

        Assert.Single(providers);
        Assert.Equal(MessagingMode.PublishSubscribe, providers[0].Mode);
    }

    [Fact]
    public void AddMassTransitMessagingProvider_RegistersProviderAsSingleton()
    {
        var services = ServicesWithSeams();

        services.AddMassTransitMessagingProvider(
            MessagingMode.MessageQueue,
            MassTransitTestSupport.ValidSettings());

        using var provider = services.BuildServiceProvider();
        var first = provider.GetRequiredService<IMessagingProvider>();
        var second = provider.GetRequiredService<IMessagingProvider>();

        Assert.Same(first, second);
    }

    [Fact]
    public void AddMassTransitMessagingProvider_ExposesPublisherConsumerHostAndTransportCheck()
    {
        var services = ServicesWithSeams(reachable: true);

        services.AddMassTransitMessagingProvider(
            MessagingMode.PublishSubscribe,
            MassTransitTestSupport.ValidSettings());

        using var provider = services.BuildServiceProvider();
        var messagingProvider = provider.GetRequiredService<IMessagingProvider>();

        Assert.NotNull(messagingProvider.Publisher);
        Assert.NotNull(messagingProvider.ConsumerHost);
        Assert.Contains(
            MassTransitProviderSettings.TransportSettingName,
            messagingProvider.RequiredSettings.Select(s => s.Name));
    }

    [Theory]
    [InlineData(MessagingMode.Streaming)]
    [InlineData(MessagingMode.BatchProcessing)]
    public void AddMassTransitMessagingProvider_WithUnsupportedMode_Throws(MessagingMode mode)
    {
        var services = ServicesWithSeams();

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            services.AddMassTransitMessagingProvider(mode, MassTransitTestSupport.ValidSettings()));
    }

    [Fact]
    public void AddMassTransitMessagingProvider_WithNullServices_Throws()
    {
        IServiceCollection services = null!;

        Assert.Throws<ArgumentNullException>(() =>
            services.AddMassTransitMessagingProvider(
                MessagingMode.PublishSubscribe,
                MassTransitTestSupport.ValidSettings()));
    }

    [Fact]
    public void AddMassTransitMessagingProvider_WithNullSettings_Throws()
    {
        var services = new ServiceCollection();

        Assert.Throws<ArgumentNullException>(() =>
            services.AddMassTransitMessagingProvider(MessagingMode.PublishSubscribe, null!));
    }
}
