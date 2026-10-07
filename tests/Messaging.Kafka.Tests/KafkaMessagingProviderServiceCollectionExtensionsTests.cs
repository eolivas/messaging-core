using Messaging.Abstractions;
using Messaging.Kafka.Streaming;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Messaging.Kafka.Tests;

/// <summary>
/// Verifies the <c>AddKafkaMessagingProvider</c> registration extension (Req 3.6): it registers a
/// resolvable <see cref="IMessagingProvider"/> for <see cref="MessagingMode.Streaming"/>, accumulates
/// into the provider enumerable the registry consumes, keeps the provider a stable singleton, and
/// rejects null arguments fail-fast.
/// </summary>
public class KafkaMessagingProviderServiceCollectionExtensionsTests
{
    private static ServiceCollection ServicesWithSeams()
    {
        var services = new ServiceCollection();

        // Logging the shells (publisher, consumer host) take via ILogger<T>.
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));

        // Deep native-client seam ports owned by the provider's later spec; the caller supplies them.
        services.AddSingleton<IKafkaProducer>(new FakeKafkaProducer());
        services.AddSingleton<IKafkaConsumer>(new FakeKafkaConsumer());
        services.AddSingleton<IStreamMessageProcessor>(new FakeStreamMessageProcessor());
        services.AddSingleton<IKafkaTransportProbe>(new FakeKafkaTransportProbe(new TransportHealth(true, null)));

        return services;
    }

    [Fact]
    public void AddKafkaMessagingProvider_RegistersResolvableStreamingProvider()
    {
        var services = ServicesWithSeams();

        services.AddKafkaMessagingProvider(KafkaTestSupport.ValidSettings());

        using var provider = services.BuildServiceProvider();
        var messagingProvider = provider.GetRequiredService<IMessagingProvider>();

        Assert.IsType<KafkaMessagingProvider>(messagingProvider);
        Assert.Equal(MessagingMode.Streaming, messagingProvider.Mode);
    }

    [Fact]
    public void AddKafkaMessagingProvider_RegistersProviderIntoTheEnumerableTheRegistryConsumes()
    {
        var services = ServicesWithSeams();

        services.AddKafkaMessagingProvider(KafkaTestSupport.ValidSettings());

        using var provider = services.BuildServiceProvider();
        var providers = provider.GetServices<IMessagingProvider>().ToArray();

        Assert.Single(providers);
        Assert.Equal(MessagingMode.Streaming, providers[0].Mode);
    }

    [Fact]
    public void AddKafkaMessagingProvider_RegistersProviderAsSingleton()
    {
        var services = ServicesWithSeams();

        services.AddKafkaMessagingProvider(KafkaTestSupport.ValidSettings());

        using var provider = services.BuildServiceProvider();
        var first = provider.GetRequiredService<IMessagingProvider>();
        var second = provider.GetRequiredService<IMessagingProvider>();

        Assert.Same(first, second);
    }

    [Fact]
    public void AddKafkaMessagingProvider_ExposesPublisherConsumerHostAndRequiredSettings()
    {
        var services = ServicesWithSeams();

        services.AddKafkaMessagingProvider(KafkaTestSupport.ValidSettings());

        using var provider = services.BuildServiceProvider();
        var messagingProvider = provider.GetRequiredService<IMessagingProvider>();

        Assert.NotNull(messagingProvider.Publisher);
        Assert.NotNull(messagingProvider.ConsumerHost);
        Assert.Contains(
            KafkaProviderSettings.BootstrapServersSettingName,
            messagingProvider.RequiredSettings.Select(s => s.Name));
    }

    [Fact]
    public void AddKafkaMessagingProvider_WithNullServices_Throws()
    {
        IServiceCollection services = null!;

        Assert.Throws<ArgumentNullException>(() =>
            services.AddKafkaMessagingProvider(KafkaTestSupport.ValidSettings()));
    }

    [Fact]
    public void AddKafkaMessagingProvider_WithNullSettings_Throws()
    {
        var services = new ServiceCollection();

        Assert.Throws<ArgumentNullException>(() =>
            services.AddKafkaMessagingProvider(null!));
    }
}
