using System;
using System.Linq;
using Messaging.Abstractions;
using Messaging.Batch.BatchProcessing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Messaging.Batch.Tests.BatchProcessing;

/// <summary>
/// Verifies the <c>AddBatchMessagingProvider</c> registration extension (Req 3.6): it registers a
/// resolvable <see cref="IMessagingProvider"/> for <see cref="MessagingMode.BatchProcessing"/>,
/// accumulates into the provider enumerable the registry consumes, keeps the provider a stable
/// singleton, and rejects null arguments fail-fast.
/// </summary>
public class BatchMessagingProviderServiceCollectionExtensionsTests
{
    private static ServiceCollection ServicesWithSeams()
    {
        var services = new ServiceCollection();

        // Logging the shells (publisher, consumer host) take via ILogger<T>.
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));

        // Deep seam ports owned by the provider's later spec; the caller supplies them.
        services.AddSingleton<IBatchSource>(new FakeBatchSource());
        services.AddSingleton<IBatchItemProcessor>(new FakeBatchItemProcessor());
        services.AddSingleton<IBatchCheckpointStore>(new FakeBatchCheckpointStore());
        services.AddSingleton<IBatchDestinationWriter>(new FakeBatchDestinationWriter());
        services.AddSingleton<IBatchTransportProbe>(new FakeBatchTransportProbe(new TransportHealth(true, null)));

        return services;
    }

    [Fact]
    public void AddBatchMessagingProvider_RegistersResolvableBatchProvider()
    {
        var services = ServicesWithSeams();

        services.AddBatchMessagingProvider(BatchTestData.Settings());

        using var provider = services.BuildServiceProvider();
        var messagingProvider = provider.GetRequiredService<IMessagingProvider>();

        Assert.IsType<BatchMessagingProvider>(messagingProvider);
        Assert.Equal(MessagingMode.BatchProcessing, messagingProvider.Mode);
    }

    [Fact]
    public void AddBatchMessagingProvider_RegistersProviderIntoTheEnumerableTheRegistryConsumes()
    {
        var services = ServicesWithSeams();

        services.AddBatchMessagingProvider(BatchTestData.Settings());

        using var provider = services.BuildServiceProvider();
        var providers = provider.GetServices<IMessagingProvider>().ToArray();

        Assert.Single(providers);
        Assert.Equal(MessagingMode.BatchProcessing, providers[0].Mode);
    }

    [Fact]
    public void AddBatchMessagingProvider_RegistersProviderAsSingleton()
    {
        var services = ServicesWithSeams();

        services.AddBatchMessagingProvider(BatchTestData.Settings());

        using var provider = services.BuildServiceProvider();
        var first = provider.GetRequiredService<IMessagingProvider>();
        var second = provider.GetRequiredService<IMessagingProvider>();

        Assert.Same(first, second);
    }

    [Fact]
    public void AddBatchMessagingProvider_ExposesPublisherConsumerHostAndRequiredSettings()
    {
        var services = ServicesWithSeams();

        services.AddBatchMessagingProvider(BatchTestData.Settings());

        using var provider = services.BuildServiceProvider();
        var messagingProvider = provider.GetRequiredService<IMessagingProvider>();

        Assert.NotNull(messagingProvider.Publisher);
        Assert.NotNull(messagingProvider.ConsumerHost);
        Assert.Contains(
            BatchProviderSettings.SourceSettingName,
            messagingProvider.RequiredSettings.Select(s => s.Name));
    }

    [Fact]
    public void AddBatchMessagingProvider_WithNullServices_Throws()
    {
        IServiceCollection services = null!;

        Assert.Throws<ArgumentNullException>(() =>
            services.AddBatchMessagingProvider(BatchTestData.Settings()));
    }

    [Fact]
    public void AddBatchMessagingProvider_WithNullSettings_Throws()
    {
        var services = new ServiceCollection();

        Assert.Throws<ArgumentNullException>(() =>
            services.AddBatchMessagingProvider(null!));
    }
}
