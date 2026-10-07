using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Messaging.Abstractions;
using Messaging.Batch.BatchProcessing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Messaging.Batch.Tests.BatchProcessing;

public class BatchMessagingProviderTests
{
    private static BatchMessagingProvider CreateProvider(TransportHealth health)
    {
        var settings = BatchTestData.Settings();
        var publisher = new BatchMessagePublisher(
            new FakeBatchDestinationWriter(),
            NullLogger<BatchMessagePublisher>.Instance);
        var host = new BatchConsumerHost(
            new FakeBatchSource(),
            new FakeBatchItemProcessor(),
            new FakeBatchCheckpointStore(),
            settings,
            NullLogger<BatchConsumerHost>.Instance);

        return new BatchMessagingProvider(publisher, host, new FakeBatchTransportProbe(health));
    }

    [Fact]
    public void Mode_Always_IsBatchProcessing()
    {
        var provider = CreateProvider(new TransportHealth(Reachable: true, UnavailableDependency: null));

        Assert.Equal(MessagingMode.BatchProcessing, provider.Mode);
    }

    [Fact]
    public void RequiredSettings_Always_DeclaresBatchSourceDestinationWindowAndTrigger()
    {
        var provider = CreateProvider(new TransportHealth(Reachable: true, UnavailableDependency: null));

        var names = provider.RequiredSettings.Select(s => s.Name).ToArray();

        Assert.Contains(BatchProviderSettings.SourceSettingName, names);
        Assert.Contains(BatchProviderSettings.DestinationSettingName, names);
        Assert.Contains(BatchProviderSettings.WindowSizeSettingName, names);
        Assert.Contains(BatchProviderSettings.TriggerModeSettingName, names);
    }

    [Fact]
    public void RequiredSettings_WindowSize_CarriesItsPermittedRange()
    {
        var provider = CreateProvider(new TransportHealth(Reachable: true, UnavailableDependency: null));

        var windowSetting = provider.RequiredSettings.Single(s => s.Name == BatchProviderSettings.WindowSizeSettingName);

        Assert.True(windowSetting.Required);
        Assert.Equal(
            $"{BatchProviderSettings.MinWindowSize}..{BatchProviderSettings.MaxWindowSize}",
            windowSetting.PermittedValuesOrRange);
    }

    [Fact]
    public async Task CheckTransportAsync_WhenReachable_ReportsReachableWithNoUnavailableDependency()
    {
        var provider = CreateProvider(new TransportHealth(Reachable: true, UnavailableDependency: null));

        var health = await provider.CheckTransportAsync(CancellationToken.None);

        Assert.True(health.Reachable);
        Assert.Null(health.UnavailableDependency);
    }

    [Fact]
    public async Task CheckTransportAsync_WhenUnreachable_NamesTheUnavailableDependency()
    {
        var provider = CreateProvider(new TransportHealth(Reachable: false, UnavailableDependency: "batch-source"));

        var health = await provider.CheckTransportAsync(CancellationToken.None);

        Assert.False(health.Reachable);
        Assert.Equal("batch-source", health.UnavailableDependency);
    }

    [Fact]
    public void Publisher_And_ConsumerHost_AreExposed()
    {
        var provider = CreateProvider(new TransportHealth(Reachable: true, UnavailableDependency: null));

        Assert.IsType<BatchMessagePublisher>(provider.Publisher);
        Assert.IsType<BatchConsumerHost>(provider.ConsumerHost);
    }
}
