using Messaging.Abstractions;
using Messaging.MassTransit.Configuration;
using Messaging.MassTransit.Consuming;
using Messaging.MassTransit.Publishing;
using Microsoft.Extensions.Logging.Abstractions;

namespace Messaging.MassTransit.Tests;

public class MassTransitMessagingProviderTests
{
    private static MassTransitMessagingProvider CreateProvider(
        MessagingMode mode,
        TransportHealth health,
        MassTransitProviderSettings? settings = null)
    {
        settings ??= MassTransitTestSupport.ValidSettings();
        var bus = new FakeMassTransitBusAdapter();
        var publisher = new MassTransitPublisher(bus, NullLogger<MassTransitPublisher>.Instance);
        var consumerHost = new MassTransitConsumerHost(
            bus,
            new FakeMassTransitConsumeProcessor(),
            NullLogger<MassTransitConsumerHost>.Instance);
        var probe = new FakeMassTransitTransportProbe(health);
        return new MassTransitMessagingProvider(mode, settings, publisher, consumerHost, probe);
    }

    [Theory]
    [InlineData(MessagingMode.PublishSubscribe)]
    [InlineData(MessagingMode.MessageQueue)]
    public void Mode_ReflectsTheConfiguredBrokerShapedMode(MessagingMode mode)
    {
        var provider = CreateProvider(mode, new TransportHealth(true, null));

        Assert.Equal(mode, provider.Mode);
    }

    [Theory]
    [InlineData(MessagingMode.Streaming)]
    [InlineData(MessagingMode.BatchProcessing)]
    public void Constructor_ForUnsupportedMode_Throws(MessagingMode mode)
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            CreateProvider(mode, new TransportHealth(true, null)));
    }

    [Fact]
    public void Publisher_And_ConsumerHost_AreExposed()
    {
        var provider = CreateProvider(MessagingMode.PublishSubscribe, new TransportHealth(true, null));

        Assert.IsType<MassTransitPublisher>(provider.Publisher);
        Assert.IsType<MassTransitConsumerHost>(provider.ConsumerHost);
    }

    [Fact]
    public void RequiredSettings_Declares_Transport_And_ConnectionString()
    {
        var provider = CreateProvider(MessagingMode.MessageQueue, new TransportHealth(true, null));

        var names = provider.RequiredSettings.Select(s => s.Name).ToArray();

        Assert.Contains(MassTransitProviderSettings.TransportSettingName, names);
        Assert.Contains(MassTransitProviderSettings.ConnectionSettingName, names);
    }

    [Fact]
    public async Task CheckTransportAsync_WhenReachable_ReportsReachableWithNoUnavailableDependency()
    {
        var provider = CreateProvider(
            MessagingMode.PublishSubscribe,
            new TransportHealth(Reachable: true, UnavailableDependency: null));

        var health = await provider.CheckTransportAsync(CancellationToken.None);

        Assert.True(health.Reachable);
        Assert.Null(health.UnavailableDependency);
    }

    [Fact]
    public async Task CheckTransportAsync_WhenUnreachable_NamesTheUnavailableDependency()
    {
        var provider = CreateProvider(
            MessagingMode.PublishSubscribe,
            new TransportHealth(Reachable: false, UnavailableDependency: "RabbitMQ (localhost)"));

        var health = await provider.CheckTransportAsync(CancellationToken.None);

        Assert.False(health.Reachable);
        Assert.Equal("RabbitMQ (localhost)", health.UnavailableDependency);
    }

    [Fact]
    public void Constructor_WithNullSettings_Throws()
    {
        var bus = new FakeMassTransitBusAdapter();
        var publisher = new MassTransitPublisher(bus, NullLogger<MassTransitPublisher>.Instance);
        var consumerHost = new MassTransitConsumerHost(
            bus,
            new FakeMassTransitConsumeProcessor(),
            NullLogger<MassTransitConsumerHost>.Instance);
        var probe = new FakeMassTransitTransportProbe(new TransportHealth(true, null));

        Assert.Throws<ArgumentNullException>(() =>
            new MassTransitMessagingProvider(MessagingMode.PublishSubscribe, null!, publisher, consumerHost, probe));
    }
}
