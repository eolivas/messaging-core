using Messaging.Abstractions;
using Messaging.Kafka.Streaming;
using Microsoft.Extensions.Logging.Abstractions;

namespace Messaging.Kafka.Tests;

public class KafkaMessagingProviderTests
{
    private static KafkaMessagingProvider CreateProvider(TransportHealth health)
    {
        var settings = KafkaTestSupport.ValidSettings();
        var publisher = new KafkaMessagePublisher(
            new FakeKafkaProducer(),
            settings,
            NullLogger<KafkaMessagePublisher>.Instance);
        var consumerHost = new KafkaConsumerHost(
            new FakeKafkaConsumer(),
            new FakeStreamMessageProcessor(),
            settings,
            NullLogger<KafkaConsumerHost>.Instance);
        var probe = new FakeKafkaTransportProbe(health);
        return new KafkaMessagingProvider(publisher, consumerHost, probe);
    }

    [Fact]
    public void Mode_Always_IsStreaming()
    {
        var provider = CreateProvider(new TransportHealth(Reachable: true, UnavailableDependency: null));

        Assert.Equal(MessagingMode.Streaming, provider.Mode);
    }

    [Fact]
    public void Publisher_And_ConsumerHost_AreExposed()
    {
        var provider = CreateProvider(new TransportHealth(Reachable: true, UnavailableDependency: null));

        Assert.IsType<KafkaMessagePublisher>(provider.Publisher);
        Assert.IsType<KafkaConsumerHost>(provider.ConsumerHost);
    }

    [Fact]
    public void RequiredSettings_Declares_BootstrapServers_ConsumerGroup_And_Topic()
    {
        var provider = CreateProvider(new TransportHealth(Reachable: true, UnavailableDependency: null));

        var names = provider.RequiredSettings.Select(s => s.Name).ToArray();

        Assert.Contains(KafkaProviderSettings.BootstrapServersSettingName, names);
        Assert.Contains(KafkaProviderSettings.ConsumerGroupIdSettingName, names);
        Assert.Contains(KafkaProviderSettings.TopicSettingName, names);
    }

    [Fact]
    public void RequiredSettings_AllThreeCoreSettings_AreRequired()
    {
        var provider = CreateProvider(new TransportHealth(Reachable: true, UnavailableDependency: null));

        Assert.All(provider.RequiredSettings, s => Assert.True(s.Required));
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
        var provider = CreateProvider(new TransportHealth(Reachable: false, UnavailableDependency: "Kafka cluster"));

        var health = await provider.CheckTransportAsync(CancellationToken.None);

        Assert.False(health.Reachable);
        Assert.Equal("Kafka cluster", health.UnavailableDependency);
    }

    [Fact]
    public void Constructor_WithNullPublisher_Throws()
    {
        var settings = KafkaTestSupport.ValidSettings();
        var consumerHost = new KafkaConsumerHost(
            new FakeKafkaConsumer(),
            new FakeStreamMessageProcessor(),
            settings,
            NullLogger<KafkaConsumerHost>.Instance);
        var probe = new FakeKafkaTransportProbe(new TransportHealth(true, null));

        Assert.Throws<ArgumentNullException>(() =>
            new KafkaMessagingProvider(null!, consumerHost, probe));
    }
}
