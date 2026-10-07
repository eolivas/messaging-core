using Messaging.MassTransit.Publishing;
using Microsoft.Extensions.Logging.Abstractions;

namespace Messaging.MassTransit.Tests;

public class MassTransitPublisherTests
{
    [Fact]
    public async Task PublishAsync_WhenBusAccepts_ReturnsSuccessAndDelegatesToBus()
    {
        var bus = new FakeMassTransitBusAdapter();
        var publisher = new MassTransitPublisher(bus, NullLogger<MassTransitPublisher>.Instance);
        var envelope = MassTransitTestSupport.ValidEnvelope();

        var result = await publisher.PublishAsync(envelope, CancellationToken.None);

        Assert.True(result.Succeeded);
        Assert.Null(result.Error);
        Assert.Single(bus.Published);
        Assert.Same(envelope, bus.Published[0]);
    }

    [Fact]
    public async Task PublishAsync_WhenBusThrows_ReturnsFailedResultAndDoesNotThrow()
    {
        var bus = new FakeMassTransitBusAdapter(throwOnPublish: new InvalidOperationException("broker down"));
        var publisher = new MassTransitPublisher(bus, NullLogger<MassTransitPublisher>.Instance);

        var result = await publisher.PublishAsync(MassTransitTestSupport.ValidEnvelope(), CancellationToken.None);

        Assert.False(result.Succeeded);
        Assert.NotNull(result.Error);
        Assert.Empty(bus.Published);
    }

    [Fact]
    public async Task PublishAsync_WhenCancellationRequested_Throws()
    {
        var bus = new FakeMassTransitBusAdapter();
        var publisher = new MassTransitPublisher(bus, NullLogger<MassTransitPublisher>.Instance);
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(() =>
            publisher.PublishAsync(MassTransitTestSupport.ValidEnvelope(), cts.Token));
    }

    [Fact]
    public async Task PublishAsync_WithNullEnvelope_Throws()
    {
        var publisher = new MassTransitPublisher(
            new FakeMassTransitBusAdapter(),
            NullLogger<MassTransitPublisher>.Instance);

        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            publisher.PublishAsync(null!, CancellationToken.None));
    }
}
