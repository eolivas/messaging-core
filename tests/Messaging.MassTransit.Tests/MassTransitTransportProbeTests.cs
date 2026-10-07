using Messaging.MassTransit.Transport;
using Microsoft.Extensions.Logging.Abstractions;

namespace Messaging.MassTransit.Tests;

public class MassTransitTransportProbeTests
{
    private static MassTransitTransportProbe CreateProbe(FakeMassTransitBusAdapter bus) =>
        new(bus, MassTransitTestSupport.ValidSettings(), NullLogger<MassTransitTransportProbe>.Instance);

    [Fact]
    public async Task ProbeAsync_WhenReachable_ReportsReachable()
    {
        var probe = CreateProbe(new FakeMassTransitBusAdapter(reachable: true));

        var health = await probe.ProbeAsync(CancellationToken.None);

        Assert.True(health.Reachable);
        Assert.Null(health.UnavailableDependency);
    }

    [Fact]
    public async Task ProbeAsync_WhenNotReachable_NamesTheDependency()
    {
        var probe = CreateProbe(new FakeMassTransitBusAdapter(reachable: false));

        var health = await probe.ProbeAsync(CancellationToken.None);

        Assert.False(health.Reachable);
        Assert.NotNull(health.UnavailableDependency);
        Assert.Contains("RabbitMq", health.UnavailableDependency);
    }

    [Fact]
    public async Task ProbeAsync_WhenProbeThrows_ReportsUnreachableWithoutThrowing()
    {
        var probe = CreateProbe(new FakeMassTransitBusAdapter(throwOnProbe: new InvalidOperationException("broker down")));

        var health = await probe.ProbeAsync(CancellationToken.None);

        Assert.False(health.Reachable);
        Assert.NotNull(health.UnavailableDependency);
    }
}
