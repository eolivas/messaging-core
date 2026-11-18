using Amazon.Runtime;
using Messaging.Abstractions;
using Messaging.Consuming;
using Messaging.Lifecycle;
using Messaging.MassTransit.AmazonSqs;
using Messaging.MassTransit.Configuration;
using Messaging.MassTransit.Transport;
using Messaging.Observability;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Messaging.MassTransit.Tests;

/// <summary>
/// Unit tests for <see cref="AmazonSqsBusAdapter.IsTransportReachableAsync"/> and the
/// <see cref="MassTransitTransportProbe"/> result built over it: reachable, unreachable, throwing,
/// authorization error, and timeout via an injected slow ping; the result names only the region or
/// the <c>ServiceUrl</c> host. Requirements 1.7, 1.8, 16.1 to 16.5, 17.7.
/// </summary>
[Trait("Feature", "sns-sqs-provider")]
[Collection(NonParallelCollection.Name)]
public class AmazonSqsReachabilityTests
{
    private static readonly AmazonSqsProviderSettings Settings =
        new() { Region = "us-east-1", EndpointName = "orders" };

    private static AmazonSqsBusAdapter NewAdapter(FakeSnsSqsTopologyClient client) =>
        new(
            new FakeBusControl(),
            Settings,
            MessagingMode.PublishSubscribe,
            MassTransitRole.Publisher,
            new[] { new ContractRegistration("order.created", typeof(object)) },
            new SnsSqsTopologyManager(client, Settings, NullLogger<SnsSqsTopologyManager>.Instance),
            client,
            new ObservabilityContext(
                new NoOpMessagingLog(),
                new NoOpMessagingMetrics(),
                new MessagingTraceSource(tracingEnabled: false),
                new PayloadRedactor()),
            new AmazonSqsMetrics(),
            new InFlightTracker(),
            30,
            NullLogger<AmazonSqsBusAdapter>.Instance);

    private static async Task<AmazonSqsBusAdapter> StartedAdapter(FakeSnsSqsTopologyClient client)
    {
        var adapter = NewAdapter(client);
        await adapter.StartBusAsync(CancellationToken.None);
        return adapter;
    }

    [Fact]
    public async Task IsTransportReachableAsync_PingSucceeds_ReturnsTrue()
    {
        var client = new FakeSnsSqsTopologyClient();
        await using var adapter = await StartedAdapter(client);

        Assert.True(await adapter.IsTransportReachableAsync(CancellationToken.None));
        Assert.Contains("Ping", client.Calls);
    }

    [Fact]
    public async Task IsTransportReachableAsync_NotStarted_ReturnsFalseWithoutPinging()
    {
        var client = new FakeSnsSqsTopologyClient();
        await using var adapter = NewAdapter(client);

        Assert.False(await adapter.IsTransportReachableAsync(CancellationToken.None));
        Assert.DoesNotContain("Ping", client.Calls);
    }

    [Fact]
    public async Task IsTransportReachableAsync_PingThrows_ReturnsFalseAndDoesNotThrow()
    {
        var client = new FakeSnsSqsTopologyClient();
        await using var adapter = await StartedAdapter(client);
        client.FailOn(SnsSqsTopologyOperation.Ping, new InvalidOperationException("sqs down"));

        Assert.False(await adapter.IsTransportReachableAsync(CancellationToken.None));
    }

    [Fact]
    public async Task IsTransportReachableAsync_AuthorizationError_ReturnsFalseAndDoesNotThrow()
    {
        var client = new FakeSnsSqsTopologyClient();
        await using var adapter = await StartedAdapter(client);
        client.FailOn(
            SnsSqsTopologyOperation.Ping,
            new AmazonServiceException("AccessDenied") { ErrorCode = "AccessDenied" });

        Assert.False(await adapter.IsTransportReachableAsync(CancellationToken.None));
    }

    [Fact]
    public async Task IsTransportReachableAsync_SlowPing_TimesOutWithinBoundedWait()
    {
        var client = new FakeSnsSqsTopologyClient();
        await using var adapter = await StartedAdapter(client);
        // Ignores cancellation so only the adapter's own bound can end the wait.
        var release = new TaskCompletionSource();
        client.PingOverride = _ => release.Task;

        var started = DateTime.UtcNow;
        var reachable = await adapter.IsTransportReachableAsync(CancellationToken.None);
        var elapsed = DateTime.UtcNow - started;
        release.SetResult();

        Assert.False(reachable);
        Assert.InRange(elapsed, TimeSpan.FromMilliseconds(500), TimeSpan.FromSeconds(5));
    }

    [Theory]
    [InlineData(null, "us-east-1")]
    [InlineData("http://user:pa55word@localstack.local:4566/path?token=secret", "localstack.local")]
    public async Task Probe_WhenUnreachable_NamesOnlyRegionOrServiceUrlHost(
        string? serviceUrl, string expectedHost)
    {
        var amazon = new AmazonSqsProviderSettings
        {
            Region = "us-east-1",
            ServiceUrl = serviceUrl,
            EndpointName = "orders",
        };
        var settings = new MassTransitProviderSettings(
            MassTransitTransport.AmazonSqs, connectionString: null, amazonSqs: amazon);
        var client = new FakeSnsSqsTopologyClient();
        await using var adapter = await StartedAdapter(client);
        client.FailOn(SnsSqsTopologyOperation.Ping, new InvalidOperationException("boom"));
        var probe = new MassTransitTransportProbe(
            adapter, settings, NullLogger<MassTransitTransportProbe>.Instance);

        var health = await probe.ProbeAsync(CancellationToken.None);

        Assert.False(health.Reachable);
        Assert.NotNull(health.UnavailableDependency);
        Assert.Contains(expectedHost, health.UnavailableDependency);
        Assert.DoesNotContain("pa55word", health.UnavailableDependency);
        Assert.DoesNotContain("secret", health.UnavailableDependency);
        Assert.DoesNotContain("/path", health.UnavailableDependency);
    }

    [Fact]
    public async Task Probe_WhenReachable_ReportsReachableWithNoDependency()
    {
        var settings = new MassTransitProviderSettings(
            MassTransitTransport.AmazonSqs, connectionString: null, amazonSqs: Settings);
        await using var adapter = await StartedAdapter(new FakeSnsSqsTopologyClient());
        var probe = new MassTransitTransportProbe(
            adapter, settings, NullLogger<MassTransitTransportProbe>.Instance);

        var health = await probe.ProbeAsync(CancellationToken.None);

        Assert.True(health.Reachable);
        Assert.Null(health.UnavailableDependency);
    }
}
