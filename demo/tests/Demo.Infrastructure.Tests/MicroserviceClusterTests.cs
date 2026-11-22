using System.Net;
using System.Net.Sockets;
using Demo.Application.Providers;
using Demo.Application.Sessions;
using Demo.Contracts;
using Demo.Infrastructure.Composition;
using Demo.Infrastructure.Hosting;
using Messaging.Abstractions;
using Messaging.Configuration;
using Messaging.Lifecycle;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Demo.Infrastructure.Tests;

public sealed class MicroserviceClusterTests
{
    private sealed class FakeHost : IMicroserviceHost
    {
        private readonly bool _failStart;

        public FakeHost(MicroserviceId id, bool failStart)
        {
            Id = id;
            _failStart = failStart;
        }

        public MicroserviceId Id { get; }
        public MessagingConfiguration Configuration { get; } =
            new() { Mode = MessagingMode.PublishSubscribe };
        public bool Started { get; private set; }
        public bool Stopped { get; private set; }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            if (_failStart)
            {
                throw new IOException("boom");
            }

            Started = true;
            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            Stopped = true;
            return Task.CompletedTask;
        }

        public Task<TransportHealth> CheckTransportAsync(CancellationToken cancellationToken) =>
            Task.FromResult(new TransportHealth(true, null));

        public Task<ReadinessResult> CheckReadinessAsync(CancellationToken cancellationToken) =>
            Task.FromResult(ReadinessResult.Ready(MessagingMode.PublishSubscribe));

        public Task<AcceptQuoteOutcome> AcceptQuoteAsync(CancellationToken cancellationToken) =>
            Task.FromResult(new AcceptQuoteOutcome(true, "c", "l", null));

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class FakeFactory : IMicroserviceHostFactory
    {
        private readonly MicroserviceId? _failing;

        public FakeFactory(MicroserviceId? failing) => _failing = failing;

        public List<FakeHost> Hosts { get; } = new();

        public IMicroserviceHost Create(
            MicroserviceId service, IProviderComposition composition, ClusterStartRequest request,
            Func<FailureMode> failureMode)
        {
            var host = new FakeHost(service, service == _failing);
            Hosts.Add(host);
            return host;
        }
    }

    private static MicroserviceCluster NewCluster(FakeFactory factory) =>
        new([new RabbitMqComposition()], factory, new DependencyPreflight(),
            NullLogger<MicroserviceCluster>.Instance);

    [Fact]
    public async Task StartAsync_BrokerUnreachable_NamesProviderHostAndCommand()
    {
        var port = FreePort();
        var factory = new FakeFactory(null);
        var cluster = NewCluster(factory);
        var request = new ClusterStartRequest(
            ProviderId.RabbitMq, new Dictionary<string, string> { ["MassTransit:Host"] = $"127.0.0.1:{port}" });

        var ex = await Assert.ThrowsAsync<DependencyUnavailableException>(
            () => cluster.StartAsync(request, CancellationToken.None));

        Assert.Contains($"127.0.0.1:{port}", ex.Message);
        Assert.Contains("start-broker.sh rabbitmq", ex.Message);
        Assert.Empty(factory.Hosts);
    }

    [Fact]
    public async Task StartAsync_OneHostFails_StopsStartedHostsAndNamesMicroservice()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        var factory = new FakeFactory(MicroserviceId.Communications);
        var cluster = NewCluster(factory);
        var request = new ClusterStartRequest(
            ProviderId.RabbitMq, new Dictionary<string, string> { ["MassTransit:Host"] = $"127.0.0.1:{port}" });

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => cluster.StartAsync(request, CancellationToken.None));

        Assert.Contains("Communications MS", ex.Message);
        Assert.All(factory.Hosts, h => Assert.True(h.Stopped));
        Assert.False((await cluster.CheckHealthAsync(CancellationToken.None)).Ready);
    }

    private static int FreePort()
    {
        using var l = new TcpListener(IPAddress.Loopback, 0);
        l.Start();
        return ((IPEndPoint)l.LocalEndpoint).Port;
    }
}
