using Demo.Application.Providers;
using Demo.Application.Sessions;
using Demo.Contracts;
using Demo.Infrastructure.Composition;
using Demo.Infrastructure.Hosting;
using Microsoft.Extensions.Logging.Abstractions;

namespace Demo.IntegrationTests.Support;

/// <summary>One Session against a real broker: the three microservice hosts plus a recording flow sink.</summary>
public sealed class Session : IAsyncDisposable
{
    public static readonly TimeSpan EventTimeout = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan Settle = TimeSpan.FromSeconds(3);

    private readonly MicroserviceCluster _cluster;
    private readonly ClusterStartRequest _request;

    private Session(MicroserviceCluster cluster, ClusterStartRequest request, RecordingFlowStepSink sink)
    {
        _cluster = cluster;
        _request = request;
        Flow = sink;
    }

    public RecordingFlowStepSink Flow { get; }

    public static async Task<Session> StartAsync(BrokerFixture broker)
    {
        ArgumentNullException.ThrowIfNull(broker);
        var sink = new RecordingFlowStepSink();
        var factory = new MicroserviceHostFactory(NullLoggerFactory.Instance, sink, TimeProvider.System);
        IProviderComposition[] compositions =
            [new RabbitMqComposition(), new AmazonSnsSqsComposition(), new KafkaComposition()];
        var cluster = new MicroserviceCluster(
            compositions, factory, new DependencyPreflight(), NullLogger<MicroserviceCluster>.Instance);
        var request = new ClusterStartRequest(broker.Provider, broker.Settings);
        var session = new Session(cluster, request, sink);
        // Throws when the broker is unreachable or not ready within the 30 s start budget.
        await cluster.StartAsync(request, CancellationToken.None);
        return session;
    }

    public void SetFailureMode(ConsumerId consumer, FailureMode mode) => _cluster.SetFailureMode(consumer, mode);

    public async Task<AcceptQuoteOutcome> TriggerAsync()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var outcome = await _cluster.SalesLeads.AcceptQuoteAsync(cts.Token);
        Assert.True(outcome.Succeeded, outcome.Error);
        Assert.False(string.IsNullOrEmpty(outcome.LeadId));
        Assert.False(string.IsNullOrEmpty(outcome.CorrelationId));
        return outcome;
    }

    /// <summary>Polls until the condition holds or 30 s pass; fails the test on timeout.</summary>
    public static async Task WaitUntilAsync(Func<bool> condition, string what)
    {
        var deadline = DateTime.UtcNow + EventTimeout;
        while (!condition())
        {
            if (DateTime.UtcNow > deadline)
            {
                throw new Xunit.Sdk.XunitException($"Timed out after {EventTimeout.TotalSeconds:0} s waiting for: {what}.");
            }

            await Task.Delay(100);
        }
    }

    /// <summary>Lets late duplicates arrive, so "exactly one" is not satisfied by a premature check.</summary>
    public static Task SettleAsync() => Task.Delay(Settle);

    public async ValueTask DisposeAsync() => await _cluster.StopAsync(CancellationToken.None);
}
