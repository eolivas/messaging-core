using Messaging.Abstractions;
using Messaging.MassTransit.Configuration;
using Messaging.MassTransit.Transport;
using Microsoft.Extensions.Logging.Abstractions;
using Testcontainers.LocalStack;
namespace Messaging.MassTransit.Tests;

/// <summary>
/// Task 14.12: against a dedicated LocalStack container (not the shared collection fixture, so it can
/// be stopped), the transport is reachable while the container runs and not reachable, naming the
/// Amazon SNS/SQS service, after it stops (Req 16.1, 16.2, 18.15).
/// </summary>
[Trait("Category", "Integration")]
public sealed class SnsSqsReadinessIntegrationTests : IAsyncLifetime
{
    private LocalStackContainer _container = null!;
    public async Task InitializeAsync()
    {
        var tag = LocalStackImageTag.Resolve(
            Environment.GetEnvironmentVariable(LocalStackImageTag.EnvironmentVariable));
        _container = new LocalStackBuilder()
            .WithImage($"localstack/localstack:{tag}")
            .WithEnvironment("SERVICES", "sns,sqs")
            .WithCleanUp(true)
            .Build();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(120));
        await _container.StartAsync(cts.Token);
    }
    public async Task DisposeAsync() => await _container.DisposeAsync();
    [Fact]
    public async Task Readiness_ReachableWhileRunning_NotReachableNamingServiceAfterStop()
    {
        await using var harness = SnsSqsIntegrationHarness.CreateForUrl(_container.GetConnectionString());
        var bus = harness.CreateBus(
            "readiness", MassTransitRole.Both, MessagingMode.PublishSubscribe, new NoOpProcessor());
        var settings = new MassTransitProviderSettings(
            MassTransitTransport.AmazonSqs,
            connectionString: null,
            amazonSqs: harness.CreateSettings("readiness"));
        var probe = new MassTransitTransportProbe(
            bus, settings, NullLogger<MassTransitTransportProbe>.Instance);
        using var cts = new CancellationTokenSource(SnsSqsIntegrationHarness.WaitTimeout);
        await bus.StartBusAsync(cts.Token);
        Assert.True(await bus.IsTransportReachableAsync(cts.Token));
        var up = await probe.ProbeAsync(cts.Token);
        Assert.True(up.Reachable);
        Assert.Null(up.UnavailableDependency);
        await _container.StopAsync(cts.Token);
        var reachable = await bus.IsTransportReachableAsync(CancellationToken.None);
        Assert.False(reachable);
        var down = await probe.ProbeAsync(CancellationToken.None);
        Assert.False(down.Reachable);
        Assert.NotNull(down.UnavailableDependency);
        Assert.Contains("AmazonSqs", down.UnavailableDependency);
    }
    private sealed class NoOpProcessor : Messaging.MassTransit.Consuming.IMassTransitConsumeProcessor
    {
        public Task ProcessAsync(
            Messaging.Contracts.MessageEnvelope envelope, CancellationToken cancellationToken) =>
            Task.CompletedTask;
    }
}
