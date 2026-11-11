using System.Diagnostics;
using Messaging.Kafka.Streaming;

namespace Messaging.Kafka.Tests;

public class ConfluentKafkaTransportProbeTests
{
    private const string Username = "secret-user-name";
    private const string Password = "secret-pass-word";

    private static ConfluentKafkaTransportProbe CreateProbe(Func<CancellationToken, Task<int>> check)
    {
        var security = new KafkaSecuritySettings
        {
            SecurityProtocol = KafkaSecurityProtocol.SaslSsl,
            SaslMechanism = KafkaSaslMechanism.Plain,
            SaslUsername = Username,
            SaslPassword = Password,
        };
        return new ConfluentKafkaTransportProbe(
            new KafkaClientConfigFactory(KafkaTestSupport.ValidSettings(), security),
            check);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(3)]
    public async Task ProbeAsync_WithAtLeastOneBroker_ReturnsReachableWithNoDependency(int brokers)
    {
        var probe = CreateProbe(_ => Task.FromResult(brokers));

        var health = await probe.ProbeAsync(CancellationToken.None);

        Assert.True(health.Reachable);
        Assert.Null(health.UnavailableDependency);
    }

    [Fact]
    public async Task ProbeAsync_WithZeroBrokers_ReturnsUnreachableWithFixedName()
    {
        var probe = CreateProbe(_ => Task.FromResult(0));

        var health = await probe.ProbeAsync(CancellationToken.None);

        Assert.False(health.Reachable);
        Assert.Equal("Kafka (bootstrap servers)", health.UnavailableDependency);
    }

    [Fact]
    public async Task ProbeAsync_WhenCheckThrows_ReturnsUnreachableWithoutPropagating()
    {
        var probe = CreateProbe(_ => throw new InvalidOperationException("boom"));

        var health = await probe.ProbeAsync(CancellationToken.None);

        Assert.False(health.Reachable);
        Assert.Equal("Kafka (bootstrap servers)", health.UnavailableDependency);
    }

    [Fact]
    public async Task ProbeAsync_WhenCheckFaultsAsynchronously_ReturnsUnreachableWithoutPropagating()
    {
        var probe = CreateProbe(async _ =>
        {
            await Task.Yield();
            throw new InvalidOperationException("boom");
        });

        var health = await probe.ProbeAsync(CancellationToken.None);

        Assert.False(health.Reachable);
    }

    [Fact]
    public async Task ProbeAsync_WhenCheckNeverCompletes_ReturnsUnreachableAfterTimeout()
    {
        var probe = CreateProbe(_ => new TaskCompletionSource<int>().Task);
        var stopwatch = Stopwatch.StartNew();

        var health = await probe.ProbeAsync(CancellationToken.None);

        stopwatch.Stop();
        Assert.False(health.Reachable);
        Assert.Equal("Kafka (bootstrap servers)", health.UnavailableDependency);
        Assert.InRange(stopwatch.Elapsed, TimeSpan.FromMilliseconds(800), TimeSpan.FromSeconds(5));
    }

    [Fact]
    public async Task ProbeAsync_WhenCallerCancels_ReturnsUnreachableWithoutPropagating()
    {
        var probe = CreateProbe(_ => new TaskCompletionSource<int>().Task);
        using var cts = new CancellationTokenSource();
        cts.CancelAfter(TimeSpan.FromMilliseconds(100));

        var health = await probe.ProbeAsync(cts.Token);

        Assert.False(health.Reachable);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public async Task ProbeAsync_WhenUnreachable_DependencyNameNeverContainsSaslCredentials(int mode)
    {
        var probe = CreateProbe(_ => mode == 0
            ? Task.FromResult(0)
            : throw new InvalidOperationException($"{Username}:{Password}"));

        var health = await probe.ProbeAsync(CancellationToken.None);

        Assert.NotNull(health.UnavailableDependency);
        Assert.DoesNotContain(Username, health.UnavailableDependency);
        Assert.DoesNotContain(Password, health.UnavailableDependency);
    }
}
