using Demo.Application.Exceptions;
using Demo.Application.Providers;
using Demo.Application.Sessions;

namespace Demo.Application.Tests;

public class SessionCoordinatorTests
{
    private static readonly CancellationToken Ct = CancellationToken.None;

    [Fact]
    public async Task Start_FromIdle_ThenComplete_IsActive()
    {
        using var sut = new SessionCoordinator();
        await sut.BeginStartAsync(ProviderId.RabbitMq, Ct);
        Assert.Equal(SessionState.Starting, sut.State);
        await sut.CompleteStartAsync(Ct);
        Assert.Equal(SessionState.Active, sut.State);
        Assert.Equal(ProviderId.RabbitMq, sut.ActiveProvider);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Start_WhileNotIdle_ThrowsNamingActiveProvider(bool complete)
    {
        using var sut = new SessionCoordinator();
        await sut.BeginStartAsync(ProviderId.RabbitMq, Ct);
        if (complete)
        {
            await sut.CompleteStartAsync(Ct);
        }

        var ex = await Assert.ThrowsAsync<SessionConflictException>(
            () => sut.BeginStartAsync(ProviderId.Kafka, Ct));
        Assert.Equal("rabbitmq", ex.ActiveProviderId);
        Assert.Contains("rabbitmq", ex.Message);
        Assert.Equal(ProviderId.RabbitMq, sut.ActiveProvider);
    }

    [Fact]
    public async Task EnsureActive_RejectedUnlessActive()
    {
        using var sut = new SessionCoordinator();
        Assert.Throws<SessionConflictException>(sut.EnsureActive);
        await sut.BeginStartAsync(ProviderId.Kafka, Ct);
        Assert.Throws<SessionConflictException>(sut.EnsureActive);
        await sut.CompleteStartAsync(Ct);
        sut.EnsureActive();
    }

    [Fact]
    public async Task FailStart_ReturnsToIdle_KeepsLastError_AndAllowsRestart()
    {
        using var sut = new SessionCoordinator();
        await sut.BeginStartAsync(ProviderId.Kafka, Ct);
        await sut.FailStartAsync(new ProblemSummaryDto("Start failed", "unreachable"), Ct);
        Assert.Equal(SessionState.Idle, sut.State);
        Assert.Null(sut.ActiveProvider);
        Assert.Equal("Start failed", sut.LastError?.Title);

        await sut.BeginStartAsync(ProviderId.RabbitMq, Ct);
        Assert.Null(sut.LastError);
    }

    [Fact]
    public async Task Stop_ReturnsToIdle_AndStopOnIdleIsNoOp()
    {
        using var sut = new SessionCoordinator();
        Assert.False(await sut.BeginStopAsync(Ct));
        await sut.BeginStartAsync(ProviderId.RabbitMq, Ct);
        await sut.CompleteStartAsync(Ct);
        Assert.True(await sut.BeginStopAsync(Ct));
        Assert.Equal(SessionState.Stopping, sut.State);
        await sut.CompleteStopAsync(Ct);
        Assert.Equal(SessionState.Idle, sut.State);
        Assert.Null(sut.ActiveProvider);
    }

    [Fact]
    public async Task FailureModes_DefaultOff_AreHeldPerConsumer_AndResetOnNewSession()
    {
        using var sut = new SessionCoordinator();
        Assert.Equal(FailureMode.Off, sut.GetFailureMode(ConsumerId.Customer));
        sut.SetFailureMode(ConsumerId.Customer, FailureMode.AlwaysFail);
        Assert.Equal(FailureMode.AlwaysFail, sut.GetFailureMode(ConsumerId.Customer));
        Assert.Equal(FailureMode.Off, sut.GetFailureMode(ConsumerId.Communications));

        await sut.BeginStartAsync(ProviderId.RabbitMq, Ct);
        Assert.Equal(FailureMode.Off, sut.GetFailureMode(ConsumerId.Customer));
    }

    [Fact]
    public async Task ConcurrentStarts_ExactlyOneWins()
    {
        using var sut = new SessionCoordinator();
        var results = await Task.WhenAll(Enumerable.Range(0, 20).Select(async _ =>
        {
            try
            {
                await sut.BeginStartAsync(ProviderId.RabbitMq, Ct);
                return true;
            }
            catch (SessionConflictException)
            {
                return false;
            }
        }));
        Assert.Equal(1, results.Count(r => r));
    }
}
