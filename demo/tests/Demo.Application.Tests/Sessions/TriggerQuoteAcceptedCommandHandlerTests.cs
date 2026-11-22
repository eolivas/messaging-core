using Demo.Application.Common;
using Demo.Application.Sessions;
using Demo.Application.Sessions.Commands;
using Demo.Application.Tests.Fakes;

namespace Demo.Application.Tests.Sessions;

public class TriggerQuoteAcceptedCommandHandlerTests
{
    private static readonly CancellationToken Ct = CancellationToken.None;

    [Fact]
    public async Task HandleAsync_NoSession_ReturnsConflictWithoutPublishing()
    {
        using var world = new SessionWorld();

        var result = await Create(world).HandleAsync(new TriggerQuoteAcceptedCommand(), Ct);

        Assert.Equal(ErrorCategory.Conflict, result.Error.Category);
        Assert.Equal(0, world.Cluster.Gateway.Calls);
    }

    [Fact]
    public async Task HandleAsync_ActiveSession_PublishesOnceAndReturnsIds()
    {
        using var world = new SessionWorld();
        await world.MakeActiveAsync();

        var result = await Create(world).HandleAsync(new TriggerQuoteAcceptedCommand(), Ct);

        Assert.Equal(new TriggerResultDto("corr-1", "lead-1"), result.Value);
        Assert.Equal(1, world.Cluster.Gateway.Calls);
    }

    [Fact]
    public async Task HandleAsync_PublishFails_ReturnsSanitizedErrorWithCorrelationId()
    {
        using var world = new SessionWorld();
        await world.MakeActiveAsync();
        world.Cluster.Gateway.OnAccept = _ => Task.FromResult(
            new AcceptQuoteOutcome(false, "corr-9", null, "failed with Password=hunter2"));

        var result = await Create(world).HandleAsync(new TriggerQuoteAcceptedCommand(), Ct);

        Assert.Equal(ErrorCategory.Unavailable, result.Error.Category);
        Assert.Equal("corr-9", result.Error.CorrelationId);
        Assert.DoesNotContain("hunter2", result.Error.Message);
        Assert.Equal(1, world.Cluster.Gateway.Calls);
    }

    [Fact]
    public async Task HandleAsync_CallerCancels_PropagatesCancellation()
    {
        using var world = new SessionWorld();
        await world.MakeActiveAsync();
        world.Cluster.Gateway.OnAccept = async token =>
        {
            await Task.Delay(Timeout.Infinite, token);
            throw new InvalidOperationException("Unreachable.");
        };
        using var cts = new CancellationTokenSource();
        var pending = Create(world).HandleAsync(new TriggerQuoteAcceptedCommand(), cts.Token);

        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pending);
    }

    private static TriggerQuoteAcceptedCommandHandler Create(SessionWorld world) =>
        new(world.Coordinator, world.Cluster, TimeProvider.System);
}
