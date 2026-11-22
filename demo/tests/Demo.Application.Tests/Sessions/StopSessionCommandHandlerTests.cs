using Demo.Application.Common;
using Demo.Application.Flow;
using Demo.Application.Sessions;
using Demo.Application.Sessions.Commands;
using Demo.Application.Tests.Fakes;

namespace Demo.Application.Tests.Sessions;

public class StopSessionCommandHandlerTests
{
    private static readonly CancellationToken Ct = CancellationToken.None;

    [Fact]
    public async Task HandleAsync_NoSession_ReturnsConflictAndDoesNotStopCluster()
    {
        using var world = new SessionWorld();

        var result = await Create(world).HandleAsync(new StopSessionCommand(), Ct);

        Assert.Equal(ErrorCategory.Conflict, result.Error.Category);
        Assert.Equal(0, world.Cluster.StopCalls);
    }

    [Fact]
    public async Task HandleAsync_ActiveSession_StopsClusterAndReturnsIdle()
    {
        using var world = new SessionWorld();
        await world.MakeActiveAsync();

        var result = await Create(world).HandleAsync(new StopSessionCommand(), Ct);

        Assert.Equal("idle", result.Value.State);
        Assert.Equal(SessionState.Idle, world.Coordinator.State);
        Assert.Equal(1, world.Cluster.StopCalls);
    }

    [Fact]
    public async Task HandleAsync_ActiveSession_PublishesStoppingThenIdle()
    {
        using var world = new SessionWorld();
        await world.MakeActiveAsync();

        await Create(world).HandleAsync(new StopSessionCommand(), Ct);

        var states = world.Broadcaster.Published
            .OfType<SessionChangedEvent>()
            .Select(e => e.Status.State)
            .ToArray();
        Assert.Equal(new[] { "stopping", "idle" }, states);
    }

    private static StopSessionCommandHandler Create(SessionWorld world) =>
        new(world.Coordinator, world.Cluster, world.Broadcaster, world.Status, TimeProvider.System);
}
