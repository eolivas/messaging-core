using Demo.Application.Common;
using Demo.Application.Flow;
using Demo.Application.Sessions.Commands;
using Demo.Application.Tests.Fakes;
using Demo.Contracts;

namespace Demo.Application.Tests.Sessions;

public class ClearFlowCommandHandlerTests
{
    private static readonly CancellationToken Ct = CancellationToken.None;

    [Fact]
    public async Task HandleAsync_NoSession_ReturnsConflictAndKeepsSteps()
    {
        using var world = new SessionWorld();
        world.FlowLog.Append(NewStep());

        var result = await Create(world).HandleAsync(new ClearFlowCommand(), Ct);

        Assert.Equal(ErrorCategory.Conflict, result.Error.Category);
        Assert.Equal(1, world.FlowLog.GetCounters().Published);
    }

    [Fact]
    public async Task HandleAsync_ActiveSession_ClearsLogAndPublishesZeroCounters()
    {
        using var world = new SessionWorld();
        await world.MakeActiveAsync();
        world.FlowLog.Append(NewStep());

        var result = await Create(world).HandleAsync(new ClearFlowCommand(), Ct);

        Assert.True(result.IsSuccess);
        Assert.Empty(world.FlowLog.Replay());
        Assert.Equal(new CountersEvent(0, 0, 0), Assert.Single(world.Broadcaster.Published));
    }

    private static ClearFlowCommandHandler Create(SessionWorld world) =>
        new(world.Coordinator, world.FlowLog, world.Broadcaster);

    private static FlowStep NewStep() =>
        new(MicroserviceId.SalesLeads, FlowStepKind.Published, "corr", "lead", DateTimeOffset.UtcNow);
}
