using Demo.Application.Common;
using Demo.Application.Sessions;
using Demo.Application.Sessions.Commands;
using Demo.Application.Tests.Fakes;

namespace Demo.Application.Tests.Sessions;

public class SetFailureModeCommandHandlerTests
{
    private static readonly CancellationToken Ct = CancellationToken.None;

    [Fact]
    public async Task HandleAsync_ValidCommand_AppliesModeToCoordinatorAndCluster()
    {
        using var world = new SessionWorld();
        await world.MakeActiveAsync();
        var sut = new SetFailureModeCommandHandler(world.Coordinator, world.Cluster);

        var result = await sut.HandleAsync(new SetFailureModeCommand("customer", "failonce"), Ct);

        Assert.True(result.IsSuccess);
        Assert.Equal(FailureMode.FailOnce, world.Coordinator.GetFailureMode(ConsumerId.Customer));
        Assert.Equal(
            (ConsumerId.Customer, FailureMode.FailOnce),
            Assert.Single(world.Cluster.FailureModes));
    }

    [Theory]
    [InlineData("Billing", "Off")]
    [InlineData("Customer", "7")]
    [InlineData("", "")]
    public async Task HandleAsync_InvalidValues_ReturnsValidationAndChangesNothing(
        string consumer, string mode)
    {
        using var world = new SessionWorld();
        await world.MakeActiveAsync();
        var sut = new SetFailureModeCommandHandler(world.Coordinator, world.Cluster);

        var result = await sut.HandleAsync(new SetFailureModeCommand(consumer, mode), Ct);

        Assert.Equal(ErrorCategory.Validation, result.Error.Category);
        Assert.NotEmpty(result.Error.FieldErrors);
        Assert.Empty(world.Cluster.FailureModes);
    }

    [Fact]
    public async Task HandleAsync_NoSession_ReturnsConflict()
    {
        using var world = new SessionWorld();
        var sut = new SetFailureModeCommandHandler(world.Coordinator, world.Cluster);

        var result = await sut.HandleAsync(new SetFailureModeCommand("Customer", "Off"), Ct);

        Assert.Equal(ErrorCategory.Conflict, result.Error.Category);
        Assert.Empty(world.Cluster.FailureModes);
    }
}
