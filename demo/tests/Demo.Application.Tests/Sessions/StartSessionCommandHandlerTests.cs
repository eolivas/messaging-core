using Demo.Application.Common;
using Demo.Application.Sessions;
using Demo.Application.Sessions.Commands;
using Demo.Application.Tests.Fakes;
using Microsoft.Extensions.Logging.Abstractions;

namespace Demo.Application.Tests.Sessions;

public class StartSessionCommandHandlerTests
{
    private static readonly CancellationToken Ct = CancellationToken.None;

    [Fact]
    public async Task HandleAsync_ValidCommand_StartsClusterAndBecomesActive()
    {
        using var world = new SessionWorld();
        var overrides = new Dictionary<string, string> { ["Host"] = "broker" };

        var result = await Create(world).HandleAsync(
            new StartSessionCommand("rabbitmq", overrides), Ct);
        await world.Coordinator.WaitForBackgroundAsync();

        Assert.Equal("rabbitmq", result.Value.ProviderId);
        Assert.Equal(SessionState.Active, world.Coordinator.State);
        Assert.Equal(1, world.Cluster.StartCalls);
        Assert.Equal("broker", world.Cluster.LastStart!.EffectiveSettings["Host"]);
    }

    [Fact]
    public async Task HandleAsync_EmptyProviderId_ReturnsValidationAndStaysIdle()
    {
        using var world = new SessionWorld();

        var result = await Create(world).HandleAsync(new StartSessionCommand("", null), Ct);

        Assert.Equal(ErrorCategory.Validation, result.Error.Category);
        Assert.Contains(result.Error.FieldErrors, f => f.Field == "providerId");
        Assert.Equal(SessionState.Idle, world.Coordinator.State);
    }

    [Fact]
    public async Task HandleAsync_UnknownProvider_ReturnsNotFound()
    {
        using var world = new SessionWorld();

        var result = await Create(world).HandleAsync(new StartSessionCommand("nope", null), Ct);

        Assert.Equal(ErrorCategory.NotFound, result.Error.Category);
    }

    [Fact]
    public async Task HandleAsync_SecretOverride_ReturnsValidationAndStaysIdle()
    {
        using var world = new SessionWorld();
        var overrides = new Dictionary<string, string> { ["Password"] = "x" };

        var result = await Create(world).HandleAsync(
            new StartSessionCommand("rabbitmq", overrides), Ct);

        Assert.Equal(ErrorCategory.Validation, result.Error.Category);
        Assert.Equal(SessionState.Idle, world.Coordinator.State);
        Assert.Equal(0, world.Cluster.StartCalls);
    }

    [Fact]
    public async Task HandleAsync_SessionAlreadyActive_ReturnsConflict()
    {
        using var world = new SessionWorld();
        await world.MakeActiveAsync();

        var result = await Create(world).HandleAsync(new StartSessionCommand("kafka", null), Ct);

        Assert.Equal(ErrorCategory.Conflict, result.Error.Category);
        Assert.Equal(0, world.Cluster.StartCalls);
    }

    [Fact]
    public async Task HandleAsync_ClusterStartFails_ReturnsToIdleWithSanitizedLastError()
    {
        using var world = new SessionWorld();
        world.Cluster.StartFailure = new InvalidOperationException("refused Password=hunter2");

        await Create(world).HandleAsync(new StartSessionCommand("rabbitmq", null), Ct);
        await world.Coordinator.WaitForBackgroundAsync();

        Assert.Equal(SessionState.Idle, world.Coordinator.State);
        Assert.Equal("Provider start failed", world.Coordinator.LastError?.Title);
        Assert.DoesNotContain("hunter2", world.Coordinator.LastError!.Detail);
        Assert.Equal(1, world.Cluster.StopCalls);
    }

    private static StartSessionCommandHandler Create(SessionWorld world) =>
        new(
            new StartSessionCommandValidator(),
            world.Catalog,
            world.Coordinator,
            world.Cluster,
            world.FlowLog,
            world.Broadcaster,
            world.Status,
            TimeProvider.System,
            NullLogger<StartSessionCommandHandler>.Instance);
}
