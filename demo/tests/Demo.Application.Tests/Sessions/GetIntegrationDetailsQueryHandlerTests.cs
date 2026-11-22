using Demo.Application.Common;
using Demo.Application.Sessions.Queries;
using Demo.Application.Tests.Fakes;

namespace Demo.Application.Tests.Sessions;

public class GetIntegrationDetailsQueryHandlerTests
{
    private static readonly CancellationToken Ct = CancellationToken.None;

    [Fact]
    public async Task HandleAsync_NoSession_ReturnsConflict()
    {
        using var world = new SessionWorld();
        var sut = new GetIntegrationDetailsQueryHandler(world.Coordinator, world.Cluster);

        var result = await sut.HandleAsync(new GetIntegrationDetailsQuery(), Ct);

        Assert.Equal(ErrorCategory.Conflict, result.Error.Category);
    }

    [Fact]
    public async Task HandleAsync_ActiveSession_ReturnsAppliedConfiguration()
    {
        using var world = new SessionWorld();
        await world.MakeActiveAsync();
        var sut = new GetIntegrationDetailsQueryHandler(world.Coordinator, world.Cluster);

        var result = await sut.HandleAsync(new GetIntegrationDetailsQuery(), Ct);

        Assert.Equal("RabbitMQ", result.Value.ProviderName);
        Assert.Equal(3, result.Value.RetryMaxAttempts);
        Assert.True(result.Value.IdempotencyEnabled);
    }

    [Fact]
    public async Task HandleAsync_HostCannotDescribeItself_ReturnsUnavailable()
    {
        using var world = new SessionWorld();
        await world.MakeActiveAsync();
        world.Cluster.DescribeFailure = new InvalidOperationException("host down");
        var sut = new GetIntegrationDetailsQueryHandler(world.Coordinator, world.Cluster);

        var result = await sut.HandleAsync(new GetIntegrationDetailsQuery(), Ct);

        Assert.Equal(ErrorCategory.Unavailable, result.Error.Category);
        Assert.DoesNotContain("host down", result.Error.Message);
    }
}
