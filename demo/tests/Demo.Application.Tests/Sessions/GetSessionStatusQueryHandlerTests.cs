using Demo.Application.Sessions;
using Demo.Application.Sessions.Queries;
using Demo.Application.Tests.Fakes;

namespace Demo.Application.Tests.Sessions;

public class GetSessionStatusQueryHandlerTests
{
    [Fact]
    public async Task HandleAsync_NoSession_ReturnsIdleStatus()
    {
        using var world = new SessionWorld();
        var sut = new GetSessionStatusQueryHandler(world.Status);

        var result = await sut.HandleAsync(new GetSessionStatusQuery(), CancellationToken.None);

        Assert.Equal("idle", result.Value.State);
        Assert.Null(result.Value.ProviderId);
    }

    [Fact]
    public async Task HandleAsync_ActiveSession_ReportsProviderAndReachableHealth()
    {
        using var world = new SessionWorld();
        await world.MakeActiveAsync();
        var sut = new GetSessionStatusQueryHandler(world.Status);

        var result = await sut.HandleAsync(new GetSessionStatusQuery(), CancellationToken.None);

        Assert.Equal("active", result.Value.State);
        Assert.Equal("rabbitmq", result.Value.ProviderId);
        Assert.Equal(SessionStatusProvider.HealthReachable, result.Value.Health);
    }
}
