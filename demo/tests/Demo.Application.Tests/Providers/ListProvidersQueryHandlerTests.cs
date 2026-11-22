using Demo.Application.Providers.Queries;
using Demo.Application.Tests.Fakes;

namespace Demo.Application.Tests.Providers;

public class ListProvidersQueryHandlerTests
{
    [Fact]
    public async Task HandleAsync_UnorderedCatalog_ReturnsFixedDisplayOrder()
    {
        var sut = new ListProvidersQueryHandler(new FakeProviderCatalog());

        var result = await sut.HandleAsync(new ListProvidersQuery(), CancellationToken.None);

        Assert.Equal(
            new[] { "amazon-sns-sqs", "rabbitmq", "kafka", "azure-service-bus" },
            result.Value.Select(p => p.Id));
    }

    [Fact]
    public async Task HandleAsync_UnsupportedProvider_CarriesTrimmedReason()
    {
        var sut = new ListProvidersQueryHandler(new FakeProviderCatalog());

        var result = await sut.HandleAsync(new ListProvidersQuery(), CancellationToken.None);

        var azure = result.Value.Single(p => p.Id == "azure-service-bus");
        Assert.False(azure.Supported);
        Assert.Equal("Not implemented.", azure.UnsupportedReason);
        Assert.All(result.Value.Where(p => p.Supported), p => Assert.Null(p.UnsupportedReason));
    }

    [Fact]
    public async Task HandleAsync_CancelledToken_Throws()
    {
        var sut = new ListProvidersQueryHandler(new FakeProviderCatalog());
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(
            () => sut.HandleAsync(new ListProvidersQuery(), cts.Token));
    }
}
