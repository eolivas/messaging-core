using Demo.Application.Common;
using Demo.Application.Providers.Queries;
using Demo.Application.Tests.Fakes;

namespace Demo.Application.Tests.Providers;

public class GetProviderDefaultsQueryHandlerTests
{
    private static readonly CancellationToken Ct = CancellationToken.None;

    [Fact]
    public async Task HandleAsync_SupportedProvider_MasksSecretValues()
    {
        var sut = new GetProviderDefaultsQueryHandler(new FakeProviderCatalog());

        var result = await sut.HandleAsync(new GetProviderDefaultsQuery("rabbitmq"), Ct);

        var settings = result.Value.Settings;
        Assert.Equal(SecretMasker.Placeholder, settings.Single(s => s.Name == "Password").Value);
        Assert.Equal("localhost", settings.Single(s => s.Name == "Host").Value);
        Assert.DoesNotContain(settings, s => s.Value == "s3cret");
    }

    [Fact]
    public async Task HandleAsync_UnknownProvider_ReturnsNotFound()
    {
        var sut = new GetProviderDefaultsQueryHandler(new FakeProviderCatalog());

        var result = await sut.HandleAsync(new GetProviderDefaultsQuery("nope"), Ct);

        Assert.Equal(ErrorCategory.NotFound, result.Error.Category);
    }

    [Fact]
    public async Task HandleAsync_UnsupportedProvider_ReturnsValidationWithReason()
    {
        var sut = new GetProviderDefaultsQueryHandler(new FakeProviderCatalog());

        var result = await sut.HandleAsync(new GetProviderDefaultsQuery("azure-service-bus"), Ct);

        Assert.Equal(ErrorCategory.Validation, result.Error.Category);
        Assert.Equal("Not implemented.", result.Error.Message);
    }

    [Fact]
    public async Task HandleAsync_DefaultsUnavailable_ReturnsUnavailable()
    {
        var catalog = new FakeProviderCatalog { DefaultsAvailable = false };
        var sut = new GetProviderDefaultsQueryHandler(catalog);

        var result = await sut.HandleAsync(new GetProviderDefaultsQuery("rabbitmq"), Ct);

        Assert.Equal(ErrorCategory.Unavailable, result.Error.Category);
    }
}
