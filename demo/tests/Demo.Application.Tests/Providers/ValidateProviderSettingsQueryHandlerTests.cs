using Demo.Application.Common;
using Demo.Application.Providers;
using Demo.Application.Providers.Queries;
using Demo.Application.Tests.Fakes;

namespace Demo.Application.Tests.Providers;

public class ValidateProviderSettingsQueryHandlerTests
{
    private static readonly CancellationToken Ct = CancellationToken.None;

    [Fact]
    public async Task HandleAsync_EditableOverride_IsValidatedAndValid()
    {
        var catalog = new FakeProviderCatalog();
        var sut = new ValidateProviderSettingsQueryHandler(catalog);
        var overrides = new Dictionary<string, string> { ["Host"] = "broker" };

        var result = await sut.HandleAsync(
            new ValidateProviderSettingsQuery("rabbitmq", overrides), Ct);

        Assert.True(result.Value.Valid);
        Assert.Equal("broker", catalog.LastValidated!["Host"]);
        Assert.Equal("s3cret", catalog.LastValidated["Password"]);
    }

    [Theory]
    [InlineData("Password", "Secret settings cannot be overridden.")]
    [InlineData("Mode", "The setting is read-only.")]
    [InlineData("Missing", "The setting does not exist.")]
    public async Task HandleAsync_RejectedOverride_ReportsErrorWithoutValidating(
        string name, string message)
    {
        var catalog = new FakeProviderCatalog();
        var sut = new ValidateProviderSettingsQueryHandler(catalog);
        var overrides = new Dictionary<string, string> { [name] = "x" };

        var result = await sut.HandleAsync(
            new ValidateProviderSettingsQuery("rabbitmq", overrides), Ct);

        Assert.False(result.Value.Valid);
        Assert.Equal(message, Assert.Single(result.Value.Errors).Message);
        Assert.Null(catalog.LastValidated);
    }

    [Fact]
    public async Task HandleAsync_LibraryError_FallsBackToSettingPermittedRange()
    {
        var catalog = new FakeProviderCatalog();
        catalog.ValidationErrors.Add(new SettingError("Port", "Out of range.", null));
        var sut = new ValidateProviderSettingsQueryHandler(catalog);

        var result = await sut.HandleAsync(
            new ValidateProviderSettingsQuery("rabbitmq", null), Ct);

        Assert.False(result.Value.Valid);
        Assert.Equal("1-65535", Assert.Single(result.Value.Errors).PermittedValuesOrRange);
    }

    [Fact]
    public async Task HandleAsync_UnknownProvider_ReturnsNotFound()
    {
        var sut = new ValidateProviderSettingsQueryHandler(new FakeProviderCatalog());

        var result = await sut.HandleAsync(new ValidateProviderSettingsQuery("nope", null), Ct);

        Assert.Equal(ErrorCategory.NotFound, result.Error.Category);
    }

    [Fact]
    public async Task HandleAsync_DefaultsUnavailable_ReturnsUnavailable()
    {
        var catalog = new FakeProviderCatalog { DefaultsAvailable = false };
        var sut = new ValidateProviderSettingsQueryHandler(catalog);

        var result = await sut.HandleAsync(
            new ValidateProviderSettingsQuery("rabbitmq", null), Ct);

        Assert.Equal(ErrorCategory.Unavailable, result.Error.Category);
    }
}
