using Demo.Application.Providers;
using Demo.Infrastructure.Composition;
using Xunit;

namespace Demo.Infrastructure.Tests;

public sealed class ProviderCatalogTests
{
    private readonly ProviderCatalog _catalog = new();

    [Fact]
    public void List_Default_ReturnsThreeSupportedAndUnsupportedAzure()
    {
        var list = _catalog.List();

        Assert.Equal(3, list.Count(d => d.Supported));
        var azure = Assert.Single(list, d => !d.Supported);
        Assert.Equal(ProviderId.AzureServiceBus, azure.Id);
        Assert.Equal(ProviderCatalog.AzureServiceBusUnsupportedReason, azure.UnsupportedReason);
    }

    [Fact]
    public void GetDefaults_UnknownOrAzure_ReturnsNull()
    {
        Assert.Null(_catalog.GetDefaults(ProviderId.AzureServiceBus));
        Assert.Null(_catalog.GetDefaults(new ProviderId("nope")));
    }

    [Theory]
    [InlineData("rabbitmq")]
    [InlineData("amazon-sns-sqs")]
    [InlineData("kafka")]
    public void GetDefaults_Supported_EditableOnlyNumericOrBooleanAndSecretsFlagged(string id)
    {
        var snapshot = _catalog.GetDefaults(new ProviderId(id));

        Assert.NotNull(snapshot);
        foreach (var s in snapshot!.Settings.Where(s => s.Editable))
        {
            Assert.True(bool.TryParse(s.Value, out _) || long.TryParse(s.Value, out _), s.Name);
        }

        Assert.All(snapshot.Settings.Where(s => s.IsSecret), s => Assert.False(s.Editable));
    }

    [Theory]
    [InlineData("rabbitmq")]
    [InlineData("amazon-sns-sqs")]
    [InlineData("kafka")]
    public void Validate_DefaultsAreValid_InvalidRetryIsRejected(string id)
    {
        var provider = new ProviderId(id);
        var effective = _catalog.GetDefaults(provider)!.Settings
            .ToDictionary(s => $"{s.Group}:{s.Name}", s => s.Value);

        Assert.Empty(_catalog.Validate(provider, effective));

        effective[CommonMessagingSettings.RetryMaxAttempts] = "0";
        Assert.Contains(_catalog.Validate(provider, effective), e => e.Field.Contains("MaxAttempts"));
    }
}
