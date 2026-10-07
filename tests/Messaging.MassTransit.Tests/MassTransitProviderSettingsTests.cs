using Messaging.MassTransit.Configuration;

namespace Messaging.MassTransit.Tests;

public class MassTransitProviderSettingsTests
{
    [Fact]
    public void ToRequiredSettings_Declares_Transport_And_ConnectionString()
    {
        var settings = MassTransitTestSupport.ValidSettings();

        var names = settings.ToRequiredSettings().Select(s => s.Name).ToArray();

        Assert.Contains(MassTransitProviderSettings.TransportSettingName, names);
        Assert.Contains(MassTransitProviderSettings.ConnectionSettingName, names);
    }

    [Fact]
    public void ToRequiredSettings_ForBrokerTransport_MarksConnectionStringRequired()
    {
        var settings = MassTransitTestSupport.ValidSettings(MassTransitTransport.RabbitMq);

        var connection = settings.ToRequiredSettings()
            .Single(s => s.Name == MassTransitProviderSettings.ConnectionSettingName);

        Assert.True(connection.Required);
    }

    [Fact]
    public void ToRequiredSettings_ForInMemoryTransport_DoesNotRequireConnectionString()
    {
        var settings = new MassTransitProviderSettings(MassTransitTransport.InMemory, connectionString: null);

        var connection = settings.ToRequiredSettings()
            .Single(s => s.Name == MassTransitProviderSettings.ConnectionSettingName);

        Assert.False(connection.Required);
    }

    [Fact]
    public void ToRequiredSettings_Transport_ListsEveryPermittedTransport()
    {
        var settings = MassTransitTestSupport.ValidSettings();

        var transport = settings.ToRequiredSettings()
            .Single(s => s.Name == MassTransitProviderSettings.TransportSettingName);

        foreach (var name in Enum.GetNames<MassTransitTransport>())
        {
            Assert.Contains(name, transport.PermittedValuesOrRange);
        }
    }

    [Fact]
    public void Constructor_ForBrokerTransport_WithoutConnectionString_Throws()
    {
        Assert.Throws<ArgumentException>(() =>
            new MassTransitProviderSettings(MassTransitTransport.RabbitMq, connectionString: null));
    }

    [Fact]
    public void Constructor_WhenHostOmitted_FallsBackToTransportName()
    {
        var settings = new MassTransitProviderSettings(MassTransitTransport.AzureServiceBus, connectionString: "sb://x");

        Assert.Equal(MassTransitTransport.AzureServiceBus.ToString(), settings.Host);
    }
}
