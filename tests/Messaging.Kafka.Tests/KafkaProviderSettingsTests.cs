using Messaging.Kafka.Streaming;

namespace Messaging.Kafka.Tests;

public class KafkaProviderSettingsTests
{
    [Fact]
    public void Describe_ReturnsThreeRequiredDescriptors()
    {
        var descriptors = KafkaProviderSettings.Describe();

        Assert.Equal(3, descriptors.Count);
        Assert.All(descriptors, d => Assert.True(d.Required));
    }

    [Fact]
    public void Validate_WithAllValues_DoesNotThrow()
    {
        var settings = KafkaTestSupport.ValidSettings();

        var exception = Record.Exception(settings.Validate);

        Assert.Null(exception);
    }

    [Theory]
    [InlineData("", "group", "topic")]
    [InlineData("  ", "group", "topic")]
    [InlineData("brokers", "", "topic")]
    [InlineData("brokers", "group", "")]
    [InlineData("brokers", "group", "   ")]
    public void Validate_WithMissingRequiredValue_Throws(string brokers, string group, string topic)
    {
        var settings = new KafkaProviderSettings
        {
            BootstrapServers = brokers,
            ConsumerGroupId = group,
            Topic = topic,
        };

        Assert.Throws<ArgumentException>(settings.Validate);
    }
}
