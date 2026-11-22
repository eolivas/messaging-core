using Demo.Application.Providers;

namespace Demo.Application.Tests.Fakes;

internal sealed class FakeProviderCatalog : IProviderCatalog
{
    public List<ProviderDescriptor> Descriptors { get; } = new()
    {
        new(ProviderId.Kafka, "Kafka", true, null),
        new(ProviderId.AzureServiceBus, "Azure Service Bus", false, "  Not implemented.  "),
        new(ProviderId.RabbitMq, "RabbitMQ", true, null),
        new(ProviderId.AmazonSnsSqs, "Amazon SNS/SQS", true, null)
    };

    public List<SettingEntry> Settings { get; } = new()
    {
        new("Broker", "Host", "localhost", false, true, null),
        new("Broker", "Port", "5672", false, true, "1-65535"),
        new("Broker", "Password", "s3cret", true, true, null),
        new("Broker", "Mode", "Queue", false, false, null)
    };

    public bool DefaultsAvailable { get; set; } = true;

    public List<SettingError> ValidationErrors { get; } = new();

    public IReadOnlyDictionary<string, string>? LastValidated { get; private set; }

    public IReadOnlyList<ProviderDescriptor> List() => Descriptors;

    public ProviderSettingsSnapshot? GetDefaults(ProviderId id) =>
        DefaultsAvailable ? new ProviderSettingsSnapshot(id, "Queue", Settings) : null;

    public IReadOnlyList<SettingError> Validate(
        ProviderId id, IReadOnlyDictionary<string, string> effective)
    {
        LastValidated = effective;
        return ValidationErrors;
    }
}
