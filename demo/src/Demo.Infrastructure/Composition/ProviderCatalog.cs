using System.Globalization;
using Demo.Application.Providers;

namespace Demo.Infrastructure.Composition;

/// <summary>
/// Infrastructure implementation of <see cref="IProviderCatalog"/>. It lists the three supported
/// compositions and the unsupported Azure Service Bus entry. Validation delegates to each
/// composition, which runs <c>ConfigurationValidator</c> and the provider validators without
/// touching the network, so no bus is built from invalid settings.
/// </summary>
public sealed class ProviderCatalog : IProviderCatalog
{
    public const string AzureServiceBusUnsupportedReason =
        "Messaging Core has no Azure Service Bus transport implementation yet";

    private readonly IReadOnlyList<ProviderDescriptor> _descriptors;
    private readonly IReadOnlyDictionary<ProviderId, IProviderComposition> _compositions;

    public ProviderCatalog()
        : this([new RabbitMqComposition(), new AmazonSnsSqsComposition(), new KafkaComposition()])
    {
    }

    public ProviderCatalog(IEnumerable<IProviderComposition> compositions)
    {
        ArgumentNullException.ThrowIfNull(compositions);
        _compositions = compositions.ToDictionary(c => c.Id);
        _descriptors =
        [
            Supported(ProviderId.RabbitMq, "RabbitMQ"),
            Supported(ProviderId.AmazonSnsSqs, "Amazon SNS/SQS"),
            Supported(ProviderId.Kafka, "Kafka"),
            new ProviderDescriptor(
                ProviderId.AzureServiceBus,
                "Azure Service Bus",
                Supported: false,
                AzureServiceBusUnsupportedReason),
        ];
    }

    public IReadOnlyList<ProviderDescriptor> List() => _descriptors;

    public ProviderSettingsSnapshot? GetDefaults(ProviderId id)
    {
        if (!_compositions.TryGetValue(id, out var composition))
        {
            return null;
        }

        var defaults = composition.Defaults();
        var settings = defaults.Settings
            .Select(s => s with { Editable = s.Editable && IsNumericOrBoolean(s.Value) })
            .ToList();
        return defaults with { Settings = settings };
    }

    public IReadOnlyList<SettingError> Validate(ProviderId id, IReadOnlyDictionary<string, string> effective)
    {
        ArgumentNullException.ThrowIfNull(effective);
        return _compositions.TryGetValue(id, out var composition)
            ? composition.Validate(effective)
            : [];
    }

    /// <summary>Returns the composition for a supported provider, or null.</summary>
    public IProviderComposition? GetComposition(ProviderId id) =>
        _compositions.TryGetValue(id, out var composition) ? composition : null;

    private ProviderDescriptor Supported(ProviderId id, string displayName)
    {
        if (!_compositions.ContainsKey(id))
        {
            throw new ArgumentException($"No composition registered for provider '{id}'.", nameof(id));
        }

        return new ProviderDescriptor(id, displayName, Supported: true, UnsupportedReason: null);
    }

    private static bool IsNumericOrBoolean(string value) =>
        bool.TryParse(value, out _)
        || long.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out _);
}
