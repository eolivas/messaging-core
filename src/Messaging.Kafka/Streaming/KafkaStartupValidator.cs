using Messaging.Abstractions;
using Messaging.Configuration;
using Messaging.Startup;

namespace Messaging.Kafka.Streaming;

/// <summary>
/// The Kafka-side startup validation entry point (Req 3.1-3.6, 12.4-12.6). In one pass it combines the
/// core <see cref="ConfigurationValidator"/> over <see cref="KafkaProviderSettings.Describe"/>, the
/// blank-value checks the core presence-only check cannot make, and the SASL cross-field rule from
/// <see cref="KafkaProviderSettings.ValidateSecurity"/>, then de-duplicates so each offending setting
/// yields exactly one error. No error carries a credential value: the core validator echoes a value
/// only for descriptors with a permitted set (protocol and mechanism selectors, which are not secrets).
/// </summary>
public static class KafkaStartupValidator
{
    private static readonly string[] NonBlankSettings =
    {
        KafkaProviderSettings.BootstrapServersSettingName,
        KafkaProviderSettings.ConsumerGroupIdSettingName,
        KafkaProviderSettings.TopicSettingName,
    };

    /// <summary>Validates the configuration and returns every distinct error, one per offending setting.</summary>
    /// <param name="configuration">The messaging configuration carrying the Kafka provider settings.</param>
    /// <returns>Zero or more errors, in discovery order.</returns>
    /// <exception cref="ArgumentNullException">If <paramref name="configuration"/> is null.</exception>
    public static IReadOnlyList<ConfigurationError> Validate(MessagingConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        var settings = configuration.ProviderSettings ?? new Dictionary<string, string>();
        var all = new List<ConfigurationError>();

        // 1. Core pass: presence + permitted-set checks.
        all.AddRange(new ConfigurationValidator(KafkaProviderSettings.Describe()).Validate(configuration).Errors);

        // 2. Present-but-blank required values (Req 3.3); absent ones are already reported by the core pass.
        foreach (var name in NonBlankSettings)
        {
            if (settings.TryGetValue(name, out var value) && string.IsNullOrWhiteSpace(value))
            {
                all.Add(new ConfigurationError(name, "Required provider setting must not be empty or whitespace.", null));
            }
        }

        // 3. SASL cross-field rule.
        all.AddRange(KafkaProviderSettings.ValidateSecurity(
            settings.ToDictionary(kv => kv.Key, kv => (string?)kv.Value, StringComparer.Ordinal)));

        // 4. One error per setting; first wins, so the out-of-set error (which carries the permitted set)
        // from the core pass is kept over a later "missing" report for the same setting.
        var seen = new HashSet<string>(StringComparer.Ordinal);
        return all.Where(e => seen.Add(e.SettingName)).ToList();
    }

    /// <summary>
    /// Validates and throws one <see cref="MessagingConfigurationException"/> carrying all errors together.
    /// </summary>
    /// <param name="configuration">The messaging configuration carrying the Kafka provider settings.</param>
    /// <exception cref="MessagingConfigurationException">If any setting is invalid.</exception>
    public static void ValidateOrThrow(MessagingConfiguration configuration)
    {
        var errors = Validate(configuration);
        if (errors.Count > 0)
        {
            throw new MessagingConfigurationException(errors);
        }
    }
}
