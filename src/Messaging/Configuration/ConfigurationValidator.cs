using Messaging.Abstractions;

namespace Messaging.Configuration;

/// <summary>
/// Fail-fast, aggregating implementation of <see cref="IConfigurationValidator"/>. Runs every
/// check in a single pass and collects all failures rather than stopping at the first, so the
/// operator sees the complete problem set in one startup failure (Req 2.2, 2.3, 2.4, 2.5, 2.6).
/// </summary>
/// <remarks>
/// Provider-specific required settings are supplied at construction time as the set of
/// <see cref="ProviderSettingDescriptor"/> declared by the active provider (Req 2.5). Injecting
/// them keeps the port signature (<see cref="IConfigurationValidator.Validate"/>) dependent only
/// on <see cref="MessagingConfiguration"/> while still letting the validator check provider
/// settings — the descriptors cross the layer boundary as an abstraction (DIP), not as a
/// concrete provider reference, so the Application layer stays free of Infrastructure coupling.
/// Numeric bounds come from <see cref="ConfigurationBounds"/> (the single source of truth), so the
/// permitted ranges reported here never drift from the ones the configuration model binds.
/// </remarks>
public sealed class ConfigurationValidator : IConfigurationValidator
{
    private readonly IReadOnlyCollection<ProviderSettingDescriptor> _providerSettingDescriptors;

    /// <summary>
    /// Creates a validator with no provider-specific settings to check (mode-only validation).
    /// </summary>
    public ConfigurationValidator()
        : this(Array.Empty<ProviderSettingDescriptor>())
    {
    }

    /// <summary>
    /// Creates a validator that also checks the given provider-specific required settings against
    /// <see cref="MessagingConfiguration.ProviderSettings"/> (Req 2.5).
    /// </summary>
    /// <param name="providerSettingDescriptors">
    /// The provider-declared settings to validate; typically the active provider's
    /// <see cref="IMessagingProvider.RequiredSettings"/>.
    /// </param>
    public ConfigurationValidator(IReadOnlyCollection<ProviderSettingDescriptor> providerSettingDescriptors)
    {
        _providerSettingDescriptors = providerSettingDescriptors
            ?? throw new ArgumentNullException(nameof(providerSettingDescriptors));
    }

    /// <inheritdoc />
    public ConfigurationValidationResult Validate(MessagingConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        var errors = new List<ConfigurationError>();

        ValidateMode(configuration, errors);
        ValidateNumericSettings(configuration, errors);
        ValidateProviderSettings(configuration, errors);

        return new ConfigurationValidationResult(errors);
    }

    /// <summary>
    /// Checks that <see cref="MessagingConfiguration.Mode"/> is present (Req 1.2, 2.2) and is a
    /// recognized <see cref="MessagingMode"/> value, reporting the full recognized set when it is
    /// not (Req 1.3, 2.4).
    /// </summary>
    private static void ValidateMode(MessagingConfiguration configuration, List<ConfigurationError> errors)
    {
        if (configuration.Mode is null)
        {
            errors.Add(new ConfigurationError(
                nameof(MessagingConfiguration.Mode),
                "Messaging mode is missing; a mode must be configured before startup.",
                PermittedModes));
            return;
        }

        if (!Enum.IsDefined(configuration.Mode.Value))
        {
            errors.Add(new ConfigurationError(
                nameof(MessagingConfiguration.Mode),
                $"Messaging mode '{(int)configuration.Mode.Value}' is not a recognized value.",
                PermittedModes));
        }
    }

    /// <summary>
    /// Validates every bounded numeric setting against its <see cref="ConfigurationBounds"/>
    /// range, collecting an error for each out-of-range value with its permitted range (Req 2.3).
    /// </summary>
    private static void ValidateNumericSettings(MessagingConfiguration configuration, List<ConfigurationError> errors)
    {
        ValidateBound(ConfigurationBounds.MaxAttempts, configuration.Retry.MaxAttempts, errors);
        ValidateBound(ConfigurationBounds.PerAttemptDelaySeconds, configuration.Retry.PerAttemptDelaySeconds, errors);
        ValidateBound(ConfigurationBounds.MaxDispatchAttempts, configuration.Outbox.MaxDispatchAttempts, errors);
        ValidateBound(ConfigurationBounds.RetentionSeconds, configuration.Idempotency.RetentionSeconds, errors);
        ValidateBound(ConfigurationBounds.ShutdownTimeoutSeconds, configuration.Lifecycle.ShutdownTimeoutSeconds, errors);
    }

    /// <summary>
    /// Collects an error when <paramref name="value"/> falls outside <paramref name="bounds"/>,
    /// reporting the setting name together with its permitted range (Req 2.3).
    /// </summary>
    private static void ValidateBound(ConfigurationBounds.Bounds bounds, int value, List<ConfigurationError> errors)
    {
        if (!bounds.Contains(value))
        {
            errors.Add(new ConfigurationError(
                bounds.SettingName,
                $"Value {value} is outside the permitted range.",
                bounds.PermittedRange));
        }
    }

    /// <summary>
    /// Validates provider-declared settings against <see cref="MessagingConfiguration.ProviderSettings"/>:
    /// a required setting absent from the bag is reported missing by name; a present setting whose
    /// value falls outside its permitted set/range is reported with that set/range (Req 2.5).
    /// </summary>
    private void ValidateProviderSettings(MessagingConfiguration configuration, List<ConfigurationError> errors)
    {
        foreach (var descriptor in _providerSettingDescriptors)
        {
            var present = configuration.ProviderSettings.TryGetValue(descriptor.Name, out var value);

            if (!present)
            {
                if (descriptor.Required)
                {
                    errors.Add(new ConfigurationError(
                        descriptor.Name,
                        "Required provider setting is missing.",
                        descriptor.PermittedValuesOrRange));
                }

                // An absent optional setting is valid; nothing to range-check.
                continue;
            }

            if (descriptor.PermittedValuesOrRange is not null
                && !IsWithinPermitted(value, descriptor.PermittedValuesOrRange))
            {
                errors.Add(new ConfigurationError(
                    descriptor.Name,
                    $"Value '{value}' is outside the permitted set or range.",
                    descriptor.PermittedValuesOrRange));
            }
        }
    }

    /// <summary>
    /// Tests a provider setting value against the descriptor's permitted-values-or-range text.
    /// Supports an inclusive numeric range in the form <c>"min..max"</c> and a comma-separated set
    /// of allowed literal values; any other format is treated as unconstrained so the validator
    /// never rejects a value it cannot interpret.
    /// </summary>
    private static bool IsWithinPermitted(string? value, string permittedValuesOrRange)
    {
        value ??= string.Empty;

        var rangeSeparator = permittedValuesOrRange.IndexOf("..", StringComparison.Ordinal);
        if (rangeSeparator > 0)
        {
            var minText = permittedValuesOrRange[..rangeSeparator];
            var maxText = permittedValuesOrRange[(rangeSeparator + 2)..];

            if (int.TryParse(minText, out var min)
                && int.TryParse(maxText, out var max)
                && int.TryParse(value, out var numeric))
            {
                return numeric >= min && numeric <= max;
            }

            // Malformed range text: do not reject a value we cannot interpret.
            return true;
        }

        if (permittedValuesOrRange.Contains(',', StringComparison.Ordinal))
        {
            foreach (var allowed in permittedValuesOrRange.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
            {
                if (string.Equals(allowed, value, StringComparison.Ordinal))
                {
                    return true;
                }
            }

            return false;
        }

        // Single literal permitted value.
        return string.Equals(permittedValuesOrRange.Trim(), value, StringComparison.Ordinal);
    }

    /// <summary>The complete recognized <see cref="MessagingMode"/> set, reported on a bad mode (Req 1.3, 2.4).</summary>
    private static readonly string PermittedModes = string.Join(", ", Enum.GetNames<MessagingMode>());
}
