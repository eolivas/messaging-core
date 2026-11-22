using System.Globalization;
using Demo.Application.Providers;
using Demo.Contracts;
using Messaging.Abstractions;
using Messaging.Configuration;

namespace Demo.Infrastructure.Composition;

/// <summary>
/// The library-wide settings every provider shares (retry, idempotency, dead-letter, lifecycle)
/// and the mapping from effective settings to <see cref="MessagingConfiguration"/>.
/// Keys are <c>Group:Name</c>.
/// </summary>
public static class CommonMessagingSettings
{
    public const string RetryMaxAttempts = "Retry:MaxAttempts";
    public const string RetryBackoff = "Retry:Backoff";
    public const string RetryDelaySeconds = "Retry:PerAttemptDelaySeconds";
    public const string IdempotencyEnabled = "Idempotency:Enabled";
    public const string IdempotencyRetentionSeconds = "Idempotency:RetentionSeconds";
    public const string DeadLetterTarget = "DeadLetter:Target";
    public const string ShutdownTimeoutSeconds = "Lifecycle:ShutdownTimeoutSeconds";

    public static string ServiceName(MicroserviceId service) => service switch
    {
        MicroserviceId.SalesLeads => "sales-leads-ms",
        MicroserviceId.Customer => "customer-ms",
        MicroserviceId.Communications => "communications-ms",
        _ => throw new ArgumentOutOfRangeException(nameof(service), service, "Unknown microservice."),
    };

    public static IReadOnlyList<SettingEntry> Defaults(string deadLetterTarget) =>
    [
        new("Retry", "MaxAttempts", "3", false, true, ConfigurationBounds.MaxAttempts.PermittedRange),
        new("Retry", "Backoff", nameof(BackoffStrategy.Fixed), false, false, null),
        new("Retry", "PerAttemptDelaySeconds", "1", false, true,
            ConfigurationBounds.PerAttemptDelaySeconds.PermittedRange),
        new("Idempotency", "Enabled", "true", false, true, "true, false"),
        new("Idempotency", "RetentionSeconds", "3600", false, true,
            ConfigurationBounds.RetentionSeconds.PermittedRange),
        new("DeadLetter", "Target", deadLetterTarget, false, false, null),
        new("Lifecycle", "ShutdownTimeoutSeconds", "10", false, true,
            ConfigurationBounds.ShutdownTimeoutSeconds.PermittedRange),
    ];

    /// <summary>
    /// Builds the configuration, or returns the errors. Errors never contain setting values.
    /// </summary>
    public static IReadOnlyList<SettingError> TryBuild(
        MessagingMode mode,
        string serviceName,
        IReadOnlyDictionary<string, string> effective,
        string defaultDeadLetterTarget,
        out MessagingConfiguration? configuration)
    {
        ArgumentNullException.ThrowIfNull(effective);
        var errors = new List<SettingError>();
        var attempts = ReadInt(effective, RetryMaxAttempts, 3, ConfigurationBounds.MaxAttempts.PermittedRange, errors);
        var delay = ReadInt(effective, RetryDelaySeconds, 1,
            ConfigurationBounds.PerAttemptDelaySeconds.PermittedRange, errors);
        var retention = ReadInt(effective, IdempotencyRetentionSeconds, 3600,
            ConfigurationBounds.RetentionSeconds.PermittedRange, errors);
        var shutdown = ReadInt(effective, ShutdownTimeoutSeconds, 10,
            ConfigurationBounds.ShutdownTimeoutSeconds.PermittedRange, errors);
        var enabled = ReadBool(effective, IdempotencyEnabled, true, errors);
        var backoff = ReadBackoff(effective, errors);

        if (errors.Count > 0)
        {
            configuration = null;
            return errors;
        }

        var target = effective.TryGetValue(DeadLetterTarget, out var t) && !string.IsNullOrWhiteSpace(t)
            ? t
            : defaultDeadLetterTarget;

        configuration = new MessagingConfiguration
        {
            Mode = mode,
            Retry = new RetryPolicyConfig
            {
                MaxAttempts = attempts,
                Backoff = backoff,
                PerAttemptDelaySeconds = delay,
            },
            Idempotency = new IdempotencyConfig { Enabled = enabled, RetentionSeconds = retention },
            DeadLetter = new DeadLetterConfig { Target = target },
            Lifecycle = new LifecycleConfig { ShutdownTimeoutSeconds = shutdown },
            Observability = new ObservabilityConfig { ServiceName = serviceName },
            ProviderSettings = effective,
        };

        var result = new ConfigurationValidator().Validate(configuration);
        return result.Errors
            .Select(e => new SettingError(e.SettingName, e.Message, e.PermittedValuesOrRange))
            .ToList();
    }

    private static int ReadInt(
        IReadOnlyDictionary<string, string> effective,
        string key,
        int fallback,
        string permitted,
        List<SettingError> errors)
    {
        if (!effective.TryGetValue(key, out var raw))
        {
            return fallback;
        }

        if (int.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var value))
        {
            return value;
        }

        errors.Add(new SettingError(key, "Must be a whole number.", permitted));
        return fallback;
    }

    private static bool ReadBool(
        IReadOnlyDictionary<string, string> effective,
        string key,
        bool fallback,
        List<SettingError> errors)
    {
        if (!effective.TryGetValue(key, out var raw))
        {
            return fallback;
        }

        if (bool.TryParse(raw, out var value))
        {
            return value;
        }

        errors.Add(new SettingError(key, "Must be true or false.", "true, false"));
        return fallback;
    }

    private static BackoffStrategy ReadBackoff(
        IReadOnlyDictionary<string, string> effective,
        List<SettingError> errors)
    {
        if (!effective.TryGetValue(RetryBackoff, out var raw))
        {
            return BackoffStrategy.Fixed;
        }

        if (Enum.TryParse<BackoffStrategy>(raw, ignoreCase: true, out var value) && Enum.IsDefined(value))
        {
            return value;
        }

        errors.Add(new SettingError(
            RetryBackoff,
            "Unknown backoff strategy.",
            string.Join(", ", Enum.GetNames<BackoffStrategy>())));
        return BackoffStrategy.Fixed;
    }
}
