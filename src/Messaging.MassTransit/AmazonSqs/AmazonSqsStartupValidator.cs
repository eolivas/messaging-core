using System.Globalization;
using System.Net;
using System.Text.RegularExpressions;
using Messaging.Abstractions;
using Messaging.Configuration;
using Messaging.Startup;

namespace Messaging.MassTransit.AmazonSqs;

/// <summary>
/// The Amazon SNS/SQS startup validation entry point (Req 13.1-13.9, 10.11, 14.7). In one pure pass it
/// combines the core <see cref="ConfigurationValidator"/> over
/// <see cref="AmazonSqsProviderSettings.Describe"/> with the explicit checks the core pass cannot make,
/// then de-duplicates so each offending setting yields exactly one error (first wins).
/// </summary>
/// <remarks>
/// Errors name settings only. Secret-bearing settings declare no permitted range, so the core validator
/// never echoes their value, and every explicit check here reports the setting name and a fixed
/// description, never the configured value (Req 13.8).
/// </remarks>
public static class AmazonSqsStartupValidator
{
    private const int QueueNameLimit = 80;
    private const int TopicNameLimit = 256;
    private const int FifoSuffixLength = 5; // ".fifo"

    private static readonly Regex RegionPattern = new(
        @"^[a-z]{2}(-[a-z]+){1,2}-\d{1,2}$",
        RegexOptions.CultureInvariant | RegexOptions.Compiled,
        TimeSpan.FromSeconds(1));

    private static readonly string[] IntegerSettings =
    {
        AmazonSqsProviderSettings.MaxReceiveCountSettingName,
        AmazonSqsProviderSettings.VisibilityTimeoutSecondsSettingName,
        AmazonSqsProviderSettings.MessageRetentionSecondsSettingName,
        AmazonSqsProviderSettings.MaxMessageSizeBytesSettingName,
        AmazonSqsProviderSettings.WaitTimeSecondsSettingName,
        AmazonSqsProviderSettings.MaxMessagesPerReceiveSettingName,
        AmazonSqsProviderSettings.ConcurrencyLimitSettingName,
    };

    private static readonly string[] BooleanSettings =
    {
        AmazonSqsProviderSettings.AutoCreateTopologySettingName,
        AmazonSqsProviderSettings.RawMessageDeliverySettingName,
        AmazonSqsProviderSettings.FifoSettingName,
    };

    private static readonly string[] NonBlankWhenPresentSettings =
    {
        AmazonSqsProviderSettings.RegionSettingName,
        AmazonSqsProviderSettings.EndpointNameSettingName,
        AmazonSqsProviderSettings.KmsMasterKeyIdSettingName,
    };

    /// <summary>Validates the configuration and returns every distinct error, one per offending setting.</summary>
    /// <param name="configuration">The messaging configuration carrying the Amazon SQS provider settings.</param>
    /// <returns>Zero or more errors, in discovery order.</returns>
    /// <exception cref="ArgumentNullException">If <paramref name="configuration"/> is null.</exception>
    public static IReadOnlyList<ConfigurationError> Validate(MessagingConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        var settings = configuration.ProviderSettings ?? new Dictionary<string, string>();
        var all = new List<ConfigurationError>();

        // 1. Core pass: presence plus permitted set or range.
        all.AddRange(new ConfigurationValidator(AmazonSqsProviderSettings.Describe())
            .Validate(configuration).Errors);

        // 2. Explicit checks the core pass does not make.
        CheckNumericText(settings, all);
        CheckBooleanText(settings, all);
        CheckBlankValues(settings, all);
        CheckRegion(settings, all);
        CheckServiceUrl(settings, all);

        // 3. Credentials: both keys or neither.
        CheckCredentials(settings, all);

        // 4. FIFO naming limits with the configured prefix or suffix.
        CheckFifoNameLengths(settings, all);

        // 5. Redrive count must allow every configured retry attempt.
        CheckMaxReceiveCount(settings, configuration.Retry.MaxAttempts, all);

        // One error per setting; first wins, so the out-of-range error from the core pass is kept.
        var seen = new HashSet<string>(StringComparer.Ordinal);
        return all.Where(e => seen.Add(e.SettingName)).ToList();
    }

    /// <summary>
    /// Validates and throws one <see cref="MessagingConfigurationException"/> carrying all errors together.
    /// </summary>
    /// <param name="configuration">The messaging configuration carrying the Amazon SQS provider settings.</param>
    /// <exception cref="MessagingConfigurationException">If any setting is invalid.</exception>
    public static void ValidateOrThrow(MessagingConfiguration configuration)
    {
        var errors = Validate(configuration);
        if (errors.Count > 0)
        {
            throw new MessagingConfigurationException(errors);
        }
    }

    private static void CheckNumericText(IReadOnlyDictionary<string, string> settings, List<ConfigurationError> errors)
    {
        foreach (var name in IntegerSettings)
        {
            if (settings.TryGetValue(name, out var value) && !TryInt(value, out _))
            {
                errors.Add(new ConfigurationError(name, "Value must be an integer.", RangeOf(name)));
            }
        }
    }

    private static void CheckBooleanText(IReadOnlyDictionary<string, string> settings, List<ConfigurationError> errors)
    {
        foreach (var name in BooleanSettings)
        {
            if (settings.TryGetValue(name, out var value) && !bool.TryParse(value?.Trim(), out _))
            {
                errors.Add(new ConfigurationError(name, "Value must be 'true' or 'false'.", "true, false"));
            }
        }
    }

    private static void CheckBlankValues(IReadOnlyDictionary<string, string> settings, List<ConfigurationError> errors)
    {
        // Absent values are already reported by the core pass; this catches present-but-blank ones.
        foreach (var name in NonBlankWhenPresentSettings)
        {
            if (settings.TryGetValue(name, out var value) && string.IsNullOrWhiteSpace(value))
            {
                errors.Add(new ConfigurationError(name, "Setting must not be empty or whitespace.", null));
            }
        }
    }

    private static void CheckRegion(IReadOnlyDictionary<string, string> settings, List<ConfigurationError> errors)
    {
        if (settings.TryGetValue(AmazonSqsProviderSettings.RegionSettingName, out var region)
            && !string.IsNullOrWhiteSpace(region)
            && !RegionPattern.IsMatch(region.Trim()))
        {
            errors.Add(new ConfigurationError(
                AmazonSqsProviderSettings.RegionSettingName,
                "Value is not a valid AWS region identifier (for example 'us-east-1').",
                null));
        }
    }

    private static void CheckServiceUrl(IReadOnlyDictionary<string, string> settings, List<ConfigurationError> errors)
    {
        const string name = AmazonSqsProviderSettings.ServiceUrlSettingName;
        if (!settings.TryGetValue(name, out var value) || string.IsNullOrWhiteSpace(value))
        {
            return;
        }

        if (!Uri.TryCreate(value.Trim(), UriKind.Absolute, out var uri)
            || (uri.Scheme != Uri.UriSchemeHttp && uri.Scheme != Uri.UriSchemeHttps)
            || string.IsNullOrEmpty(uri.Host))
        {
            errors.Add(new ConfigurationError(name, "Value must be an absolute http or https URI.", null));
            return;
        }

        if (uri.Scheme == Uri.UriSchemeHttp && !IsLocalHost(uri))
        {
            errors.Add(new ConfigurationError(
                name,
                "An http URI is permitted only for a loopback or container-network host; use https.",
                null));
        }
    }

    private static bool IsLocalHost(Uri uri)
    {
        var host = uri.Host.Trim('[', ']').TrimEnd('.');

        if (IPAddress.TryParse(host, out var address))
        {
            return IPAddress.IsLoopback(address);
        }

        return host.Equals("localhost", StringComparison.OrdinalIgnoreCase)
            || host.EndsWith(".localhost", StringComparison.OrdinalIgnoreCase)
            || host.Equals("host.docker.internal", StringComparison.OrdinalIgnoreCase)
            || host.Equals("host.containers.internal", StringComparison.OrdinalIgnoreCase)
            || !host.Contains('.', StringComparison.Ordinal); // single-label compose service name
    }

    private static void CheckCredentials(IReadOnlyDictionary<string, string> settings, List<ConfigurationError> errors)
    {
        var hasKey = HasText(settings, AmazonSqsProviderSettings.AccessKeyIdSettingName);
        var hasSecret = HasText(settings, AmazonSqsProviderSettings.SecretAccessKeySettingName);

        if (hasKey != hasSecret)
        {
            errors.Add(new ConfigurationError(
                hasKey
                    ? AmazonSqsProviderSettings.SecretAccessKeySettingName
                    : AmazonSqsProviderSettings.AccessKeyIdSettingName,
                $"'{AmazonSqsProviderSettings.AccessKeyIdSettingName}' and " +
                $"'{AmazonSqsProviderSettings.SecretAccessKeySettingName}' must be supplied together or not at all.",
                null));
        }
    }

    private static void CheckFifoNameLengths(IReadOnlyDictionary<string, string> settings, List<ConfigurationError> errors)
    {
        if (!settings.TryGetValue(AmazonSqsProviderSettings.FifoSettingName, out var fifoText)
            || !bool.TryParse(fifoText?.Trim(), out var fifo)
            || !fifo)
        {
            return;
        }

        var topicPrefix = Length(settings, AmazonSqsProviderSettings.TopicNamePrefixSettingName);
        var queuePrefix = Length(settings, AmazonSqsProviderSettings.QueueNamePrefixSettingName);
        var suffix = settings.TryGetValue(AmazonSqsProviderSettings.DeadLetterQueueSuffixSettingName, out var s)
            && !string.IsNullOrWhiteSpace(s)
                ? s.Length
                : AmazonSqsProviderSettings.DefaultDeadLetterQueueSuffix.Length;

        if (topicPrefix + FifoSuffixLength > TopicNameLimit)
        {
            errors.Add(FifoError(AmazonSqsProviderSettings.TopicNamePrefixSettingName, TopicNameLimit));
        }

        if (queuePrefix + FifoSuffixLength > QueueNameLimit)
        {
            errors.Add(FifoError(AmazonSqsProviderSettings.QueueNamePrefixSettingName, QueueNameLimit));
        }

        if (queuePrefix + suffix + FifoSuffixLength > QueueNameLimit)
        {
            errors.Add(FifoError(AmazonSqsProviderSettings.DeadLetterQueueSuffixSettingName, QueueNameLimit));
        }
    }

    private static ConfigurationError FifoError(string name, int limit) =>
        new(
            name,
            string.Create(
                CultureInfo.InvariantCulture,
                $"With FIFO enabled, this value leaves no room for a name within {limit} characters including '.fifo'."),
            null);

    private static void CheckMaxReceiveCount(
        IReadOnlyDictionary<string, string> settings,
        int maxAttempts,
        List<ConfigurationError> errors)
    {
        var maxReceiveCount = AmazonSqsProviderSettings.DefaultMaxReceiveCount;
        if (settings.TryGetValue(AmazonSqsProviderSettings.MaxReceiveCountSettingName, out var text))
        {
            if (!TryInt(text, out maxReceiveCount))
            {
                return; // reported as non-numeric
            }
        }

        if (maxReceiveCount < maxAttempts)
        {
            errors.Add(new ConfigurationError(
                AmazonSqsProviderSettings.MaxReceiveCountSettingName,
                $"Value must not be below '{ConfigurationBounds.MaxAttempts.SettingName}'.",
                null));
        }
    }

    private static string? RangeOf(string name) =>
        AmazonSqsProviderSettings.Describe().First(d => d.Name == name).PermittedValuesOrRange;

    private static bool TryInt(string? value, out int result) =>
        int.TryParse(value?.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out result);

    private static bool HasText(IReadOnlyDictionary<string, string> settings, string name) =>
        settings.TryGetValue(name, out var value) && !string.IsNullOrWhiteSpace(value);

    private static int Length(IReadOnlyDictionary<string, string> settings, string name) =>
        settings.TryGetValue(name, out var value) ? value?.Length ?? 0 : 0;
}
