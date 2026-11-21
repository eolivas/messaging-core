using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using FsCheck;
using FsCheck.Fluent;
using FsCheck.Xunit;
using Messaging.Abstractions;
using Messaging.Configuration;
using Messaging.MassTransit.AmazonSqs;
using Xunit;

namespace Messaging.MassTransit.Tests;

// Feature: sns-sqs-provider, Property 13: Startup validation reports each invalid setting exactly once.
// Validates: Requirements 10.11, 13.1, 13.2, 13.3, 13.4, 13.5, 13.6, 13.7, 13.9, 14.7.
//
// For any settings dictionary mixing absent, valid, out-of-range, non-numeric, and malformed values,
// credential combinations, ServiceUrl variants, FIFO name-length pressure, and MaxReceiveCount against
// Retry.MaxAttempts, AmazonSqsStartupValidator.Validate returns an error for exactly the set of invalid
// settings, and no setting is named more than once. The expected set is computed by an oracle that
// reads only the generated dictionary and the documented rules, not the validator.
[Trait("Feature", "sns-sqs-provider")]
[Trait("Property", "13")]
public class AmazonSqsStartupValidationPropertyTests
{
    private const int QueueNameLimit = 80;
    private const int TopicNameLimit = 256;
    private const int FifoSuffixLength = 5;

    private static readonly string[] ValidRegions = { "us-east-1", "eu-west-2", "ap-southeast-1" };

    private static readonly string[] ValidServiceUrls =
    {
        string.Empty,
        "https://sqs.us-east-1.amazonaws.com",
        "http://localhost:4566",
        "http://127.0.0.1:4566",
        "http://localstack:4566",
    };

    // Feature: sns-sqs-provider, Property 13: Startup validation reports each invalid setting exactly once
    [Property(MaxTest = 100, QuietOnSuccess = true, Arbitrary = new[] { typeof(AmazonSqsStartupValidationPropertyTests) })]
    public Property Validate_ForAnySettingsMix_ReportsEachInvalidSettingExactlyOnce(SettingsCase settingsCase)
    {
        var configuration = new MessagingConfiguration
        {
            Mode = MessagingMode.PublishSubscribe,
            Retry = new RetryPolicyConfig { MaxAttempts = settingsCase.MaxAttempts },
            ProviderSettings = settingsCase.Settings,
        };

        var reported = AmazonSqsStartupValidator.Validate(configuration)
            .Select(e => e.SettingName)
            .ToList();
        var expected = ExpectedInvalidSettings(settingsCase.Settings, settingsCase.MaxAttempts);

        var exactlyOnce = reported.Count == reported.Distinct(StringComparer.Ordinal).Count();
        var sameSet = expected.SetEquals(reported);

        return (exactlyOnce && sameSet)
            .ToProperty()
            .Classify(expected.Count == 0, "valid")
            .Classify(expected.Count == 1, "one invalid setting")
            .Classify(expected.Count > 1, "several invalid settings")
            .Label(
                $"maxAttempts={settingsCase.MaxAttempts} " +
                $"expected=[{string.Join(", ", expected.OrderBy(n => n, StringComparer.Ordinal))}] " +
                $"reported=[{string.Join(", ", reported)}]");
    }

    // ----- Oracle -----------------------------------------------------------------------------

    private static HashSet<string> ExpectedInvalidSettings(
        IReadOnlyDictionary<string, string> settings,
        int maxAttempts)
    {
        var invalid = new HashSet<string>(StringComparer.Ordinal);

        // Required settings: absent or blank, plus region format.
        var region = Get(settings, AmazonSqsProviderSettings.RegionSettingName);
        if (region is null || !ValidRegions.Contains(region))
        {
            invalid.Add(AmazonSqsProviderSettings.RegionSettingName);
        }

        if (string.IsNullOrWhiteSpace(Get(settings, AmazonSqsProviderSettings.EndpointNameSettingName)))
        {
            invalid.Add(AmazonSqsProviderSettings.EndpointNameSettingName);
        }

        // ServiceUrl variants.
        var serviceUrl = Get(settings, AmazonSqsProviderSettings.ServiceUrlSettingName);
        if (serviceUrl is not null && !ValidServiceUrls.Contains(serviceUrl))
        {
            invalid.Add(AmazonSqsProviderSettings.ServiceUrlSettingName);
        }

        // Credentials: both or neither.
        var hasKey = Get(settings, AmazonSqsProviderSettings.AccessKeyIdSettingName) is not null;
        var hasSecret = Get(settings, AmazonSqsProviderSettings.SecretAccessKeySettingName) is not null;
        if (hasKey && !hasSecret)
        {
            invalid.Add(AmazonSqsProviderSettings.SecretAccessKeySettingName);
        }
        else if (hasSecret && !hasKey)
        {
            invalid.Add(AmazonSqsProviderSettings.AccessKeyIdSettingName);
        }

        // Bounded integers.
        var receiveText = Get(settings, AmazonSqsProviderSettings.MaxReceiveCountSettingName);
        var receiveCount = AmazonSqsProviderSettings.DefaultMaxReceiveCount;
        var receiveInvalid = receiveText is not null
            && (!int.TryParse(receiveText, NumberStyles.Integer, CultureInfo.InvariantCulture, out receiveCount)
                || receiveCount < 1
                || receiveCount > 1000);
        if (receiveInvalid || receiveCount < maxAttempts)
        {
            invalid.Add(AmazonSqsProviderSettings.MaxReceiveCountSettingName);
        }

        var visibility = Get(settings, AmazonSqsProviderSettings.VisibilityTimeoutSecondsSettingName);
        if (visibility is not null
            && (!int.TryParse(visibility, NumberStyles.Integer, CultureInfo.InvariantCulture, out var seconds)
                || seconds < 1
                || seconds > 43200))
        {
            invalid.Add(AmazonSqsProviderSettings.VisibilityTimeoutSecondsSettingName);
        }

        // Boolean text.
        var fifoText = Get(settings, AmazonSqsProviderSettings.FifoSettingName);
        if (fifoText is not null && fifoText != "true" && fifoText != "false")
        {
            invalid.Add(AmazonSqsProviderSettings.FifoSettingName);
        }

        // FIFO name-length limits (only when FIFO is exactly 'true').
        if (fifoText == "true")
        {
            var topicPrefix = (Get(settings, AmazonSqsProviderSettings.TopicNamePrefixSettingName) ?? string.Empty).Length;
            var queuePrefix = (Get(settings, AmazonSqsProviderSettings.QueueNamePrefixSettingName) ?? string.Empty).Length;
            var suffix = (Get(settings, AmazonSqsProviderSettings.DeadLetterQueueSuffixSettingName)
                ?? AmazonSqsProviderSettings.DefaultDeadLetterQueueSuffix).Length;

            if (topicPrefix + FifoSuffixLength > TopicNameLimit)
            {
                invalid.Add(AmazonSqsProviderSettings.TopicNamePrefixSettingName);
            }

            if (queuePrefix + FifoSuffixLength > QueueNameLimit)
            {
                invalid.Add(AmazonSqsProviderSettings.QueueNamePrefixSettingName);
            }

            if (queuePrefix + suffix + FifoSuffixLength > QueueNameLimit)
            {
                invalid.Add(AmazonSqsProviderSettings.DeadLetterQueueSuffixSettingName);
            }
        }

        return invalid;
    }

    private static string? Get(IReadOnlyDictionary<string, string> settings, string name) =>
        settings.TryGetValue(name, out var value) ? value : null;

    // ----- Generators -------------------------------------------------------------------------

    /// <summary>A generated settings dictionary together with the Retry.MaxAttempts it is checked against.</summary>
    public sealed record SettingsCase(IReadOnlyDictionary<string, string> Settings, int MaxAttempts)
    {
        public override string ToString() =>
            $"MaxAttempts={MaxAttempts}, Settings=[{string.Join("; ", Settings.Select(kv => $"{kv.Key}={Describe(kv.Key, kv.Value)}"))}]";

        // Keep long prefixes and credential-like values out of failure output.
        private static string Describe(string key, string value) =>
            key.EndsWith("Prefix", StringComparison.Ordinal) ? $"<{value.Length} chars>" : value;
    }

    public static Arbitrary<SettingsCase> SettingsCaseArbitrary() => Arb.From(SettingsCaseGen());

    private static Gen<SettingsCase> SettingsCaseGen() =>
        from region in Pick(null, "us-east-1", "eu-west-2", "ap-southeast-1", string.Empty, "  ", "useast1", "US-EAST-1")
        from endpoint in Pick(null, "orders", "billing-1", string.Empty, "  ")
        from serviceUrl in Pick(
            null,
            string.Empty,
            "https://sqs.us-east-1.amazonaws.com",
            "http://localhost:4566",
            "http://127.0.0.1:4566",
            "http://localstack:4566",
            "http://example.com",
            "ftp://localhost",
            "not a url")
        from accessKey in Pick(null, "AKIAEXAMPLE")
        from secretKey in Pick(null, "example-secret")
        from receiveCount in Pick(null, "5", "1", "1000", "0", "1001", "-3", "abc", string.Empty, " 7 ")
        from visibility in Pick(null, "30", "1", "43200", "0", "43201", "x")
        from fifo in Pick(null, "true", "false", "maybe")
        from topicPrefixLength in Gen.Elements(0, 10, 251, 252, 300)
        from queuePrefixLength in Gen.Elements(0, 10, 69, 70, 75, 76, 80, 90)
        from suffix in Pick(null, "_dlq", "_error")
        from smallAttempts in Gen.Elements(true, false)
        from maxAttempts in smallAttempts ? Gen.Choose(1, 10) : Gen.Choose(1, 100)
        select new SettingsCase(
            BuildSettings(
                new (string Name, string? Value)[]
                {
                    (AmazonSqsProviderSettings.RegionSettingName, region),
                    (AmazonSqsProviderSettings.EndpointNameSettingName, endpoint),
                    (AmazonSqsProviderSettings.ServiceUrlSettingName, serviceUrl),
                    (AmazonSqsProviderSettings.AccessKeyIdSettingName, accessKey),
                    (AmazonSqsProviderSettings.SecretAccessKeySettingName, secretKey),
                    (AmazonSqsProviderSettings.MaxReceiveCountSettingName, receiveCount),
                    (AmazonSqsProviderSettings.VisibilityTimeoutSecondsSettingName, visibility),
                    (AmazonSqsProviderSettings.FifoSettingName, fifo),
                    (AmazonSqsProviderSettings.TopicNamePrefixSettingName,
                        topicPrefixLength == 0 ? null : new string('t', topicPrefixLength)),
                    (AmazonSqsProviderSettings.QueueNamePrefixSettingName,
                        queuePrefixLength == 0 ? null : new string('q', queuePrefixLength)),
                    (AmazonSqsProviderSettings.DeadLetterQueueSuffixSettingName, suffix),
                }),
            maxAttempts);

    private static Gen<string?> Pick(params string?[] options) => Gen.Elements(options);

    private static IReadOnlyDictionary<string, string> BuildSettings(IEnumerable<(string Name, string? Value)> entries) =>
        entries
            .Where(e => e.Value is not null)
            .ToDictionary(e => e.Name, e => e.Value!, StringComparer.Ordinal);
}
