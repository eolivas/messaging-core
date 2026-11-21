using System.Globalization;
using Messaging.Abstractions;
using Messaging.Configuration;
using Messaging.MassTransit.AmazonSqs;
using Messaging.Startup;

namespace Messaging.MassTransit.Tests;

// Validates: Requirements 13.2, 13.3, 13.4, 13.5, 13.7, 13.8, 13.10, 17.4.
public class AmazonSqsProviderSettingsAndValidationTests
{
    private const string SecretKey = "super-secret-key-value-9f3a";
    private const string AccessKey = "AKIAUNIQUEACCESSKEY77";
    private const string SessionToken = "session-token-value-c41d";

    public static TheoryData<string, int, int> BoundedSettings => new()
    {
        { AmazonSqsProviderSettings.MaxReceiveCountSettingName, 1, 1000 },
        { AmazonSqsProviderSettings.VisibilityTimeoutSecondsSettingName, 1, 43200 },
        { AmazonSqsProviderSettings.MessageRetentionSecondsSettingName, 60, 1209600 },
        { AmazonSqsProviderSettings.MaxMessageSizeBytesSettingName, 1, 262144 },
        { AmazonSqsProviderSettings.WaitTimeSecondsSettingName, 0, 20 },
        { AmazonSqsProviderSettings.MaxMessagesPerReceiveSettingName, 1, 10 },
        { AmazonSqsProviderSettings.ConcurrencyLimitSettingName, 1, 1000 },
    };

    // ----- Binding ----------------------------------------------------------------------------

    [Fact]
    public void FromProviderSettings_WithOnlyRequiredSettings_AppliesDocumentedDefaults()
    {
        var settings = AmazonSqsProviderSettings.FromProviderSettings(ValidBag());

        Assert.Equal("us-east-1", settings.Region);
        Assert.Equal("orders", settings.EndpointName);
        Assert.Equal(AmazonSqsProviderSettings.DefaultMaxReceiveCount, settings.MaxReceiveCount);
        Assert.Equal(AmazonSqsProviderSettings.DefaultVisibilityTimeoutSeconds, settings.VisibilityTimeoutSeconds);
        Assert.Equal(AmazonSqsProviderSettings.DefaultDeadLetterQueueSuffix, settings.DeadLetterQueueSuffix);
        Assert.True(settings.AutoCreateTopology);
        Assert.True(settings.RawMessageDelivery);
        Assert.False(settings.Fifo);
        Assert.Null(settings.ServiceUrl);
    }

    [Fact]
    public void FromProviderSettings_WithExplicitValues_BindsThem()
    {
        var bag = ValidBag();
        bag[AmazonSqsProviderSettings.MaxReceiveCountSettingName] = "12";
        bag[AmazonSqsProviderSettings.FifoSettingName] = "true";
        bag[AmazonSqsProviderSettings.FifoGroupSelectionSettingName] = "MessageType";
        bag[AmazonSqsProviderSettings.ServiceUrlSettingName] = "http://localhost:4566";

        var settings = AmazonSqsProviderSettings.FromProviderSettings(bag);

        Assert.Equal(12, settings.MaxReceiveCount);
        Assert.True(settings.Fifo);
        Assert.Equal(FifoGroupSelection.MessageType, settings.FifoGroupSelection);
        Assert.Equal("http://localhost:4566", settings.ServiceUrl);
    }

    [Fact]
    public void FromProviderSettings_WithUninterpretableValues_FallsBackToDefaults()
    {
        var bag = ValidBag();
        bag[AmazonSqsProviderSettings.MaxReceiveCountSettingName] = "abc";
        bag[AmazonSqsProviderSettings.FifoSettingName] = "maybe";

        var settings = AmazonSqsProviderSettings.FromProviderSettings(bag);

        Assert.Equal(AmazonSqsProviderSettings.DefaultMaxReceiveCount, settings.MaxReceiveCount);
        Assert.False(settings.Fifo);
    }

    [Fact]
    public void FromProviderSettings_WithNull_Throws()
    {
        Assert.Throws<ArgumentNullException>(() => AmazonSqsProviderSettings.FromProviderSettings(null!));
    }

    // ----- Descriptors ------------------------------------------------------------------------

    [Fact]
    public void Describe_DeclaresRegionAndEndpointNameAsRequired()
    {
        var required = AmazonSqsProviderSettings.Describe()
            .Where(d => d.Required)
            .Select(d => d.Name)
            .ToArray();

        Assert.Equal(
            new[] { AmazonSqsProviderSettings.RegionSettingName, AmazonSqsProviderSettings.EndpointNameSettingName }
                .OrderBy(n => n, StringComparer.Ordinal),
            required.OrderBy(n => n, StringComparer.Ordinal));
    }

    [Theory]
    [MemberData(nameof(BoundedSettings))]
    public void Describe_ForBoundedSetting_DeclaresRange(string name, int min, int max)
    {
        var descriptor = AmazonSqsProviderSettings.Describe().Single(d => d.Name == name);

        Assert.Equal($"{min}..{max}", descriptor.PermittedValuesOrRange);
    }

    [Fact]
    public void Describe_DeclaresEachSettingOnce_UnderTheAmazonSqsPrefix()
    {
        var names = AmazonSqsProviderSettings.Describe().Select(d => d.Name).ToArray();

        Assert.Equal(names.Length, names.Distinct(StringComparer.Ordinal).Count());
        Assert.All(names, n => Assert.StartsWith(AmazonSqsProviderSettings.Prefix, n, StringComparison.Ordinal));
    }

    [Theory]
    [InlineData(AmazonSqsProviderSettings.ServiceUrlSettingName)]
    [InlineData(AmazonSqsProviderSettings.AccessKeyIdSettingName)]
    [InlineData(AmazonSqsProviderSettings.SecretAccessKeySettingName)]
    [InlineData(AmazonSqsProviderSettings.SessionTokenSettingName)]
    public void Describe_ForSecretBearingSetting_DeclaresNoPermittedRange(string name)
    {
        var descriptor = AmazonSqsProviderSettings.Describe().Single(d => d.Name == name);

        Assert.Null(descriptor.PermittedValuesOrRange);
        Assert.False(descriptor.Required);
    }

    // ----- Valid baseline ---------------------------------------------------------------------

    [Fact]
    public void Validate_WithRequiredSettingsOnly_ReturnsNoErrors()
    {
        Assert.Empty(AmazonSqsStartupValidator.Validate(Configure(ValidBag())));
    }

    [Fact]
    public void Validate_WithNullConfiguration_Throws()
    {
        Assert.Throws<ArgumentNullException>(() => AmazonSqsStartupValidator.Validate(null!));
    }

    [Fact]
    public void ValidateOrThrow_WithInvalidSettings_ThrowsOneExceptionCarryingAllErrors()
    {
        var bag = ValidBag();
        bag.Remove(AmazonSqsProviderSettings.RegionSettingName);
        bag[AmazonSqsProviderSettings.VisibilityTimeoutSecondsSettingName] = "0";

        var ex = Assert.Throws<MessagingConfigurationException>(
            () => AmazonSqsStartupValidator.ValidateOrThrow(Configure(bag)));

        Assert.Equal(2, ex.Errors.Count);
    }

    // ----- Required settings missing (13.2) ---------------------------------------------------

    [Theory]
    [InlineData(AmazonSqsProviderSettings.RegionSettingName)]
    [InlineData(AmazonSqsProviderSettings.EndpointNameSettingName)]
    public void Validate_WhenRequiredSettingMissing_ReportsOnlyThatSetting(string name)
    {
        var bag = ValidBag();
        bag.Remove(name);

        var error = Assert.Single(AmazonSqsStartupValidator.Validate(Configure(bag)));

        Assert.Equal(name, error.SettingName);
    }

    [Theory]
    [InlineData(AmazonSqsProviderSettings.RegionSettingName, "")]
    [InlineData(AmazonSqsProviderSettings.RegionSettingName, "   ")]
    [InlineData(AmazonSqsProviderSettings.EndpointNameSettingName, "")]
    [InlineData(AmazonSqsProviderSettings.EndpointNameSettingName, "  ")]
    public void Validate_WhenRequiredSettingBlank_ReportsOnlyThatSetting(string name, string value)
    {
        var bag = ValidBag();
        bag[name] = value;

        var error = Assert.Single(AmazonSqsStartupValidator.Validate(Configure(bag)));

        Assert.Equal(name, error.SettingName);
    }

    [Theory]
    [InlineData("useast1")]
    [InlineData("US-EAST-1")]
    [InlineData("not a region")]
    public void Validate_WhenRegionMalformed_ReportsRegion(string region)
    {
        var bag = ValidBag();
        bag[AmazonSqsProviderSettings.RegionSettingName] = region;

        var error = Assert.Single(AmazonSqsStartupValidator.Validate(Configure(bag)));

        Assert.Equal(AmazonSqsProviderSettings.RegionSettingName, error.SettingName);
    }

    // ----- Bounded settings (13.3) ------------------------------------------------------------

    [Theory]
    [MemberData(nameof(BoundedSettings))]
    public void Validate_WhenBoundedSettingAtBounds_ReturnsNoErrors(string name, int min, int max)
    {
        foreach (var value in new[] { min, max })
        {
            var bag = ValidBag();
            bag[name] = value.ToString(CultureInfo.InvariantCulture);

            // MaxAttempts 1 keeps the redrive-versus-retry rule out of this bounds check.
            Assert.Empty(AmazonSqsStartupValidator.Validate(Configure(bag, 1)));
        }
    }

    [Theory]
    [MemberData(nameof(BoundedSettings))]
    public void Validate_WhenBoundedSettingOutsideBounds_ReportsThatSettingWithRange(string name, int min, int max)
    {
        foreach (var value in new[] { min - 1, max + 1 })
        {
            var bag = ValidBag();
            bag[name] = value.ToString(CultureInfo.InvariantCulture);

            var error = Assert.Single(AmazonSqsStartupValidator.Validate(Configure(bag, 1)));

            Assert.Equal(name, error.SettingName);
            Assert.Equal($"{min}..{max}", error.PermittedValuesOrRange);
        }
    }

    [Theory]
    [InlineData(AmazonSqsProviderSettings.MaxReceiveCountSettingName)]
    [InlineData(AmazonSqsProviderSettings.VisibilityTimeoutSecondsSettingName)]
    [InlineData(AmazonSqsProviderSettings.MessageRetentionSecondsSettingName)]
    [InlineData(AmazonSqsProviderSettings.MaxMessageSizeBytesSettingName)]
    [InlineData(AmazonSqsProviderSettings.WaitTimeSecondsSettingName)]
    [InlineData(AmazonSqsProviderSettings.MaxMessagesPerReceiveSettingName)]
    [InlineData(AmazonSqsProviderSettings.ConcurrencyLimitSettingName)]
    public void Validate_WhenBoundedSettingNonNumeric_ReportsThatSettingOnce(string name)
    {
        var bag = ValidBag();
        bag[name] = "abc";

        var errors = AmazonSqsStartupValidator.Validate(Configure(bag));

        Assert.Equal(name, Assert.Single(errors).SettingName);
    }

    [Theory]
    [InlineData(AmazonSqsProviderSettings.FifoSettingName)]
    [InlineData(AmazonSqsProviderSettings.AutoCreateTopologySettingName)]
    [InlineData(AmazonSqsProviderSettings.RawMessageDeliverySettingName)]
    public void Validate_WhenBooleanSettingNotBoolean_ReportsThatSetting(string name)
    {
        var bag = ValidBag();
        bag[name] = "maybe";

        Assert.Equal(name, Assert.Single(AmazonSqsStartupValidator.Validate(Configure(bag))).SettingName);
    }

    [Fact]
    public void Validate_WhenFifoGroupSelectionNotPermitted_ReportsThatSetting()
    {
        var bag = ValidBag();
        bag[AmazonSqsProviderSettings.FifoGroupSelectionSettingName] = "Random";

        var error = Assert.Single(AmazonSqsStartupValidator.Validate(Configure(bag)));

        Assert.Equal(AmazonSqsProviderSettings.FifoGroupSelectionSettingName, error.SettingName);
        Assert.Equal("CorrelationId, MessageType", error.PermittedValuesOrRange);
    }

    // ----- FIFO name lengths ------------------------------------------------------------------

    [Theory]
    [InlineData(75, false)]
    [InlineData(76, true)]
    public void Validate_WithFifoAndQueuePrefix_ChecksTheEightyCharacterLimit(int prefixLength, bool expectError)
    {
        var bag = ValidBag();
        bag[AmazonSqsProviderSettings.FifoSettingName] = "true";
        bag[AmazonSqsProviderSettings.QueueNamePrefixSettingName] = new string('q', prefixLength);
        bag[AmazonSqsProviderSettings.DeadLetterQueueSuffixSettingName] = "_e"; // keep the suffix check quiet

        var names = AmazonSqsStartupValidator.Validate(Configure(bag)).Select(e => e.SettingName).ToArray();

        Assert.Equal(expectError, names.Contains(AmazonSqsProviderSettings.QueueNamePrefixSettingName));
    }

    // ----- Redrive versus retry (13.4) --------------------------------------------------------

    [Theory]
    [InlineData(5, 5, false)]
    [InlineData(4, 5, true)]
    [InlineData(6, 5, false)]
    public void Validate_ComparesMaxReceiveCountWithRetryMaxAttempts(int receiveCount, int maxAttempts, bool expectError)
    {
        var bag = ValidBag();
        bag[AmazonSqsProviderSettings.MaxReceiveCountSettingName] = receiveCount.ToString(CultureInfo.InvariantCulture);

        var errors = AmazonSqsStartupValidator.Validate(Configure(bag, maxAttempts));

        if (expectError)
        {
            var error = Assert.Single(errors);
            Assert.Equal(AmazonSqsProviderSettings.MaxReceiveCountSettingName, error.SettingName);
            Assert.Contains(ConfigurationBounds.MaxAttempts.SettingName, error.Message, StringComparison.Ordinal);
        }
        else
        {
            Assert.Empty(errors);
        }
    }

    [Fact]
    public void Validate_WhenMaxReceiveCountAbsent_UsesDefaultAgainstRetryMaxAttempts()
    {
        var below = AmazonSqsStartupValidator.Validate(Configure(ValidBag(), AmazonSqsProviderSettings.DefaultMaxReceiveCount + 1));
        var equal = AmazonSqsStartupValidator.Validate(Configure(ValidBag(), AmazonSqsProviderSettings.DefaultMaxReceiveCount));

        Assert.Equal(AmazonSqsProviderSettings.MaxReceiveCountSettingName, Assert.Single(below).SettingName);
        Assert.Empty(equal);
    }

    // ----- Endpoint override (13.5) -----------------------------------------------------------

    [Theory]
    [InlineData("https://sqs.us-east-1.amazonaws.com")]
    [InlineData("http://localhost:4566")]
    [InlineData("http://127.0.0.1:4566")]
    [InlineData("http://[::1]:4566")]
    [InlineData("http://localstack:4566")]
    [InlineData("http://host.docker.internal:4566")]
    public void Validate_WhenServiceUrlPermitted_ReturnsNoErrors(string url)
    {
        var bag = ValidBag();
        bag[AmazonSqsProviderSettings.ServiceUrlSettingName] = url;

        Assert.Empty(AmazonSqsStartupValidator.Validate(Configure(bag)));
    }

    [Theory]
    [InlineData("not a url")]
    [InlineData("/relative/path")]
    [InlineData("ftp://localhost")]
    [InlineData("http://example.com")]
    [InlineData("http://203.0.113.10:4566")]
    public void Validate_WhenServiceUrlNotPermitted_ReportsServiceUrl(string url)
    {
        var bag = ValidBag();
        bag[AmazonSqsProviderSettings.ServiceUrlSettingName] = url;

        var error = Assert.Single(AmazonSqsStartupValidator.Validate(Configure(bag)));

        Assert.Equal(AmazonSqsProviderSettings.ServiceUrlSettingName, error.SettingName);
    }

    [Fact]
    public void Validate_WhenServiceUrlBlank_TreatsItAsAbsent()
    {
        var bag = ValidBag();
        bag[AmazonSqsProviderSettings.ServiceUrlSettingName] = " ";

        Assert.Empty(AmazonSqsStartupValidator.Validate(Configure(bag)));
    }

    // ----- Credentials (13.7) -----------------------------------------------------------------

    [Fact]
    public void Validate_WithBothCredentials_ReturnsNoErrors()
    {
        var bag = ValidBag();
        bag[AmazonSqsProviderSettings.AccessKeyIdSettingName] = AccessKey;
        bag[AmazonSqsProviderSettings.SecretAccessKeySettingName] = SecretKey;

        Assert.Empty(AmazonSqsStartupValidator.Validate(Configure(bag)));
    }

    [Fact]
    public void Validate_WithAccessKeyOnly_ReportsSecretAccessKey()
    {
        var bag = ValidBag();
        bag[AmazonSqsProviderSettings.AccessKeyIdSettingName] = AccessKey;

        var error = Assert.Single(AmazonSqsStartupValidator.Validate(Configure(bag)));

        Assert.Equal(AmazonSqsProviderSettings.SecretAccessKeySettingName, error.SettingName);
    }

    [Fact]
    public void Validate_WithSecretOnly_ReportsAccessKeyId()
    {
        var bag = ValidBag();
        bag[AmazonSqsProviderSettings.SecretAccessKeySettingName] = SecretKey;

        var error = Assert.Single(AmazonSqsStartupValidator.Validate(Configure(bag)));

        Assert.Equal(AmazonSqsProviderSettings.AccessKeyIdSettingName, error.SettingName);
    }

    // ----- Simultaneous failures (13.10) ------------------------------------------------------

    [Fact]
    public void Validate_WithTwoIndependentFailures_ReportsEachSeparately()
    {
        var bag = ValidBag();
        bag.Remove(AmazonSqsProviderSettings.RegionSettingName);
        bag[AmazonSqsProviderSettings.WaitTimeSecondsSettingName] = "21";
        bag[AmazonSqsProviderSettings.AccessKeyIdSettingName] = AccessKey;

        var names = AmazonSqsStartupValidator.Validate(Configure(bag)).Select(e => e.SettingName).ToArray();

        Assert.Equal(3, names.Length);
        Assert.Contains(AmazonSqsProviderSettings.RegionSettingName, names);
        Assert.Contains(AmazonSqsProviderSettings.WaitTimeSecondsSettingName, names);
        Assert.Contains(AmazonSqsProviderSettings.SecretAccessKeySettingName, names);
    }

    [Fact]
    public void Validate_WhenOneSettingFailsSeveralChecks_ReportsItOnce()
    {
        var bag = ValidBag();
        bag[AmazonSqsProviderSettings.MaxReceiveCountSettingName] = "0"; // out of range and below MaxAttempts

        var errors = AmazonSqsStartupValidator.Validate(Configure(bag, 3));

        Assert.Equal(AmazonSqsProviderSettings.MaxReceiveCountSettingName, Assert.Single(errors).SettingName);
    }

    // ----- Secrets excluded (13.8, 17.4) ------------------------------------------------------

    [Fact]
    public void Validate_WithSecretsAndSeveralFailures_NeverEchoesSecretValues()
    {
        var bag = ValidBag();
        bag[AmazonSqsProviderSettings.AccessKeyIdSettingName] = AccessKey;
        bag[AmazonSqsProviderSettings.SessionTokenSettingName] = SessionToken;
        bag[AmazonSqsProviderSettings.ServiceUrlSettingName] = "http://user:" + SecretKey + "@example.com";
        bag[AmazonSqsProviderSettings.MaxReceiveCountSettingName] = "0";
        bag.Remove(AmazonSqsProviderSettings.RegionSettingName);

        var errors = AmazonSqsStartupValidator.Validate(Configure(bag, 3));

        Assert.NotEmpty(errors);
        foreach (var error in errors)
        {
            var text = error.SettingName + "|" + error.Message + "|" + error.PermittedValuesOrRange;
            Assert.DoesNotContain(SecretKey, text, StringComparison.Ordinal);
            Assert.DoesNotContain(AccessKey, text, StringComparison.Ordinal);
            Assert.DoesNotContain(SessionToken, text, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void ValidateOrThrow_WithSecrets_ExceptionMessageExcludesSecretValues()
    {
        var bag = ValidBag();
        bag[AmazonSqsProviderSettings.SecretAccessKeySettingName] = SecretKey;
        bag[AmazonSqsProviderSettings.ServiceUrlSettingName] = "http://" + SecretKey + ".example.com";

        var ex = Assert.Throws<MessagingConfigurationException>(
            () => AmazonSqsStartupValidator.ValidateOrThrow(Configure(bag)));

        Assert.DoesNotContain(SecretKey, ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(SecretKey, ex.ToString(), StringComparison.Ordinal);
    }

    // ----- Helpers ----------------------------------------------------------------------------

    private static Dictionary<string, string> ValidBag() => new(StringComparer.Ordinal)
    {
        [AmazonSqsProviderSettings.RegionSettingName] = "us-east-1",
        [AmazonSqsProviderSettings.EndpointNameSettingName] = "orders",
    };

    private static MessagingConfiguration Configure(Dictionary<string, string> bag, int maxAttempts = 3) =>
        new()
        {
            Mode = MessagingMode.PublishSubscribe,
            Retry = new RetryPolicyConfig { MaxAttempts = maxAttempts },
            ProviderSettings = bag,
        };
}
