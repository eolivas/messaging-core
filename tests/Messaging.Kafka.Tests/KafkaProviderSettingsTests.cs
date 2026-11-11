using Messaging.Abstractions;
using Messaging.Configuration;
using Messaging.Kafka.Streaming;

namespace Messaging.Kafka.Tests;

public class KafkaProviderSettingsTests
{
    private const string Secret = "s3cr3t-value-do-not-leak";

    [Fact]
    public void Describe_Always_DeclaresThreeRequiredAndFiveOptionalDescriptors()
    {
        var descriptors = KafkaProviderSettings.Describe();

        Assert.Equal(8, descriptors.Count);
        Assert.Equal(3, descriptors.Count(d => d.Required));
        Assert.Equal(5, descriptors.Count(d => !d.Required));
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

    [Fact]
    public void Describe_SecuritySettings_AreOptional()
    {
        var descriptors = KafkaProviderSettings.Describe();

        var security = descriptors.Where(d => d.Name is
            KafkaProviderSettings.SecurityProtocolSettingName or
            KafkaProviderSettings.SaslMechanismSettingName or
            KafkaProviderSettings.SaslUsernameSettingName or
            KafkaProviderSettings.SaslPasswordSettingName or
            KafkaProviderSettings.TlsCaLocationSettingName).ToList();

        Assert.Equal(5, security.Count);
        Assert.All(security, d => Assert.False(d.Required));
    }

    [Fact]
    public void Validate_WithRequiredOnlyAndNoSecuritySettings_YieldsNoErrors()
    {
        // Absent security settings mean Plaintext / no SASL (Req 12.1).
        var bag = RequiredBag();

        var result = ValidateWithCore(bag);

        Assert.Empty(result.Errors);
        Assert.Empty(KafkaProviderSettings.ValidateSecurity(ToNullable(bag)));
    }

    [Fact]
    public void Validate_WithExplicitPlaintextProtocol_YieldsNoErrors()
    {
        var bag = RequiredBag();
        bag[KafkaProviderSettings.SecurityProtocolSettingName] = "Plaintext";

        var result = ValidateWithCore(bag);

        Assert.Empty(result.Errors);
        Assert.Empty(KafkaProviderSettings.ValidateSecurity(ToNullable(bag)));
    }

    [Fact]
    public void Validate_WithOutOfSetProtocol_ReportsSettingNameAndPermittedSet()
    {
        var bag = RequiredBag();
        bag[KafkaProviderSettings.SecurityProtocolSettingName] = "Carrier-Pigeon";

        var result = ValidateWithCore(bag);

        var error = Assert.Single(result.Errors);
        Assert.Equal(KafkaProviderSettings.SecurityProtocolSettingName, error.SettingName);
        Assert.Equal("Plaintext, Ssl, SaslPlaintext, SaslSsl", error.PermittedValuesOrRange);
    }

    [Fact]
    public void Validate_WithOutOfSetMechanism_ReportsSettingNameAndPermittedSet()
    {
        var bag = RequiredBag();
        bag[KafkaProviderSettings.SaslMechanismSettingName] = "Kerberos";

        var result = ValidateWithCore(bag);

        var error = Assert.Single(result.Errors);
        Assert.Equal(KafkaProviderSettings.SaslMechanismSettingName, error.SettingName);
        Assert.Equal("None, Plain, ScramSha256, ScramSha512", error.PermittedValuesOrRange);
    }

    [Theory]
    [InlineData("SaslPlaintext")]
    [InlineData("SaslSsl")]
    public void ValidateSecurity_SaslProtocolWithMechanismNone_ReportsMechanismOnly(string protocol)
    {
        var bag = SaslBag(protocol, "None", "user", Secret);

        var errors = KafkaProviderSettings.ValidateSecurity(bag);

        var error = Assert.Single(errors);
        Assert.Equal(KafkaProviderSettings.SaslMechanismSettingName, error.SettingName);
    }

    [Fact]
    public void ValidateSecurity_SaslProtocolWithMissingMechanism_ReportsMechanism()
    {
        var bag = SaslBag("SaslSsl", null, "user", Secret);

        var errors = KafkaProviderSettings.ValidateSecurity(bag);

        var error = Assert.Single(errors);
        Assert.Equal(KafkaProviderSettings.SaslMechanismSettingName, error.SettingName);
    }

    [Fact]
    public void ValidateSecurity_SaslProtocolWithMissingUsername_ReportsUsernameOnly()
    {
        var bag = SaslBag("SaslSsl", "Plain", null, Secret);

        var errors = KafkaProviderSettings.ValidateSecurity(bag);

        var error = Assert.Single(errors);
        Assert.Equal(KafkaProviderSettings.SaslUsernameSettingName, error.SettingName);
    }

    [Fact]
    public void ValidateSecurity_SaslProtocolWithBlankPassword_ReportsPasswordOnly()
    {
        var bag = SaslBag("SaslSsl", "Plain", "user", "   ");

        var errors = KafkaProviderSettings.ValidateSecurity(bag);

        var error = Assert.Single(errors);
        Assert.Equal(KafkaProviderSettings.SaslPasswordSettingName, error.SettingName);
    }

    [Fact]
    public void ValidateSecurity_SaslProtocolWithNothingElse_ReportsEachMissingSettingSeparately()
    {
        var bag = SaslBag("SaslPlaintext", null, null, null);

        var errors = KafkaProviderSettings.ValidateSecurity(bag);

        Assert.Equal(
            new[]
            {
                KafkaProviderSettings.SaslMechanismSettingName,
                KafkaProviderSettings.SaslUsernameSettingName,
                KafkaProviderSettings.SaslPasswordSettingName,
            },
            errors.Select(e => e.SettingName).ToArray());
    }

    [Fact]
    public void ValidateSecurity_WhenCredentialsPresentButMechanismNone_DoesNotLeakValuesInErrors()
    {
        var bag = SaslBag("SaslSsl", "None", "alice-user", Secret);

        var errors = KafkaProviderSettings.ValidateSecurity(bag);

        Assert.NotEmpty(errors);
        Assert.All(errors, e =>
        {
            Assert.DoesNotContain(Secret, e.Message);
            Assert.DoesNotContain("alice-user", e.Message);
            Assert.DoesNotContain(Secret, e.PermittedValuesOrRange ?? string.Empty);
            Assert.DoesNotContain("alice-user", e.PermittedValuesOrRange ?? string.Empty);
        });
    }

    [Theory]
    [InlineData("Plaintext")]
    [InlineData("Ssl")]
    public void ValidateSecurity_NonSaslProtocol_YieldsNoErrors(string protocol)
    {
        var bag = SaslBag(protocol, null, null, null);

        var errors = KafkaProviderSettings.ValidateSecurity(bag);

        Assert.Empty(errors);
    }

    [Fact]
    public void ValidateSecurity_UnrecognizedProtocol_IsNotTreatedAsSasl()
    {
        var bag = SaslBag("Carrier-Pigeon", null, null, null);

        var errors = KafkaProviderSettings.ValidateSecurity(bag);

        Assert.Empty(errors);
    }

    [Fact]
    public void ValidateSecurity_NullBag_Throws()
    {
        Assert.Throws<ArgumentNullException>(() => KafkaProviderSettings.ValidateSecurity(null!));
    }

    [Theory]
    [InlineData("Plaintext", "None")]
    [InlineData("Ssl", "None")]
    [InlineData("SaslPlaintext", "Plain")]
    [InlineData("SaslSsl", "ScramSha256")]
    [InlineData("SaslSsl", "ScramSha512")]
    public void Validate_WithAllValidSettings_YieldsZeroErrors(string protocol, string mechanism)
    {
        var bag = RequiredBag();
        bag[KafkaProviderSettings.SecurityProtocolSettingName] = protocol;
        bag[KafkaProviderSettings.SaslMechanismSettingName] = mechanism;
        bag[KafkaProviderSettings.SaslUsernameSettingName] = "user";
        bag[KafkaProviderSettings.SaslPasswordSettingName] = Secret;
        bag[KafkaProviderSettings.TlsCaLocationSettingName] = "/etc/ssl/ca.pem";

        var coreResult = ValidateWithCore(bag);
        var crossField = KafkaProviderSettings.ValidateSecurity(ToNullable(bag));

        Assert.Empty(coreResult.Errors);
        Assert.Empty(crossField);
    }

    private static Dictionary<string, string> RequiredBag() => new()
    {
        [KafkaProviderSettings.BootstrapServersSettingName] = "localhost:9092",
        [KafkaProviderSettings.ConsumerGroupIdSettingName] = "group",
        [KafkaProviderSettings.TopicSettingName] = "topic",
    };

    private static Dictionary<string, string?> SaslBag(string protocol, string? mechanism, string? username, string? password)
    {
        var bag = new Dictionary<string, string?>
        {
            [KafkaProviderSettings.SecurityProtocolSettingName] = protocol,
        };

        if (mechanism is not null)
        {
            bag[KafkaProviderSettings.SaslMechanismSettingName] = mechanism;
        }

        if (username is not null)
        {
            bag[KafkaProviderSettings.SaslUsernameSettingName] = username;
        }

        if (password is not null)
        {
            bag[KafkaProviderSettings.SaslPasswordSettingName] = password;
        }

        return bag;
    }

    private static Dictionary<string, string?> ToNullable(Dictionary<string, string> bag) =>
        bag.ToDictionary(kv => kv.Key, kv => (string?)kv.Value);

    private static ConfigurationValidationResult ValidateWithCore(Dictionary<string, string> providerSettings)
    {
        var validator = new ConfigurationValidator(KafkaProviderSettings.Describe());
        var configuration = new MessagingConfiguration
        {
            Mode = MessagingMode.Streaming,
            ProviderSettings = providerSettings,
        };

        return validator.Validate(configuration);
    }
}
