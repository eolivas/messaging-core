using FsCheck;
using FsCheck.Fluent;
using FsCheck.Xunit;
using Messaging.Abstractions;
using Messaging.Configuration;
using Messaging.Kafka.Streaming;

namespace Messaging.Kafka.Tests;

// Feature: kafka-provider, Property 7: Startup validation aggregates all failures and excludes secret values.
// Validates: Requirements 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 12.4, 12.5, 12.6.
//
// For ANY combination of missing / empty / whitespace-only / out-of-set / SASL-without-credential
// settings, the combined startup validation (the core ConfigurationValidator over
// KafkaProviderSettings.Describe() PLUS the static KafkaProviderSettings.ValidateSecurity cross-field
// rule, which the sealed core validator cannot run) reports:
//   * exactly one error per offending setting, identified by its setting name (enum selectors carry
//     their permitted set), in one aggregated pass, and
//   * no error text containing any configured SASL credential value, whole or in any fragment.
[Trait("Feature", "kafka-provider")]
[Trait("Property", "7")]
public class KafkaStartupValidationPropertyTests
{
    private static readonly string[] ProtocolValues = { "Plaintext", "Ssl", "SaslPlaintext", "SaslSsl" };
    private static readonly string[] MechanismValues = { "None", "Plain", "ScramSha256", "ScramSha512" };
    private static readonly string[] InvalidEnumValues = { "", " ", "   ", "\t", "plaintext", "SASL_SSL", "Kerberos", "SaslSSL", "Plain ", "scramsha256" };

    // Credential alphabet deliberately disjoint from letters, digits, '.', '\'' and spaces used by error
    // messages (the permitted-set text "ScramSha256, ScramSha512" contains digit pairs such as "25"/"56"),
    // so any fragment of 2+ characters appearing in an error can only have come from a leaked secret.
    private const string SecretAlphabet = "!@#$%^&*~";

    /// <summary>A generated settings bag plus the credential values that must never be echoed.</summary>
    public sealed record StartupScenario(IReadOnlyDictionary<string, string> Settings, IReadOnlyList<string> Secrets)
    {
        public override string ToString() =>
            string.Join("; ", Settings.Select(kv => $"{kv.Key}=[{kv.Value.Length} chars]"));
    }

    [Property(MaxTest = 200, QuietOnSuccess = true, Arbitrary = new[] { typeof(KafkaStartupValidationPropertyTests) })]
    public Property Validation_ForAnyMisconfiguration_ReportsOneErrorPerOffendingSettingAndNoSecrets(StartupScenario scenario)
    {
        var errors = RunCombinedValidation(scenario.Settings);
        var expected = ExpectedOffendingSettings(scenario.Settings);

        var byName = errors.GroupBy(e => e.SettingName).ToDictionary(g => g.Key, g => g.Count());

        var exactlyOnePerOffender = expected.All(n => byName.TryGetValue(n, out var c) && c == 1);
        var noUnexpectedErrors = byName.Keys.All(expected.Contains);

        var enumErrorsCarryPermittedSet = errors
            .Where(e => e.SettingName is KafkaProviderSettings.SecurityProtocolSettingName or KafkaProviderSettings.SaslMechanismSettingName)
            .All(e => e.PermittedValuesOrRange is not null
                && (e.SettingName == KafkaProviderSettings.SecurityProtocolSettingName ? ProtocolValues : MechanismValues)
                    .All(v => e.PermittedValuesOrRange.Contains(v, StringComparison.Ordinal)));

        var leaked = FindLeakedFragment(errors, scenario.Secrets);

        return (exactlyOnePerOffender && noUnexpectedErrors && enumErrorsCarryPermittedSet && leaked is null)
            .ToProperty()
            .Label(
                $"expected=[{string.Join(", ", expected.OrderBy(x => x))}] " +
                $"actual=[{string.Join(", ", errors.Select(e => e.SettingName).OrderBy(x => x))}] " +
                $"leakedFragment={(leaked is null ? "<none>" : "<redacted:" + leaked.Length + " chars>")} " +
                $"settings=[{scenario}]")
            .Classify(expected.Count == 0, "valid")
            .Classify(expected.Count >= 2, "multiple failures");
    }

    // ----- System under test, combined as the DI path will (task 10.1) -------------------------

    private static IReadOnlyList<ConfigurationError> RunCombinedValidation(IReadOnlyDictionary<string, string> settings)
    {
        var configuration = new MessagingConfiguration
        {
            Mode = MessagingMode.Streaming,
            ProviderSettings = settings,
        };

        return KafkaStartupValidator.Validate(configuration);
    }

    // ----- Oracle -----------------------------------------------------------------------------

    private static HashSet<string> ExpectedOffendingSettings(IReadOnlyDictionary<string, string> s)
    {
        var offenders = new HashSet<string>(StringComparer.Ordinal);

        foreach (var required in new[]
        {
            KafkaProviderSettings.BootstrapServersSettingName,
            KafkaProviderSettings.ConsumerGroupIdSettingName,
            KafkaProviderSettings.TopicSettingName,
        })
        {
            if (IsBlank(s, required))
            {
                offenders.Add(required); // absent, empty or whitespace-only (Req 3.2, 3.3)
            }
        }

        var hasProtocol = s.TryGetValue(KafkaProviderSettings.SecurityProtocolSettingName, out var protocol);
        if (hasProtocol && !ProtocolValues.Contains(protocol, StringComparer.Ordinal))
        {
            offenders.Add(KafkaProviderSettings.SecurityProtocolSettingName); // Req 3.4, 12.4
        }

        var hasMechanism = s.TryGetValue(KafkaProviderSettings.SaslMechanismSettingName, out var mechanism);
        if (hasMechanism && !MechanismValues.Contains(mechanism, StringComparer.Ordinal))
        {
            offenders.Add(KafkaProviderSettings.SaslMechanismSettingName); // Req 3.4, 12.4
        }

        if (protocol is "SaslPlaintext" or "SaslSsl")
        {
            if (!hasMechanism || mechanism == "None" || string.IsNullOrWhiteSpace(mechanism))
            {
                offenders.Add(KafkaProviderSettings.SaslMechanismSettingName); // Req 12.5, 12.6
            }

            if (IsBlank(s, KafkaProviderSettings.SaslUsernameSettingName))
            {
                offenders.Add(KafkaProviderSettings.SaslUsernameSettingName);
            }

            if (IsBlank(s, KafkaProviderSettings.SaslPasswordSettingName))
            {
                offenders.Add(KafkaProviderSettings.SaslPasswordSettingName);
            }
        }

        return offenders;
    }

    private static bool IsBlank(IReadOnlyDictionary<string, string> s, string name) =>
        !s.TryGetValue(name, out var v) || string.IsNullOrWhiteSpace(v);

    /// <summary>Returns the first secret fragment (whole value or any 2+ char substring) found in any error text.</summary>
    private static string? FindLeakedFragment(IReadOnlyList<ConfigurationError> errors, IReadOnlyList<string> secrets)
    {
        var texts = errors.Select(e => $"{e.SettingName}|{e.Message}|{e.PermittedValuesOrRange}").ToList();

        foreach (var secret in secrets)
        {
            for (var start = 0; start < secret.Length; start++)
            {
                for (var length = 2; start + length <= secret.Length; length++)
                {
                    var fragment = secret.Substring(start, length);
                    if (texts.Any(t => t.Contains(fragment, StringComparison.Ordinal)))
                    {
                        return fragment;
                    }
                }
            }
        }

        return null;
    }

    // ----- Generators -------------------------------------------------------------------------

    public static Arbitrary<StartupScenario> StartupScenarioArbitrary() => Arb.From(StartupScenarioGen());

    private static Gen<StartupScenario> StartupScenarioGen() =>
        from bootstrap in RequiredValueGen("broker-1:9092")
        from groupId in RequiredValueGen("group-a")
        from topic in RequiredValueGen("orders")
        from protocol in EnumValueGen(ProtocolValues)
        from mechanism in EnumValueGen(MechanismValues)
        from username in CredentialGen()
        from password in CredentialGen()
        from caLocation in Gen.Elements<string?>(null, "/etc/ssl/ca.pem")
        select Build(bootstrap, groupId, topic, protocol, mechanism, username, password, caLocation);

    private static StartupScenario Build(
        string? bootstrap, string? group, string? topic, string? protocol, string? mechanism,
        string? username, string? password, string? caLocation)
    {
        var settings = new Dictionary<string, string>(StringComparer.Ordinal);
        void Put(string name, string? value)
        {
            if (value is not null)
            {
                settings[name] = value;
            }
        }

        Put(KafkaProviderSettings.BootstrapServersSettingName, bootstrap);
        Put(KafkaProviderSettings.ConsumerGroupIdSettingName, group);
        Put(KafkaProviderSettings.TopicSettingName, topic);
        Put(KafkaProviderSettings.SecurityProtocolSettingName, protocol);
        Put(KafkaProviderSettings.SaslMechanismSettingName, mechanism);
        Put(KafkaProviderSettings.SaslUsernameSettingName, username);
        Put(KafkaProviderSettings.SaslPasswordSettingName, password);
        Put(KafkaProviderSettings.TlsCaLocationSettingName, caLocation);

        // Only non-blank credentials are "configured" values that must never leak.
        var secrets = new[] { username, password }
            .Where(v => !string.IsNullOrWhiteSpace(v))
            .Select(v => v!)
            .ToList();

        return new StartupScenario(settings, secrets);
    }

    /// <summary>A required setting: valid, absent (null), empty, or whitespace-only.</summary>
    private static Gen<string?> RequiredValueGen(string valid) =>
        Gen.Frequency(
            (6, Gen.Constant<string?>(valid)),
            (1, Gen.Constant<string?>(null)),
            (1, Gen.Elements<string?>("", " ", "   ", "\t")));

    /// <summary>An optional enum selector: absent, one permitted value, or an out-of-set/blank value.</summary>
    private static Gen<string?> EnumValueGen(string[] permitted) =>
        Gen.Frequency(
            (3, Gen.Constant<string?>(null)),
            (4, Gen.Elements<string?>(permitted)),
            (3, Gen.Elements<string?>(InvalidEnumValues)));

    /// <summary>A SASL credential: absent, whitespace-only, or a secret string from the leak-detectable alphabet.</summary>
    private static Gen<string?> CredentialGen() =>
        Gen.Frequency(
            (2, Gen.Constant<string?>(null)),
            (1, Gen.Elements<string?>("", " ", "  ")),
            (5, from chars in Gen.NonEmptyListOf(Gen.Elements(SecretAlphabet.ToCharArray()))
                select (string?)("9#" + new string(chars.ToArray()))));
}
