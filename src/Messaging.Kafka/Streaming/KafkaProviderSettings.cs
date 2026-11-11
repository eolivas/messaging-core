using Messaging.Abstractions;

namespace Messaging.Kafka.Streaming;

/// <summary>
/// The provider-specific settings the Streaming (Kafka) adapter needs: which brokers to connect to,
/// which consumer group the host joins, and which topic it produces to / consumes from. These are
/// declared to the configuration validator through <see cref="Describe"/> so missing or malformed
/// values fail fast at startup (Req 2.5).
/// </summary>
/// <remarks>
/// Setting names are the configuration keys the validator reports by (Req 2.5). None of these values
/// are secrets: SASL credentials and TLS material for a secured cluster are injected into the
/// concrete producer/consumer behind the seam ports, not held here, and are never logged
/// (code-conventions security).
/// </remarks>
public sealed record KafkaProviderSettings
{
    /// <summary>The configuration key for the Kafka bootstrap servers.</summary>
    public const string BootstrapServersSettingName = "Kafka:BootstrapServers";

    /// <summary>The configuration key for the consumer group id the host joins.</summary>
    public const string ConsumerGroupIdSettingName = "Kafka:ConsumerGroupId";

    /// <summary>The configuration key for the topic the adapter produces to and consumes from.</summary>
    public const string TopicSettingName = "Kafka:Topic";

    /// <summary>The configuration key for the wire security protocol (optional).</summary>
    public const string SecurityProtocolSettingName = "Kafka:SecurityProtocol";

    /// <summary>The configuration key for the SASL mechanism (optional).</summary>
    public const string SaslMechanismSettingName = "Kafka:SaslMechanism";

    /// <summary>The configuration key for the SASL username (optional, secret).</summary>
    public const string SaslUsernameSettingName = "Kafka:SaslUsername";

    /// <summary>The configuration key for the SASL password (optional, secret).</summary>
    public const string SaslPasswordSettingName = "Kafka:SaslPassword";

    /// <summary>The configuration key for the broker CA certificate path (optional).</summary>
    public const string TlsCaLocationSettingName = "Kafka:TlsCaLocation";

    private const string PermittedSecurityProtocols = "Plaintext, Ssl, SaslPlaintext, SaslSsl";

    private const string PermittedSaslMechanisms = "None, Plain, ScramSha256, ScramSha512";

    /// <summary>The bootstrap servers list (host:port[,host:port...]) the client connects to.</summary>
    public required string BootstrapServers { get; init; }

    /// <summary>
    /// The consumer group id the host joins. Group membership is what gives streaming its
    /// partition-assignment and offset-tracking semantics, so it is required (Req 6.7).
    /// </summary>
    public required string ConsumerGroupId { get; init; }

    /// <summary>The topic the publisher produces to and the consumer host subscribes to.</summary>
    public required string Topic { get; init; }

    /// <summary>
    /// Declares the provider-specific required settings so the configuration validator can check
    /// their presence and permitted values during startup validation (Req 2.5). This set is what the
    /// provider's <see cref="IMessagingProvider.RequiredSettings"/> exposes and does not depend on any
    /// instance values.
    /// </summary>
    public static IReadOnlyCollection<ProviderSettingDescriptor> Describe() => new[]
    {
        new ProviderSettingDescriptor(BootstrapServersSettingName, Required: true, PermittedValuesOrRange: "host:port[,host:port...]"),
        new ProviderSettingDescriptor(ConsumerGroupIdSettingName, Required: true, PermittedValuesOrRange: null),
        new ProviderSettingDescriptor(TopicSettingName, Required: true, PermittedValuesOrRange: null),

        // Optional security settings (Req 3.x, 12.1-12.6). Absent means Plaintext / no SASL / host trust store.
        new ProviderSettingDescriptor(SecurityProtocolSettingName, Required: false, PermittedValuesOrRange: PermittedSecurityProtocols),
        new ProviderSettingDescriptor(SaslMechanismSettingName, Required: false, PermittedValuesOrRange: PermittedSaslMechanisms),
        new ProviderSettingDescriptor(SaslUsernameSettingName, Required: false, PermittedValuesOrRange: null),
        new ProviderSettingDescriptor(SaslPasswordSettingName, Required: false, PermittedValuesOrRange: null),
        new ProviderSettingDescriptor(TlsCaLocationSettingName, Required: false, PermittedValuesOrRange: null),
    };

    /// <summary>
    /// Evaluates the SASL cross-field rule against a provider-settings bag (Req 12.4-12.6): when the
    /// security protocol is <c>SaslPlaintext</c> or <c>SaslSsl</c>, a mechanism other than <c>None</c>,
    /// a username and a password are all required. Emits one error per absent setting, identified by
    /// setting name only; no setting value (credential or otherwise) is ever placed in an error.
    /// Value-set checks for each individual setting remain the validator's job via <see cref="Describe"/>;
    /// an unrecognized protocol is therefore not treated as SASL here.
    /// </summary>
    /// <param name="providerSettings">The configured provider settings bag.</param>
    /// <returns>Zero or more errors; empty when the rule is satisfied or no SASL protocol is selected.</returns>
    public static IReadOnlyList<ConfigurationError> ValidateSecurity(IReadOnlyDictionary<string, string?> providerSettings)
    {
        ArgumentNullException.ThrowIfNull(providerSettings);

        providerSettings.TryGetValue(SecurityProtocolSettingName, out var protocol);
        var isSasl = string.Equals(protocol, nameof(KafkaSecurityProtocol.SaslPlaintext), StringComparison.Ordinal)
            || string.Equals(protocol, nameof(KafkaSecurityProtocol.SaslSsl), StringComparison.Ordinal);
        if (!isSasl)
        {
            return Array.Empty<ConfigurationError>();
        }

        var errors = new List<ConfigurationError>();

        providerSettings.TryGetValue(SaslMechanismSettingName, out var mechanism);
        if (string.IsNullOrWhiteSpace(mechanism)
            || string.Equals(mechanism, nameof(KafkaSaslMechanism.None), StringComparison.Ordinal))
        {
            errors.Add(new ConfigurationError(
                SaslMechanismSettingName,
                "A SASL mechanism other than None is required when the security protocol is a SASL protocol.",
                PermittedSaslMechanisms));
        }

        AddIfAbsent(providerSettings, SaslUsernameSettingName, errors);
        AddIfAbsent(providerSettings, SaslPasswordSettingName, errors);

        return errors;
    }

    private static void AddIfAbsent(
        IReadOnlyDictionary<string, string?> providerSettings,
        string settingName,
        List<ConfigurationError> errors)
    {
        if (!providerSettings.TryGetValue(settingName, out var value) || string.IsNullOrWhiteSpace(value))
        {
            errors.Add(new ConfigurationError(
                settingName,
                "Setting is required when the security protocol is a SASL protocol.",
                null));
        }
    }

    /// <summary>
    /// Validates the shell's own invariants fail-fast (Req 2.5): the three required values must be
    /// present and non-whitespace. The validator checks presence from <see cref="Describe"/>; this
    /// guard protects the shell so a misconfigured client never reaches the produce/consume loop.
    /// </summary>
    /// <exception cref="ArgumentException">If any required setting is null, empty, or whitespace.</exception>
    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(BootstrapServers))
        {
            throw new ArgumentException(
                "BootstrapServers must be a non-empty, non-whitespace value.",
                nameof(BootstrapServers));
        }

        if (string.IsNullOrWhiteSpace(ConsumerGroupId))
        {
            throw new ArgumentException(
                "ConsumerGroupId must be a non-empty, non-whitespace value.",
                nameof(ConsumerGroupId));
        }

        if (string.IsNullOrWhiteSpace(Topic))
        {
            throw new ArgumentException(
                "Topic must be a non-empty, non-whitespace value.",
                nameof(Topic));
        }
    }
}
