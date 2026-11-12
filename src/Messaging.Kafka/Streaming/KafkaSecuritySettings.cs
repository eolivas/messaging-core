namespace Messaging.Kafka.Streaming;

/// <summary>
/// Injected-only security material for a secured Kafka cluster. Held separately from the non-secret
/// <see cref="KafkaProviderSettings"/> and bound from the environment / secret store, never from
/// source (Req 12.9, 12.10).
/// </summary>
/// <remarks>
/// <see cref="SaslUsername"/> and <see cref="SaslPassword"/> are secrets. <see cref="ToString"/> is
/// overridden so the compiler-generated record printing can never expose them through logs or
/// exception messages.
/// </remarks>
public sealed record KafkaSecuritySettings
{
    private const string Redacted = "[REDACTED]";

    /// <summary>The wire security protocol. Defaults to <see cref="KafkaSecurityProtocol.Plaintext"/> (Req 12.1).</summary>
    public KafkaSecurityProtocol SecurityProtocol { get; init; } = KafkaSecurityProtocol.Plaintext;

    /// <summary>The SASL mechanism. Defaults to <see cref="KafkaSaslMechanism.None"/> (Req 12.3).</summary>
    public KafkaSaslMechanism SaslMechanism { get; init; } = KafkaSaslMechanism.None;

    /// <summary>The SASL username. Secret: injected, never logged (Req 12.9, 12.10).</summary>
    public string? SaslUsername { get; init; }

    /// <summary>The SASL password. Secret: injected, never logged (Req 12.9, 12.10).</summary>
    public string? SaslPassword { get; init; }

    /// <summary>
    /// Path to the CA certificate used to verify the broker. When absent, the host default trust
    /// store is used (Req 12.7, 12.8).
    /// </summary>
    public string? TlsCaLocation { get; init; }

    /// <summary>
    /// Returns a description that omits credential values; only whether each was supplied is shown.
    /// </summary>
    public override string ToString() =>
        $"{nameof(KafkaSecuritySettings)} {{ {nameof(SecurityProtocol)} = {SecurityProtocol}, " +
        $"{nameof(SaslMechanism)} = {SaslMechanism}, " +
        $"{nameof(SaslUsername)} = {(SaslUsername is null ? "null" : Redacted)}, " +
        $"{nameof(SaslPassword)} = {(SaslPassword is null ? "null" : Redacted)}, " +
        $"{nameof(TlsCaLocation)} = {(TlsCaLocation is null ? "null" : "set")} }}";
}
