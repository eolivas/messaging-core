namespace Messaging.Kafka.Streaming;

/// <summary>
/// The SASL mechanism used when <see cref="KafkaSecurityProtocol.SaslPlaintext"/> or
/// <see cref="KafkaSecurityProtocol.SaslSsl"/> is selected (Req 12.3).
/// </summary>
public enum KafkaSaslMechanism
{
    /// <summary>No SASL mechanism. The default (Req 12.3).</summary>
    None = 0,

    /// <summary>SASL/PLAIN.</summary>
    Plain = 1,

    /// <summary>SASL/SCRAM-SHA-256.</summary>
    ScramSha256 = 2,

    /// <summary>SASL/SCRAM-SHA-512.</summary>
    ScramSha512 = 3,
}
