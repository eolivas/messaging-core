namespace Messaging.Kafka.Streaming;

/// <summary>
/// The wire security protocol the Kafka clients use to talk to the cluster (Req 12.1).
/// </summary>
public enum KafkaSecurityProtocol
{
    /// <summary>Unauthenticated, unencrypted. The default for local development (Req 12.1).</summary>
    Plaintext = 0,

    /// <summary>TLS-encrypted, no SASL authentication.</summary>
    Ssl = 1,

    /// <summary>SASL authentication over an unencrypted channel.</summary>
    SaslPlaintext = 2,

    /// <summary>SASL authentication over a TLS-encrypted channel.</summary>
    SaslSsl = 3,
}
