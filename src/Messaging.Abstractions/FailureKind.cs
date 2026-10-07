namespace Messaging.Abstractions;

/// <summary>
/// Classification of a processing failure that decides whether a message is retried or routed
/// straight to the dead-letter sink (Req 6.2, 6.3).
/// </summary>
public enum FailureKind
{
    /// <summary>
    /// A recoverable failure (timeout, connection failure, downstream-unavailable) eligible for
    /// retry under the configured retry policy (Req 6.2).
    /// </summary>
    Transient,

    /// <summary>
    /// A non-recoverable failure (validation failure, malformed payload) routed to the
    /// dead-letter sink without any retry attempt (Req 6.3).
    /// </summary>
    NonTransient,
}
