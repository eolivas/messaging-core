namespace Messaging.Persistence.Idempotency;

/// <summary>
/// Thrown when persisting a processed idempotency key fails (Req 7.5). The failure is surfaced —
/// never swallowed — so the consume pipeline does <em>not</em> acknowledge the message and the
/// stored record is left unchanged; the record and the acknowledgement can therefore never diverge.
/// </summary>
/// <remarks>
/// This wraps the underlying persistence error (for example a connection or constraint failure)
/// in a domain-specific exception rather than letting a bare framework exception escape
/// (code-conventions: throw domain-specific exceptions, not bare <see cref="Exception"/>). The
/// idempotency key is carried for diagnostics; it is an opaque derived key, not a secret or PII,
/// so it is safe to include. The original error is preserved as the
/// <see cref="Exception.InnerException"/> for logging where the failure is handled.
/// </remarks>
public sealed class IdempotencyRecordingException : Exception
{
    /// <summary>
    /// Creates the exception for a failed recording of <paramref name="idempotencyKey"/>.
    /// </summary>
    /// <param name="idempotencyKey">The key whose recording failed.</param>
    /// <param name="innerException">The underlying persistence error that caused the failure.</param>
    public IdempotencyRecordingException(string idempotencyKey, Exception innerException)
        : base($"Failed to record the processed idempotency key '{idempotencyKey}'.", innerException)
    {
        IdempotencyKey = idempotencyKey;
    }

    /// <summary>The idempotency key whose recording failed (opaque derived key, safe to log).</summary>
    public string IdempotencyKey { get; }
}
