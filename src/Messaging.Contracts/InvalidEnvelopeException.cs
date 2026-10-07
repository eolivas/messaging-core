namespace Messaging.Contracts;

/// <summary>
/// Domain-specific exception raised when a <see cref="MessageEnvelope"/> violates one of its
/// invariants (Req 4.1, 4.3). Fail-fast: validation throws this rather than returning a
/// malformed envelope into the pipeline.
/// </summary>
public sealed class InvalidEnvelopeException : Exception
{
    /// <summary>
    /// The name of the envelope field whose invariant was violated.
    /// </summary>
    public string FieldName { get; }

    public InvalidEnvelopeException(string fieldName, string message)
        : base(message)
    {
        FieldName = fieldName;
    }
}
