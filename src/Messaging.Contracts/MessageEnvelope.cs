namespace Messaging.Contracts;

/// <summary>
/// The transport-neutral representation of a message carrying payload plus metadata
/// (Req 4). Immutable: the publisher preserves every field unchanged through to delivery
/// (Req 4.4).
/// </summary>
/// <remarks>
/// <para>
/// Validation approach: invariants are enforced fail-fast by <see cref="Validate"/>, which
/// throws <see cref="InvalidEnvelopeException"/> on the first violated invariant. This keeps
/// the <c>Messaging.Contracts</c> package free of external dependencies (no FluentValidation)
/// and consistent with the "throw domain-specific exceptions, fail fast on invalid state"
/// convention. Callers that require aggregated errors sit in the Application layer and wrap
/// this gate.
/// </para>
/// <para>
/// Additive versioning (Req 4.7): the envelope evolves additively — new optional fields with
/// defaults are appended and <see cref="ContractVersion"/> (default 1) distinguishes revisions,
/// so a consumer bound to an earlier version keeps binding and ignores unrecognized fields.
/// </para>
/// </remarks>
public sealed record MessageEnvelope
{
    /// <summary>Maximum length, in characters, permitted for <see cref="CorrelationId"/> (Req 4.3).</summary>
    public const int MaxCorrelationIdLength = 128;

    /// <summary>Maximum number of entries permitted in <see cref="Headers"/> (Req 4.1).</summary>
    public const int MaxHeaderCount = 64;

    /// <summary>Unique, non-empty message id (Req 4.1).</summary>
    public required Guid MessageId { get; init; }

    /// <summary>Correlation id, 1..128 characters inclusive (Req 4.1, 4.3).</summary>
    public required string CorrelationId { get; init; }

    /// <summary>Non-empty contract identifier used to bind the payload on consume (Req 4.1, 4.5).</summary>
    public required string MessageType { get; init; }

    /// <summary>UTC creation timestamp (Req 4.1).</summary>
    public required DateTimeOffset CreatedAtUtc { get; init; }

    /// <summary>Header collection, 0..64 entries inclusive (Req 4.1).</summary>
    public required IReadOnlyDictionary<string, string> Headers { get; init; }

    /// <summary>Opaque payload bytes; validated against a known contract on consume (Req 4.5).</summary>
    public required ReadOnlyMemory<byte> Payload { get; init; }

    /// <summary>Contract revision, >= 1; supports additive, backward-compatible versioning (Req 4.7).</summary>
    public int ContractVersion { get; init; } = 1;

    /// <summary>
    /// Validates every envelope invariant and throws <see cref="InvalidEnvelopeException"/>
    /// on the first violation (Req 4.1, 4.3, 4.7).
    /// </summary>
    public void Validate()
    {
        if (MessageId == Guid.Empty)
        {
            throw new InvalidEnvelopeException(
                nameof(MessageId),
                "MessageId must be a non-empty GUID.");
        }

        if (string.IsNullOrWhiteSpace(MessageType))
        {
            throw new InvalidEnvelopeException(
                nameof(MessageType),
                "MessageType must be a non-empty, non-whitespace value.");
        }

        if (CreatedAtUtc.Offset != TimeSpan.Zero)
        {
            throw new InvalidEnvelopeException(
                nameof(CreatedAtUtc),
                "CreatedAtUtc must be a UTC timestamp (zero offset).");
        }

        if (CorrelationId is null ||
            CorrelationId.Length < 1 ||
            CorrelationId.Length > MaxCorrelationIdLength)
        {
            throw new InvalidEnvelopeException(
                nameof(CorrelationId),
                $"CorrelationId length must be between 1 and {MaxCorrelationIdLength} characters inclusive.");
        }

        if (Headers is null || Headers.Count > MaxHeaderCount)
        {
            throw new InvalidEnvelopeException(
                nameof(Headers),
                $"Headers count must be between 0 and {MaxHeaderCount} entries inclusive.");
        }

        if (ContractVersion < 1)
        {
            throw new InvalidEnvelopeException(
                nameof(ContractVersion),
                "ContractVersion must be greater than or equal to 1.");
        }
    }
}
