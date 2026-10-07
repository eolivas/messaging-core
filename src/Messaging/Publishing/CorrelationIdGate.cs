using Messaging.Contracts;

namespace Messaging.Publishing;

/// <summary>
/// The first stage of the publish pipeline: it guarantees every outgoing <see cref="MessageEnvelope"/>
/// carries a valid correlation id before any later validation or delivery runs (Req 4.2, 4.3, 9.3).
/// </summary>
/// <remarks>
/// <para>
/// Single responsibility (SRP): this type does one thing — normalize the correlation id. It does
/// not validate the rest of the envelope (that is the envelope validation gate in task 8.2) and it
/// does not deliver anything. It sits in the Application layer because generating/validating the
/// correlation id is a cross-cutting policy applied uniformly across every messaging mode, not a
/// transport concern.
/// </para>
/// <para>
/// Absent vs. invalid: a <see langword="null"/>, empty, or whitespace-only correlation id is treated
/// as <em>absent</em> — the gate generates a globally unique id and attaches it (Req 4.2, 9.3). A
/// present, non-whitespace value that exceeds <see cref="MessageEnvelope.MaxCorrelationIdLength"/>
/// characters is <em>invalid</em> and is rejected with an <see cref="InvalidCorrelationIdException"/>
/// (Req 4.3); the gate never truncates or silently replaces a correlation id the caller meant to
/// supply.
/// </para>
/// <para>
/// Generation uses <see cref="Guid.NewGuid"/> rendered without separators (32 lowercase hex
/// characters), which is globally unique and comfortably within the 1..128 bound, so a generated id
/// always satisfies the envelope invariant.
/// </para>
/// </remarks>
public sealed class CorrelationIdGate
{
    /// <summary>
    /// Returns an envelope guaranteed to carry a valid correlation id. When the incoming correlation
    /// id is absent (null/empty/whitespace) a new globally unique id is generated and attached;
    /// otherwise the existing id is validated and preserved unchanged.
    /// </summary>
    /// <param name="envelope">The envelope whose correlation id should be normalized.</param>
    /// <returns>
    /// The same instance when the correlation id was already valid, or a copy with a generated
    /// correlation id when one was absent. The envelope is immutable, so a copy is returned rather
    /// than mutating the input.
    /// </returns>
    /// <exception cref="ArgumentNullException">If <paramref name="envelope"/> is null.</exception>
    /// <exception cref="InvalidCorrelationIdException">
    /// If a correlation id was supplied but exceeds <see cref="MessageEnvelope.MaxCorrelationIdLength"/>
    /// characters (Req 4.3).
    /// </exception>
    public MessageEnvelope EnsureCorrelationId(MessageEnvelope envelope)
    {
        ArgumentNullException.ThrowIfNull(envelope);

        if (string.IsNullOrWhiteSpace(envelope.CorrelationId))
        {
            // Absent: generate and attach a globally unique id (Req 4.2, 9.3).
            return envelope with { CorrelationId = GenerateCorrelationId() };
        }

        if (envelope.CorrelationId.Length > MessageEnvelope.MaxCorrelationIdLength)
        {
            // Supplied but overlong: reject without publishing (Req 4.3).
            throw new InvalidCorrelationIdException(
                envelope.CorrelationId,
                $"CorrelationId length {envelope.CorrelationId.Length} exceeds the maximum of "
                + $"{MessageEnvelope.MaxCorrelationIdLength} characters.");
        }

        // Present and within bounds: preserve unchanged (Req 4.4).
        return envelope;
    }

    /// <summary>
    /// Generates a globally unique correlation id (32 lowercase hex characters, well within the
    /// 1..128 bound) using <see cref="Guid.NewGuid"/> (Req 4.2, 9.3).
    /// </summary>
    public static string GenerateCorrelationId() => Guid.NewGuid().ToString("N");
}
