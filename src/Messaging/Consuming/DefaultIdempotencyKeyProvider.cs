using System.Security.Cryptography;
using System.Text;
using Messaging.Abstractions;
using Messaging.Contracts;

namespace Messaging.Consuming;

/// <summary>
/// The default <see cref="IIdempotencyKeyProvider"/>: derives a deterministic idempotency key from an
/// envelope's stable identity fields so that two <see cref="MessageEnvelope"/> instances representing
/// the same logical message always produce the identical key (Req 7.1). Returns <see langword="false"/>
/// from <see cref="TryDerive"/> when the key is underivable (Req 7.6).
/// </summary>
/// <remarks>
/// <para>
/// <b>Derivation scheme (documented and deterministic).</b> The key is the lowercase hexadecimal
/// SHA-256 digest over a canonical, delimited concatenation of the envelope's logical-identity
/// fields:
/// </para>
/// <list type="number">
///   <item><description><see cref="MessageEnvelope.MessageId"/> in its invariant "D" GUID form,</description></item>
///   <item><description><see cref="MessageEnvelope.MessageType"/>, and</description></item>
///   <item><description>the raw <see cref="MessageEnvelope.Payload"/> bytes.</description></item>
/// </list>
/// <para>
/// Only these three fields participate. Transport/handling metadata that can legitimately differ
/// between two deliveries of the <em>same</em> logical message — correlation id, headers, creation
/// timestamp, contract version — is intentionally excluded, so a redelivery of the same message
/// hashes to the same key (Req 7.1). A field separator byte that cannot appear inside the GUID or
/// UTF-8 message type is used so distinct field boundaries can never collide (e.g. a message type
/// ending where a payload begins).
/// </para>
/// <para>
/// <b>Underivable (Req 7.6).</b> The key cannot be derived when the envelope lacks a stable identity:
/// an empty <see cref="MessageEnvelope.MessageId"/> or a null/blank <see cref="MessageEnvelope.MessageType"/>.
/// In that case <see cref="TryDerive"/> returns <see langword="false"/> and yields an empty key; the
/// consume gate rejects such a message before any side effects run.
/// </para>
/// <para>
/// SRP: this type only computes the key. Deciding what to do with a derived/underivable key — the
/// store lookup, duplicate handling, and rejection — belongs to <see cref="IdempotencyGate"/>.
/// </para>
/// </remarks>
public sealed class DefaultIdempotencyKeyProvider : IIdempotencyKeyProvider
{
    // A separator that cannot appear in a canonical GUID string nor split a UTF-8 code unit, so the
    // boundary between concatenated fields is unambiguous and two different field splits cannot hash
    // to the same input.
    private static readonly byte[] FieldSeparator = { 0x1F }; // ASCII unit separator.

    /// <inheritdoc />
    public bool TryDerive(MessageEnvelope envelope, out string idempotencyKey)
    {
        ArgumentNullException.ThrowIfNull(envelope);

        // Underivable when the envelope carries no stable logical identity (Req 7.6).
        if (envelope.MessageId == Guid.Empty || string.IsNullOrWhiteSpace(envelope.MessageType))
        {
            idempotencyKey = string.Empty;
            return false;
        }

        using var sha256 = SHA256.Create();
        using var buffer = new MemoryStream();

        var messageIdBytes = Encoding.UTF8.GetBytes(envelope.MessageId.ToString("D"));
        var messageTypeBytes = Encoding.UTF8.GetBytes(envelope.MessageType);

        buffer.Write(messageIdBytes, 0, messageIdBytes.Length);
        buffer.Write(FieldSeparator, 0, FieldSeparator.Length);
        buffer.Write(messageTypeBytes, 0, messageTypeBytes.Length);
        buffer.Write(FieldSeparator, 0, FieldSeparator.Length);
        buffer.Write(envelope.Payload.Span);

        buffer.Position = 0;
        var digest = sha256.ComputeHash(buffer);
        idempotencyKey = Convert.ToHexString(digest).ToLowerInvariant();
        return true;
    }
}
