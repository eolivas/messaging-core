using System.Security.Cryptography;
using System.Text;

namespace Messaging.MassTransit.AmazonSqs;

/// <summary>
/// Maps a string correlation id to the <see cref="Guid"/> MassTransit requires for
/// <c>SendContext.CorrelationId</c>. The authoritative value travels unchanged in the body and the
/// <c>x-correlation-id</c> attribute.
/// </summary>
public static class CorrelationGuid
{
    /// <summary>Returns the parsed Guid, or a Guid built from the first 16 bytes of the SHA-256 hash.</summary>
    /// <param name="correlationId">The envelope correlation id.</param>
    /// <returns>A deterministic Guid for the correlation id.</returns>
    /// <exception cref="ArgumentNullException">If <paramref name="correlationId"/> is null.</exception>
    public static Guid From(string correlationId)
    {
        ArgumentNullException.ThrowIfNull(correlationId);

        if (Guid.TryParse(correlationId, out var parsed))
        {
            return parsed;
        }

        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(correlationId));
        return new Guid(hash.AsSpan(0, 16));
    }
}
