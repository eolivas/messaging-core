using System.Text.Json;
using Messaging.Contracts;

namespace Messaging.Consuming;

/// <summary>
/// The default <see cref="IContractBinder"/>: binds an envelope's JSON payload bytes to a
/// pre-registered contract type (Req 4.5), rejecting unknown contracts (Req 4.6) and tolerating
/// additive versioning by ignoring unrecognized fields (Req 4.7).
/// </summary>
/// <remarks>
/// <para>
/// <b>Known contracts only (Req 4.6, security).</b> The binder is constructed with a closed set of
/// <see cref="ContractRegistration"/> entries. It deserializes into the CLR type registered for the
/// envelope's <see cref="MessageEnvelope.MessageType"/> and never into a type chosen from the
/// untrusted message. A message type with no registration is rejected with
/// <see cref="UnknownContractException"/> before anything is deserialized.
/// </para>
/// <para>
/// <b>Additive versioning (Req 4.7).</b> Deserialization uses
/// <see cref="JsonSerializerOptions"/> configured so unknown JSON members are ignored rather than
/// causing a failure. A consumer bound to an earlier contract version therefore keeps binding when a
/// newer producer adds optional fields: the extra fields are silently dropped and the earlier
/// contract still populates. This relies only on additive (never renamed/removed) field evolution,
/// which is the versioning contract the envelope documents.
/// </para>
/// <para>
/// <b>SRP.</b> This type only turns payload bytes into a typed contract. It does not decide the
/// consume sequence, run the handler, or route failures — those belong to the consume pipeline.
/// </para>
/// </remarks>
public sealed class JsonContractBinder : IContractBinder
{
    // Tolerant options: unknown members are ignored so additive versioning stays backward-compatible
    // (Req 4.7). Case-insensitive matching keeps binding resilient to casing drift across versions.
    private static readonly JsonSerializerOptions BindingOptions = new()
    {
        PropertyNameCaseInsensitive = true,
        // System.Text.Json ignores unknown members by default (no UnmappedMemberHandling.Disallow),
        // which is exactly the additive-versioning tolerance we require; set explicitly for intent.
        ReadCommentHandling = JsonCommentHandling.Skip,
        AllowTrailingCommas = true,
    };

    private readonly IReadOnlyDictionary<string, Type> _contractsByType;

    /// <summary>
    /// Creates a binder over a closed set of known contract registrations.
    /// </summary>
    /// <param name="registrations">
    /// The known <c>MessageType</c> → contract-type mappings. Any message type outside this set is an
    /// unknown contract (Req 4.6).
    /// </param>
    /// <exception cref="ArgumentNullException">If <paramref name="registrations"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// If two registrations declare the same <see cref="ContractRegistration.MessageType"/>.
    /// </exception>
    public JsonContractBinder(IEnumerable<ContractRegistration> registrations)
    {
        ArgumentNullException.ThrowIfNull(registrations);

        var map = new Dictionary<string, Type>(StringComparer.Ordinal);
        foreach (var registration in registrations)
        {
            ArgumentNullException.ThrowIfNull(registration);
            if (!map.TryAdd(registration.MessageType, registration.ContractType))
            {
                throw new ArgumentException(
                    $"Duplicate contract registration for message type '{registration.MessageType}'.",
                    nameof(registrations));
            }
        }

        _contractsByType = map;
    }

    /// <inheritdoc />
    public bool IsKnown(string messageType)
    {
        ArgumentNullException.ThrowIfNull(messageType);
        return _contractsByType.ContainsKey(messageType);
    }

    /// <inheritdoc />
    /// <exception cref="ArgumentNullException">If <paramref name="envelope"/> is null.</exception>
    public object Bind(MessageEnvelope envelope)
    {
        ArgumentNullException.ThrowIfNull(envelope);

        if (!_contractsByType.TryGetValue(envelope.MessageType, out var contractType))
        {
            // No contract known for this message type: reject before the handler (Req 4.6).
            throw new UnknownContractException(
                envelope.MessageType,
                $"No message contract is registered for message type '{envelope.MessageType}'.");
        }

        try
        {
            // Deserialize untrusted bytes into the KNOWN, pre-registered type only (security).
            // Unknown members are ignored, keeping earlier-version consumers compatible (Req 4.7).
            var bound = JsonSerializer.Deserialize(envelope.Payload.Span, contractType, BindingOptions);
            if (bound is null)
            {
                throw new UnknownContractException(
                    envelope.MessageType,
                    $"Payload for message type '{envelope.MessageType}' bound to a null contract instance.");
            }

            return bound;
        }
        catch (JsonException ex)
        {
            // Payload does not match the known contract shape: treat as an unknown/unbindable contract
            // so the pipeline rejects it before the handler (Req 4.6). Later poison routing (task 13)
            // can observe this via UnknownContractException.
            throw new UnknownContractException(
                envelope.MessageType,
                $"Payload for message type '{envelope.MessageType}' could not be bound to contract "
                + $"'{contractType.Name}'.",
                ex);
        }
    }
}
