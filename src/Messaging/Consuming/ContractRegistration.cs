namespace Messaging.Consuming;

/// <summary>
/// A single mapping from a <c>MessageType</c> identifier to the known contract CLR type its payload
/// binds to on consume (Req 4.5). The set of registrations is the universe of <em>known</em>
/// contracts; a <c>MessageType</c> with no registration is an unknown contract and is rejected
/// (Req 4.6).
/// </summary>
/// <remarks>
/// Binding is restricted to pre-registered types on purpose: message payloads are untrusted bytes,
/// so the binder deserializes only into a known contract type and never into an arbitrary type named
/// by the (equally untrusted) message (code-conventions security — "bind to known contracts, never
/// deserialize into arbitrary types").
/// </remarks>
/// <param name="MessageType">The contract identifier carried by <c>MessageEnvelope.MessageType</c>.</param>
/// <param name="ContractType">The CLR type the payload is bound to for this message type.</param>
public sealed record ContractRegistration(string MessageType, Type ContractType);
