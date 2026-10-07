namespace Messaging.Tests.Consuming;

/// <summary>
/// A minimal known contract used by the consume-pipeline property tests as a registered
/// <see cref="Messaging.Consuming.ContractRegistration"/> target. Its shape is irrelevant to the
/// unknown-contract property (Property 8) — it exists only so the binder has a non-empty set of
/// known message types to differentiate "unknown" message types from.
/// </summary>
public sealed record OrderCreatedContract(string OrderId, decimal Amount);
