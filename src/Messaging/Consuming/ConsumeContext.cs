namespace Messaging.Consuming;

/// <summary>
/// The per-message context the consume pipeline exposes to the handler alongside the bound message
/// and the original envelope. It surfaces the derived idempotency key to consumers for duplicate
/// detection (Req 6.8, 7.1) and signals when the pipeline has already identified the message as a
/// duplicate (Req 7.2).
/// </summary>
/// <remarks>
/// <para>
/// Keeping these cross-cutting signals in a dedicated context — rather than mutating the immutable
/// <see cref="Messaging.Contracts.MessageEnvelope"/> or overloading its headers — preserves the
/// envelope's field-preservation guarantee (Req 4.4) and gives consumers a typed, stable surface for
/// the idempotency key without coupling them to how the key is derived.
/// </para>
/// <para>
/// <see cref="IdempotencyKey"/> is <see langword="null"/> only when idempotency is disabled; when it
/// is enabled, an underivable key is rejected before the handler runs (Req 7.6), so a handler that
/// executes under enabled idempotency always observes a non-null key.
/// </para>
/// </remarks>
/// <param name="IdempotencyKey">
/// The deterministic idempotency key derived for the message, or <see langword="null"/> when
/// idempotency is disabled (Req 6.8, 7.1).
/// </param>
/// <param name="IsDuplicate">
/// <see langword="true"/> when the pipeline detected this key as already processed (Req 7.2). Under
/// the current gate a duplicate is acknowledged without invoking the handler, so a handler that runs
/// observes <see langword="false"/>; the flag exists so the signal is explicit and future callers can
/// surface it.
/// </param>
public sealed record ConsumeContext(string? IdempotencyKey, bool IsDuplicate);
