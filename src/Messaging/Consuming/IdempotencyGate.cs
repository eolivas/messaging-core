using Messaging.Abstractions;
using Messaging.Configuration;
using Messaging.Contracts;

namespace Messaging.Consuming;

/// <summary>
/// The Application-layer idempotent-consume gate that runs for each received message <em>before</em>
/// the consumer handler, so at-least-once delivery (Req 6.7) does not cause duplicate side effects
/// (Req 7). It composes the two Domain ports — <see cref="IIdempotencyKeyProvider"/> to derive the
/// key and <see cref="IIdempotencyStore"/> to detect an already-processed key — and turns them into a
/// single <see cref="IdempotencyGateDecision"/> the <see cref="ConsumePipeline"/> acts on.
/// </summary>
/// <remarks>
/// <para>
/// Evaluation for one envelope, honoring <see cref="IdempotencyConfig"/>:
/// </para>
/// <list type="number">
///   <item>
///     <description>
///     <b>Disabled</b> (<see cref="IdempotencyConfig.Enabled"/> is <see langword="false"/>): the gate
///     is bypassed and returns <see cref="IdempotencyGateOutcome.Bypassed"/> with a null key, so the
///     handler runs exactly as before idempotency existed.
///     </description>
///   </item>
///   <item>
///     <description>
///     <b>Underivable key</b> (Req 7.6): when enabled and
///     <see cref="IIdempotencyKeyProvider.TryDerive"/> returns <see langword="false"/>, the gate
///     throws <see cref="MissingIdempotencyKeyException"/> before consulting the store and before the
///     handler — rejecting the message with a missing-key error without any side effects.
///     </description>
///   </item>
///   <item>
///     <description>
///     <b>Duplicate</b> (Req 7.2, 7.4): when the derived key is already recorded as processed
///     (<see cref="IIdempotencyStore.IsProcessedAsync"/> returns <see langword="true"/>), the gate
///     returns <see cref="IdempotencyGateOutcome.Duplicate"/> so the pipeline acknowledges the message
///     without re-invoking the handler, exposing a duplicate indication through
///     <see cref="ConsumeContext.IsDuplicate"/>.
///     </description>
///   </item>
///   <item>
///     <description>
///     <b>Proceed</b> (Req 7.1): otherwise the gate returns the derived key so the handler runs with
///     the key exposed in <see cref="ConsumeContext.IdempotencyKey"/> (Req 6.8).
///     </description>
///   </item>
/// </list>
/// <para>
/// Recording the processed key with its retention window (Req 7.3) happens <em>after</em> the handler
/// succeeds; that step is layered on with the EF-backed store in a later task. This gate owns only the
/// pre-handler decision. It depends solely on the Domain ports, keeping EF/infrastructure types out of
/// the Application layer (DIP).
/// </para>
/// </remarks>
public sealed class IdempotencyGate
{
    private readonly IIdempotencyKeyProvider _keyProvider;
    private readonly IIdempotencyStore _idempotencyStore;
    private readonly IdempotencyConfig _config;

    /// <summary>
    /// Creates the idempotency gate.
    /// </summary>
    /// <param name="keyProvider">Derives the deterministic key from the envelope (Req 7.1, 7.6).</param>
    /// <param name="idempotencyStore">Detects a previously-processed key (Req 7.2).</param>
    /// <param name="config">The idempotency settings (enabled flag, retention) (Req 7.3).</param>
    /// <exception cref="ArgumentNullException">If any dependency is null.</exception>
    public IdempotencyGate(
        IIdempotencyKeyProvider keyProvider,
        IIdempotencyStore idempotencyStore,
        IdempotencyConfig config)
    {
        _keyProvider = keyProvider ?? throw new ArgumentNullException(nameof(keyProvider));
        _idempotencyStore = idempotencyStore ?? throw new ArgumentNullException(nameof(idempotencyStore));
        _config = config ?? throw new ArgumentNullException(nameof(config));
    }

    /// <summary>
    /// Evaluates the gate for a single received envelope, returning what the pipeline should do next
    /// (Req 7.2, 7.4, 7.6).
    /// </summary>
    /// <param name="envelope">The received envelope.</param>
    /// <param name="cancellationToken">Token to cancel the store lookup.</param>
    /// <returns>The gate decision carrying the outcome and the consume context.</returns>
    /// <exception cref="ArgumentNullException">If <paramref name="envelope"/> is null.</exception>
    /// <exception cref="MissingIdempotencyKeyException">
    /// If idempotency is enabled and the key cannot be derived; thrown before the store is consulted
    /// and before any side effects (Req 7.6).
    /// </exception>
    public async Task<IdempotencyGateDecision> EvaluateAsync(
        MessageEnvelope envelope,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(envelope);

        // Disabled: bypass the gate entirely so behavior matches a non-idempotent pipeline.
        if (!_config.Enabled)
        {
            return new IdempotencyGateDecision(
                IdempotencyGateOutcome.Bypassed,
                new ConsumeContext(IdempotencyKey: null, IsDuplicate: false));
        }

        // Enabled but underivable: reject BEFORE the store lookup and BEFORE the handler (Req 7.6).
        if (!_keyProvider.TryDerive(envelope, out var idempotencyKey))
        {
            throw new MissingIdempotencyKeyException(envelope.MessageType);
        }

        cancellationToken.ThrowIfCancellationRequested();

        // Duplicate: already processed — acknowledge without re-executing side effects (Req 7.2, 7.4).
        var alreadyProcessed = await _idempotencyStore
            .IsProcessedAsync(idempotencyKey, cancellationToken)
            .ConfigureAwait(false);

        if (alreadyProcessed)
        {
            return new IdempotencyGateDecision(
                IdempotencyGateOutcome.Duplicate,
                new ConsumeContext(idempotencyKey, IsDuplicate: true));
        }

        // Proceed: expose the derived key to the consumer (Req 6.8, 7.1).
        return new IdempotencyGateDecision(
            IdempotencyGateOutcome.Proceed,
            new ConsumeContext(idempotencyKey, IsDuplicate: false));
    }
}
