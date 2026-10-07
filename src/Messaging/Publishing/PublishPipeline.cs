using Messaging.Abstractions;
using Messaging.Contracts;

namespace Messaging.Publishing;

/// <summary>
/// The Application-layer <see cref="IMessagePublisher"/> that fronts every outgoing message
/// (Req 3.1, 3.2, 4.4). It applies the mode-independent publish policy — normalize the
/// correlation id, validate the envelope invariants — and then delegates the actual transport
/// publish to the resolved active provider's publisher. Application code depends only on
/// <see cref="IMessagePublisher"/>, never on a concrete provider (Req 3.1, 3.8).
/// </summary>
/// <remarks>
/// <para>
/// Pipeline order and failure semantics:
/// </para>
/// <list type="number">
///   <item>
///     <description>
///     <b>Correlation id gate</b> (<see cref="CorrelationIdGate"/>): generates one when absent,
///     rejects a supplied-but-invalid value. An <see cref="InvalidCorrelationIdException"/> is
///     mapped to a failed <see cref="PublishResult"/> rather than propagated, so a caller gets a
///     deterministic failed result instead of an exception, and the message stays undelivered
///     (Req 4.3).
///     </description>
///   </item>
///   <item>
///     <description>
///     <b>Envelope validation</b> (<see cref="MessageEnvelope.Validate"/>): any violated invariant
///     throws <see cref="InvalidEnvelopeException"/>, which is likewise mapped to a failed result;
///     the provider is never invoked so the message is left undelivered (Req 3.2, 4.3).
///     </description>
///   </item>
///   <item>
///     <description>
///     <b>Delegation</b>: on success the active provider is resolved via
///     <see cref="IProviderRegistry"/> and the (correlation-normalized) envelope is handed to its
///     <see cref="IMessagingProvider.Publisher"/> unchanged — message id, correlation id, message
///     type, timestamp, headers, payload, and contract version all preserved (Req 4.4). The
///     provider's <see cref="PublishResult"/> is returned verbatim.
///     </description>
///   </item>
/// </list>
/// <para>
/// Resolution is delegated to the registry on each call; the registry contract guarantees a stable,
/// single provider for the configured mode across the instance lifetime (Req 1.4, 3.4, 3.7), so
/// this never resolves a different provider mid-lifetime. Keeping resolution behind the registry
/// (rather than caching a provider here) preserves the fail-fast unresolved/ambiguous diagnostics
/// the registry owns.
/// </para>
/// <para>
/// SRP: this type orchestrates the publish policy only. Correlation handling lives in
/// <see cref="CorrelationIdGate"/>, envelope invariants live on <see cref="MessageEnvelope"/>, and
/// provider selection lives in <see cref="IProviderRegistry"/>; the pipeline composes them.
/// </para>
/// </remarks>
public sealed class PublishPipeline : IMessagePublisher
{
    private readonly CorrelationIdGate _correlationIdGate;
    private readonly IProviderRegistry _providerRegistry;
    private readonly MessagingMode _activeMode;

    /// <summary>
    /// Creates the publish pipeline for the configured active mode.
    /// </summary>
    /// <param name="correlationIdGate">The correlation id generation/validation gate (task 8.1).</param>
    /// <param name="providerRegistry">The registry that resolves the active provider (Req 3.4, 3.5).</param>
    /// <param name="activeMode">
    /// The mode selected for this instance's lifetime, sourced from the validated
    /// <c>MessagingConfiguration.Mode</c>. Fixed at construction; the mode cannot change after
    /// startup (Req 1.4).
    /// </param>
    /// <exception cref="ArgumentNullException">
    /// If <paramref name="correlationIdGate"/> or <paramref name="providerRegistry"/> is null.
    /// </exception>
    public PublishPipeline(
        CorrelationIdGate correlationIdGate,
        IProviderRegistry providerRegistry,
        MessagingMode activeMode)
    {
        _correlationIdGate = correlationIdGate ?? throw new ArgumentNullException(nameof(correlationIdGate));
        _providerRegistry = providerRegistry ?? throw new ArgumentNullException(nameof(providerRegistry));
        _activeMode = activeMode;
    }

    /// <inheritdoc />
    /// <exception cref="ArgumentNullException">If <paramref name="envelope"/> is null.</exception>
    public async Task<PublishResult> PublishAsync(MessageEnvelope envelope, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(envelope);

        MessageEnvelope normalized;
        try
        {
            // Stage 1: normalize/validate the correlation id (generate-if-absent, reject-if-invalid).
            normalized = _correlationIdGate.EnsureCorrelationId(envelope);

            // Stage 2: validate the remaining envelope invariants fail-fast.
            normalized.Validate();
        }
        catch (InvalidCorrelationIdException ex)
        {
            // Supplied-but-invalid correlation id: leave the message undelivered (Req 4.3).
            return PublishResult.Fail(ex.Message);
        }
        catch (InvalidEnvelopeException ex)
        {
            // Violated envelope invariant: leave the message undelivered (Req 3.2, 4.3).
            return PublishResult.Fail(ex.Message);
        }

        // Stage 3: resolve the active provider and delegate, preserving every field unchanged (Req 4.4).
        var provider = _providerRegistry.Resolve(_activeMode);
        return await provider.Publisher
            .PublishAsync(normalized, cancellationToken)
            .ConfigureAwait(false);
    }
}
