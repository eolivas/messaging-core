using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Messaging.Abstractions;
using Messaging.Contracts;

namespace Messaging.Tests.Fakes;

/// <summary>
/// The publish side of the <see cref="FakeMessagingProvider"/>: an <see cref="IMessagePublisher"/>
/// that stands in for a real transport. It captures every envelope the publish pipeline hands it,
/// returns a configurable confirm/fail <see cref="PublishResult"/>, and — on a confirmed publish —
/// loops the envelope into the consume side so a round trip can be exercised entirely in memory.
/// </summary>
/// <remarks>
/// <para>
/// The provider's publish pipeline validates and normalizes the envelope before it reaches here, so
/// everything captured by this publisher is already a valid, correlation-normalized envelope — the
/// same contract a real transport adapter would receive. Captured envelopes are retained in publish
/// order for test inspection.
/// </para>
/// <para>
/// Confirm/fail is governed by a mutable <see cref="Outcome"/> so a single provider instance can
/// drive the success path and the transport-failure path across a test. A <see cref="FakePublishOutcome.Fail"/>
/// outcome returns a failed result and does NOT loop the envelope into the consume side, mirroring a
/// transport that never accepted the message (it stays undelivered).
/// </para>
/// <para>
/// Loopback is optional: when no consume sink is wired, the publisher behaves as a pure capture-and-
/// confirm publish target (useful for publish-only pipeline tests). When a sink is wired, a confirmed
/// publish is delivered to it, giving an in-memory publish → consume round trip with no broker.
/// </para>
/// </remarks>
public sealed class FakePublisher : IMessagePublisher
{
    private readonly List<MessageEnvelope> _publishedEnvelopes = new();
    private readonly Func<MessageEnvelope, CancellationToken, Task>? _deliverToConsumer;

    /// <summary>
    /// Creates a fake publisher.
    /// </summary>
    /// <param name="deliverToConsumer">
    /// Optional loopback sink invoked with each confirmed envelope. When null, confirmed publishes are
    /// captured only (no consume-side delivery).
    /// </param>
    /// <param name="outcome">The initial publish outcome (defaults to <see cref="FakePublishOutcome.Confirm"/>).</param>
    public FakePublisher(
        Func<MessageEnvelope, CancellationToken, Task>? deliverToConsumer = null,
        FakePublishOutcome outcome = FakePublishOutcome.Confirm)
    {
        _deliverToConsumer = deliverToConsumer;
        Outcome = outcome;
    }

    /// <summary>The outcome this publisher simulates for subsequent publishes. Mutable so one instance can drive both paths.</summary>
    public FakePublishOutcome Outcome { get; set; }

    /// <summary>The error message returned on a <see cref="FakePublishOutcome.Fail"/> outcome.</summary>
    public string FailureReason { get; set; } = "fake transport rejected the publish";

    /// <summary>The envelopes handed to the publisher, in publish order (for test inspection).</summary>
    public IReadOnlyList<MessageEnvelope> PublishedEnvelopes => _publishedEnvelopes;

    /// <inheritdoc />
    /// <exception cref="ArgumentNullException">If <paramref name="envelope"/> is null.</exception>
    public async Task<PublishResult> PublishAsync(MessageEnvelope envelope, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(envelope);

        // Capture first so a failed publish is still observable to tests inspecting what was attempted.
        _publishedEnvelopes.Add(envelope);

        if (Outcome == FakePublishOutcome.Fail)
        {
            // Transport rejected the message: leave it undelivered (no loopback) and surface the failure.
            return PublishResult.Fail(FailureReason);
        }

        // Confirmed: loop into the consume side when a sink is wired, modelling an in-memory round trip.
        if (_deliverToConsumer is not null)
        {
            await _deliverToConsumer(envelope, cancellationToken).ConfigureAwait(false);
        }

        return PublishResult.Ok();
    }
}
