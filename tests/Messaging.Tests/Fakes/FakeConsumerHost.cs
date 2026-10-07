using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Messaging.Abstractions;
using Messaging.Consuming;
using Messaging.Contracts;

namespace Messaging.Tests.Fakes;

/// <summary>
/// The consume side of the <see cref="FakeMessagingProvider"/>: an <see cref="IMessageConsumerHost"/>
/// that stands in for a real transport's subscribe/consume loop. It captures every envelope delivered
/// to it and then drives the real Application-layer <see cref="ConsumePipeline"/> (contract binding →
/// handler invocation), so the actual consume pipeline runs end-to-end against an in-memory source
/// instead of a broker.
/// </summary>
/// <remarks>
/// <para>
/// Lifecycle (Req 3.3) is delegated to the wrapped <see cref="ConsumePipeline"/>: <see cref="State"/>,
/// <see cref="StartAsync"/>, and <see cref="StopAsync"/> forward to it so the fake exhibits the exact
/// Stopped → Starting → Running → Stopping → Stopped sequence the core defines, with no bespoke state
/// of its own (SRP; no divergent second state machine to keep in sync).
/// </para>
/// <para>
/// "Delivery" is the seam a transport adapter would own: a real provider pulls bytes off the wire and
/// pushes them through the pipeline. Here <see cref="DeliverAsync"/> plays that role, invoked either
/// directly by a test or by the fake publisher's loopback (see <see cref="FakeMessagingProvider"/>).
/// Captured envelopes are retained in delivery order so a test can inspect exactly what reached the
/// consume side.
/// </para>
/// </remarks>
public sealed class FakeConsumerHost : IMessageConsumerHost
{
    private readonly ConsumePipeline _pipeline;
    private readonly List<MessageEnvelope> _deliveredEnvelopes = new();

    /// <summary>
    /// Creates a fake consumer host over a real consume pipeline.
    /// </summary>
    /// <param name="pipeline">The Application-layer consume pipeline this host drives.</param>
    /// <exception cref="System.ArgumentNullException">If <paramref name="pipeline"/> is null.</exception>
    public FakeConsumerHost(ConsumePipeline pipeline)
    {
        _pipeline = pipeline ?? throw new System.ArgumentNullException(nameof(pipeline));
    }

    /// <summary>The envelopes handed to the consume side, in delivery order (for test inspection).</summary>
    public IReadOnlyList<MessageEnvelope> DeliveredEnvelopes => _deliveredEnvelopes;

    /// <inheritdoc />
    public ConsumerHostState State => _pipeline.State;

    /// <inheritdoc />
    public Task StartAsync(CancellationToken cancellationToken) => _pipeline.StartAsync(cancellationToken);

    /// <inheritdoc />
    public Task StopAsync(CancellationToken cancellationToken) => _pipeline.StopAsync(cancellationToken);

    /// <summary>
    /// Simulates the transport delivering one envelope to the consumer: records it, then runs it
    /// through the real consume pipeline (bind contract → invoke handler). Any pipeline rejection
    /// (for example an unknown contract) surfaces to the caller after capture, exactly as a transport
    /// adapter would observe it.
    /// </summary>
    /// <param name="envelope">The envelope the "transport" delivered.</param>
    /// <param name="cancellationToken">Token to cancel processing.</param>
    /// <exception cref="System.ArgumentNullException">If <paramref name="envelope"/> is null.</exception>
    public async Task DeliverAsync(MessageEnvelope envelope, CancellationToken cancellationToken)
    {
        System.ArgumentNullException.ThrowIfNull(envelope);

        _deliveredEnvelopes.Add(envelope);
        await _pipeline.DeliverAsync(envelope, cancellationToken).ConfigureAwait(false);
    }
}
