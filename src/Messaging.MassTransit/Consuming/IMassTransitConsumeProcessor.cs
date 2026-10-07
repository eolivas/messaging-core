using Messaging.Contracts;

namespace Messaging.MassTransit.Consuming;

/// <summary>
/// The seam each message is run through <b>inside, or ahead of,</b> the MassTransit consumer pipeline
/// before it reaches an application handler. It is the single insertion point where the core's
/// correlation-id propagation, idempotency check, failure classification, and dead-letter routing run,
/// so a message consumed over a MassTransit transport behaves exactly as it does in the other modes
/// (design: "The core's retry classification, idempotency check, and dead-letter routing run inside
/// the MassTransit consumer pipeline as filters, or ahead of it").
/// </summary>
/// <remarks>
/// In production this is implemented by driving the Application-layer consume pipeline (wired in the
/// DI task). The shell depends only on this shallow port so the "core steps run ahead of the handler"
/// shape is expressed and testable without a live broker or MassTransit filter stack. The real
/// MassTransit <c>IFilter&lt;ConsumeContext&gt;</c> that calls this is owned by the provider's own
/// later spec.
/// </remarks>
public interface IMassTransitConsumeProcessor
{
    /// <summary>
    /// Runs the core consume steps for a single envelope delivered by MassTransit: propagate the
    /// correlation id, evaluate the idempotency gate, bind the payload to a known contract, invoke the
    /// handler, and classify failures / route to dead-letter. The MassTransit message is acknowledged
    /// only after this completes without throwing, so behavior matches the other modes.
    /// </summary>
    /// <param name="envelope">The envelope MassTransit delivered.</param>
    /// <param name="cancellationToken">Token to cancel processing.</param>
    Task ProcessAsync(MessageEnvelope envelope, CancellationToken cancellationToken);
}
