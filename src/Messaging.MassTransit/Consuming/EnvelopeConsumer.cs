using MassTransit;
using Messaging.Contracts;
using Messaging.MassTransit.Transport;

namespace Messaging.MassTransit.Consuming;

/// <summary>
/// The MassTransit <see cref="IConsumer{TMessage}"/> that bridges a delivered
/// <see cref="EnvelopeMessage"/> back to the transport-neutral <see cref="MessageEnvelope"/> and
/// hands it to the core consume-steps seam. It is the single entry point where a message delivered
/// over RabbitMQ crosses from the MassTransit wire contract into the core consume pipeline.
/// </summary>
/// <remarks>
/// <para>
/// <b>Single responsibility.</b> This consumer does exactly one thing: rebuild the envelope verbatim
/// via <see cref="EnvelopeMessage.ToEnvelope"/> and forward it to
/// <see cref="IMassTransitConsumeProcessor.ProcessAsync"/>. All policy — correlation propagation, the
/// idempotency gate, contract binding, handler invocation, failure classification, retry, and
/// dead-letter routing — lives behind the processor seam, so this type stays a thin bridge
/// (design §2, Req 1.3).
/// </para>
/// <para>
/// <b>Fidelity &amp; correlation.</b> Because <see cref="EnvelopeMessage.ToEnvelope"/> reconstructs
/// the envelope field-for-field (including the correlation id and payload bytes), the correlation id
/// reaches the core consume steps unchanged and binding sees the original payload (Req 6.4, 6.5).
/// </para>
/// <para>
/// <b>Ack / redelivery.</b> When the processor returns normally the delivery is acknowledged; when it
/// throws the exception propagates so MassTransit redelivers. The attempt bound and the terminal
/// dead-letter decision are owned by the core <c>RetryExecutor</c> driven inside the processor, not by
/// this consumer.
/// </para>
/// </remarks>
public sealed class EnvelopeConsumer : IConsumer<EnvelopeMessage>
{
    private readonly IMassTransitConsumeProcessor _processor;

    /// <summary>
    /// Creates the consumer with the sole dependency it needs — the core consume-steps seam.
    /// </summary>
    /// <param name="processor">The seam that runs the core consume steps for each delivered envelope.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="processor"/> is <c>null</c>.</exception>
    public EnvelopeConsumer(IMassTransitConsumeProcessor processor) =>
        _processor = processor ?? throw new ArgumentNullException(nameof(processor));

    /// <summary>
    /// Rebuilds the transport-neutral <see cref="MessageEnvelope"/> from the delivered
    /// <see cref="EnvelopeMessage"/> and forwards it to the core consume-steps seam, propagating the
    /// delivery's cancellation token.
    /// </summary>
    /// <param name="context">The MassTransit consume context carrying the delivered wire message.</param>
    public Task Consume(ConsumeContext<EnvelopeMessage> context) =>
        _processor.ProcessAsync(context.Message.ToEnvelope(), context.CancellationToken);
}
