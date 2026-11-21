using System.Diagnostics;
using System.Globalization;
using MassTransit;
using Messaging.Contracts;
using Messaging.MassTransit.Consuming;
using Messaging.MassTransit.Transport;
using Messaging.Observability;

namespace Messaging.MassTransit.AmazonSqs;

/// <summary>
/// Receive-side <see cref="IConsumer{TMessage}"/> for the Amazon SQS transport. It rebuilds the
/// <see cref="MessageEnvelope"/> through <see cref="SnsSqsWireFormat.FromWire"/>, restores the publish
/// trace context from <c>ConsumeContext.Headers</c> (populated in both raw and SNS-wrapped delivery),
/// and runs the consume processor inside a <c>messaging.receive</c> activity.
/// </summary>
/// <remarks>
/// The receive activity becomes the parent of the core consume span, so each fan-out consume trace
/// links to the one publish trace (Req 15.5). Ack and redelivery follow the processor: a normal
/// return acknowledges, an exception propagates so the message is redelivered.
/// </remarks>
public sealed class SqsEnvelopeConsumer : IConsumer<EnvelopeMessage>
{
    /// <summary>The name of the receive activity.</summary>
    public const string ReceiveActivityName = "messaging.receive";

    private static readonly ActivitySource ActivitySource = new(MessagingTraceSource.ActivitySourceName);

    private readonly IMassTransitConsumeProcessor _processor;

    /// <summary>Creates the consumer.</summary>
    /// <param name="processor">The seam that runs the core consume steps.</param>
    /// <exception cref="ArgumentNullException">If <paramref name="processor"/> is null.</exception>
    public SqsEnvelopeConsumer(IMassTransitConsumeProcessor processor) =>
        _processor = processor ?? throw new ArgumentNullException(nameof(processor));

    /// <summary>Restores trace context and the envelope, then calls the consume processor.</summary>
    /// <param name="context">The MassTransit consume context.</param>
    public async Task Consume(ConsumeContext<EnvelopeMessage> context)
    {
        ArgumentNullException.ThrowIfNull(context);

        var consumeHeaders = context.Headers.GetAll().ToList();
        var envelope = SnsSqsWireFormat.FromWire(context.Message, consumeHeaders);

        using var activity = StartReceiveActivity(consumeHeaders);
        activity?.SetTag("messaging.correlation_id", envelope.CorrelationId);

        await _processor.ProcessAsync(envelope, context.CancellationToken).ConfigureAwait(false);
    }

    private static Activity? StartReceiveActivity(IEnumerable<KeyValuePair<string, object>> headers)
    {
        foreach (var header in headers)
        {
            if (!string.Equals(header.Key, MessagingTraceSource.TraceParentHeader, StringComparison.Ordinal))
            {
                continue;
            }

            var traceParent = Convert.ToString(header.Value, CultureInfo.InvariantCulture);
            if (ActivityContext.TryParse(traceParent, traceState: null, out var parent))
            {
                return ActivitySource.StartActivity(ReceiveActivityName, ActivityKind.Consumer, parent);
            }

            break;
        }

        return ActivitySource.StartActivity(ReceiveActivityName, ActivityKind.Consumer);
    }
}
