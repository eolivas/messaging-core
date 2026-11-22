using Demo.Contracts;
using Messaging.Abstractions;
using Messaging.Contracts;

namespace Demo.Infrastructure.Messaging;

/// <summary>
/// Emits <see cref="FlowStepKind.Consumed"/> when the message first reaches the handler chain
/// and <see cref="FlowStepKind.Retrying"/> when a transient failure will be retried
/// (<c>attempt &lt; maxAttempts</c>). Records the classified failure kind in the
/// <see cref="AttemptTracker"/> and rethrows the original exception.
/// </summary>
public sealed class FlowReportingHandler : IMessageHandler<QuoteAcceptedEvent>
{
    public const string TransientKind = "transient";

    private readonly MicroserviceId _service;
    private readonly IMessageHandler<QuoteAcceptedEvent> _inner;
    private readonly IFailureClassifier _classifier;
    private readonly AttemptTracker _tracker;
    private readonly IFlowStepSink _sink;
    private readonly int _maxAttempts;
    private readonly TimeProvider _time;

    public FlowReportingHandler(
        MicroserviceId service,
        IMessageHandler<QuoteAcceptedEvent> inner,
        IFailureClassifier classifier,
        AttemptTracker tracker,
        IFlowStepSink sink,
        int maxAttempts,
        TimeProvider? time = null)
    {
        _service = service;
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        _classifier = classifier ?? throw new ArgumentNullException(nameof(classifier));
        _tracker = tracker ?? throw new ArgumentNullException(nameof(tracker));
        _sink = sink ?? throw new ArgumentNullException(nameof(sink));
        _maxAttempts = maxAttempts;
        _time = time ?? TimeProvider.System;
    }

    public async Task HandleAsync(
        MessageEnvelope envelope,
        QuoteAcceptedEvent message,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(envelope);
        ArgumentNullException.ThrowIfNull(message);
        var correlationId = envelope.CorrelationId;
        var attempt = _tracker.NextAttempt(correlationId);

        if (attempt == 1)
        {
            Emit(FlowStepKind.Consumed, correlationId, message.LeadId, attempt, null);
        }

        try
        {
            await _inner.HandleAsync(envelope, message, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException and not OutOfMemoryException)
        {
            var transient = _classifier.Classify(ex) == FailureKind.Transient;
            _tracker.Record(
                correlationId, attempt, transient ? TransientKind : DemoDeadLetterLog.NonTransient);
            if (transient && attempt < _maxAttempts)
            {
                Emit(FlowStepKind.Retrying, correlationId, message.LeadId, attempt, TransientKind);
            }
            throw;
        }
    }

    private void Emit(FlowStepKind kind, string correlationId, string leadId, int attempt, string? failureKind) =>
        _sink.Emit(new FlowStep(
            _service, kind, correlationId, leadId, _time.GetUtcNow(), attempt, failureKind));
}
