using Demo.Contracts;
using Messaging.Observability;
using Microsoft.Extensions.Logging;
namespace Demo.Infrastructure.Messaging;

/// <summary>
/// Writes one structured entry per library event (correlation id, operation type, message
/// type; never payload or secrets) and emits <see cref="FlowStepKind.Delivered"/> on consume.
/// Best-effort: failures never reach the message operation, and are counted.
/// </summary>
public sealed class DemoMessagingLog : IMessagingLog
{
    private readonly MicroserviceId _service;
    private readonly string _messageType;
    private readonly IFlowStepSink _sink;
    private readonly ILogger<DemoMessagingLog> _logger;
    private long _suppressedFailures;

    public DemoMessagingLog(
        MicroserviceId service,
        string messageType,
        IFlowStepSink sink,
        ILogger<DemoMessagingLog> logger)
    {
        _service = service;
        _messageType = messageType ?? throw new ArgumentNullException(nameof(messageType));
        _sink = sink ?? throw new ArgumentNullException(nameof(sink));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <summary>Count of logging or flow-reporting failures suppressed to protect the message operation.</summary>
    public long SuppressedFailureCount => Interlocked.Read(ref _suppressedFailures);

    public void MessageEvent(MessagingEventType eventType, string correlationId, DateTimeOffset timestampUtc)
    {
        try
        {
            _logger.LogInformation(
                "Messaging event {OperationType} for {MessageType} on {Service} at {TimestampUtc} (correlation {CorrelationId})",
                eventType, _messageType, _service, timestampUtc, correlationId);
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            // Best-effort (Req 9.8). The logger itself failed, so record on a counter.
            Interlocked.Increment(ref _suppressedFailures);
        }

        if (eventType != MessagingEventType.Consume)
        {
            return;
        }
        try
        {
            _sink.Emit(new FlowStep(_service, FlowStepKind.Delivered, correlationId, null, timestampUtc));
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            // Flow reporting must not change the message outcome; count and log the failure.
            Interlocked.Increment(ref _suppressedFailures);
            TryLogSinkFailure(ex, correlationId);
        }
    }

    private void TryLogSinkFailure(Exception ex, string correlationId)
    {
        try
        {
            _logger.LogError(
                "Flow step emit failed on {Service}: {ErrorType} (correlation {CorrelationId})",
                _service, ex.GetType().Name, correlationId);
        }
        catch (Exception logEx) when (logEx is not OutOfMemoryException)
        {
            // Already counted by the caller; a failing logger cannot report itself.
            Interlocked.Increment(ref _suppressedFailures);
        }
    }
}
