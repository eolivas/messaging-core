using Demo.Application;
using Demo.Contracts;
using Messaging.Observability;
using Microsoft.Extensions.Logging;
namespace Demo.Infrastructure.Messaging;

/// <summary>
/// Emits <see cref="FlowStepKind.DeadLettered"/> with the library's attempt count and the
/// failure kind from the <see cref="AttemptTracker"/>. A correlation id the tracker has not
/// seen never reached the handler, which the library treats as non-transient.
/// </summary>
public sealed class DemoDeadLetterLog : IDeadLetterLog
{
    public const string NonTransient = "non-transient";
    private readonly MicroserviceId _service;
    private readonly AttemptTracker _tracker;
    private readonly IFlowStepSink _sink;
    private readonly ILogger<DemoDeadLetterLog> _logger;
    private readonly TimeProvider _time;
    private long _suppressedFailures;

    public DemoDeadLetterLog(
        MicroserviceId service,
        AttemptTracker tracker,
        IFlowStepSink sink,
        ILogger<DemoDeadLetterLog> logger,
        TimeProvider? time = null)
    {
        _service = service;
        _tracker = tracker ?? throw new ArgumentNullException(nameof(tracker));
        _sink = sink ?? throw new ArgumentNullException(nameof(sink));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _time = time ?? TimeProvider.System;
    }

    /// <summary>Count of logging or flow-reporting failures suppressed to protect dead-letter routing.</summary>
    public long SuppressedFailureCount => Interlocked.Read(ref _suppressedFailures);

    public void DeadLettered(string correlationId, string failureReason, int attemptCount)
    {
        var kind = _tracker.TryGet(correlationId, out var info) && info is not null
            ? info.FailureKind
            : NonTransient;
        var reason = FailureReasonSanitizer.Sanitize(failureReason);
        try
        {
            _logger.LogWarning(
                "Message dead-lettered on {Service} after {AttemptCount} attempts, failure kind {FailureKind}: {Reason} (correlation {CorrelationId})",
                _service, attemptCount, kind, reason, correlationId);
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            // Best-effort (Req 9.8). The logger itself failed, so record on a counter.
            Interlocked.Increment(ref _suppressedFailures);
        }
        try
        {
            _sink.Emit(new FlowStep(
                _service, FlowStepKind.DeadLettered, correlationId, null,
                _time.GetUtcNow(), attemptCount, kind, reason));
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            // Flow reporting must not change the dead-letter outcome; count and log the failure.
            Interlocked.Increment(ref _suppressedFailures);
            TryLogSinkFailure(ex, correlationId);
        }
    }

    public void DeadLetterRoutingFailed(
        string correlationId,
        string failureReason,
        int attemptCount,
        Exception routingError)
    {
        try
        {
            _logger.LogError(
                "Dead-letter routing failed on {Service} after {AttemptCount} attempts: {Reason}; routing error {ErrorType} (correlation {CorrelationId})",
                _service, attemptCount, FailureReasonSanitizer.Sanitize(failureReason),
                routingError?.GetType().Name, correlationId);
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            Interlocked.Increment(ref _suppressedFailures);
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
