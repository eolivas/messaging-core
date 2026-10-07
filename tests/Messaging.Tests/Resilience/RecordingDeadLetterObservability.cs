using System;
using System.Collections.Generic;
using Messaging.Observability;

namespace Messaging.Tests.Resilience;

/// <summary>
/// A recording <see cref="IDeadLetterMetrics"/> double that counts how many times the dead-letter
/// metric was recorded, so a test can assert the metric is emitted exactly once per successful
/// dead-letter (Req 8.4).
/// </summary>
internal sealed class RecordingDeadLetterMetrics : IDeadLetterMetrics
{
    /// <summary>The number of times <see cref="RecordDeadLetter"/> was called.</summary>
    public int RecordDeadLetterCount { get; private set; }

    /// <inheritdoc />
    public void RecordDeadLetter() => RecordDeadLetterCount++;
}

/// <summary>
/// A recording <see cref="IDeadLetterLog"/> double that captures the exact field values passed to
/// each structured log operation, so a test can assert the dead-letter entry carries the correct
/// correlation id, failure reason, and attempt count, and that it is emitted exactly once (Req 8.4).
/// </summary>
internal sealed class RecordingDeadLetterLog : IDeadLetterLog
{
    private readonly List<DeadLetteredEntry> _deadLettered = new();
    private readonly List<RoutingFailedEntry> _routingFailed = new();

    /// <summary>Every <see cref="DeadLettered"/> entry captured, in order.</summary>
    public IReadOnlyList<DeadLetteredEntry> DeadLetteredEntries => _deadLettered;

    /// <summary>Every <see cref="DeadLetterRoutingFailed"/> entry captured, in order.</summary>
    public IReadOnlyList<RoutingFailedEntry> RoutingFailedEntries => _routingFailed;

    /// <inheritdoc />
    public void DeadLettered(string correlationId, string failureReason, int attemptCount)
        => _deadLettered.Add(new DeadLetteredEntry(correlationId, failureReason, attemptCount));

    /// <inheritdoc />
    public void DeadLetterRoutingFailed(
        string correlationId,
        string failureReason,
        int attemptCount,
        Exception routingError)
        => _routingFailed.Add(new RoutingFailedEntry(correlationId, failureReason, attemptCount, routingError));

    /// <summary>A captured <see cref="DeadLettered"/> structured log entry.</summary>
    internal sealed record DeadLetteredEntry(string CorrelationId, string FailureReason, int AttemptCount);

    /// <summary>A captured <see cref="DeadLetterRoutingFailed"/> structured log entry.</summary>
    internal sealed record RoutingFailedEntry(
        string CorrelationId,
        string FailureReason,
        int AttemptCount,
        Exception RoutingError);
}
