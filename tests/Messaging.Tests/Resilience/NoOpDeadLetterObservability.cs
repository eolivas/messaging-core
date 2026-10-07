using System;
using Messaging.Observability;

namespace Messaging.Tests.Resilience;

/// <summary>
/// No-op <see cref="IDeadLetterMetrics"/> and <see cref="IDeadLetterLog"/> doubles for tests that
/// exercise dead-letter routing but do not assert on the emitted metric or structured log. They let a
/// test build a <see cref="Messaging.Resilience.DeadLetterRouter"/> around a recording sink without
/// standing up real observability.
/// </summary>
internal sealed class NoOpDeadLetterMetrics : IDeadLetterMetrics
{
    public void RecordDeadLetter()
    {
    }
}

internal sealed class NoOpDeadLetterLog : IDeadLetterLog
{
    public void DeadLettered(string correlationId, string failureReason, int attemptCount)
    {
    }

    public void DeadLetterRoutingFailed(
        string correlationId,
        string failureReason,
        int attemptCount,
        Exception routingError)
    {
    }
}
