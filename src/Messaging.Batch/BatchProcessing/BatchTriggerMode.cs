namespace Messaging.Batch.BatchProcessing;

/// <summary>
/// How the batch consumer host decides when to run a pull cycle.
/// </summary>
public enum BatchTriggerMode
{
    /// <summary>Run a pull cycle on a fixed schedule (the <c>Schedule</c> interval).</summary>
    Scheduled,

    /// <summary>Run a pull cycle only when externally triggered (e.g. an on-demand signal).</summary>
    Triggered,
}
