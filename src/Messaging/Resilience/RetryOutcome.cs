namespace Messaging.Resilience;

/// <summary>
/// The terminal disposition of a message driven through the retry policy (Req 6.2, 6.3, 6.5).
/// </summary>
public enum RetryOutcome
{
    /// <summary>A processing attempt succeeded; the message is done and was not dead-lettered.</summary>
    Succeeded,

    /// <summary>
    /// A non-transient failure routed the message straight to the dead-letter sink with zero retries
    /// (Req 6.3).
    /// </summary>
    DeadLetteredNonTransient,

    /// <summary>
    /// A transient failure exhausted the configured maximum attempts and the message was routed to
    /// the dead-letter sink (Req 6.5).
    /// </summary>
    DeadLetteredExhausted,
}
