namespace Messaging.Configuration;

/// <summary>
/// The retry backoff shape applied between redelivery attempts; mapped onto the active transport's
/// native retry mechanism rather than driven by a bespoke engine (Req 6.1, 6.4).
/// </summary>
public enum BackoffStrategy
{
    /// <summary>Constant delay between every attempt.</summary>
    Fixed,

    /// <summary>Delay grows exponentially with the attempt number (the default).</summary>
    Exponential,

    /// <summary>Exponential growth with randomized jitter to avoid retry stampedes.</summary>
    ExponentialWithJitter,
}
