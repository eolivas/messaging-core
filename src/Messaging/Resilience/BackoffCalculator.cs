using Messaging.Configuration;

namespace Messaging.Resilience;

/// <summary>
/// Computes the per-attempt backoff delay the core defines for a <see cref="RetryPolicyConfig"/> and
/// an attempt number (Req 6.4). The computed delay is always clamped to the inclusive bound
/// [<c>1s</c>, <c>300s</c>] sourced from <see cref="ConfigurationBounds.PerAttemptDelaySeconds"/>, so
/// no strategy, base delay, or attempt number can ever produce a delay outside the permitted range —
/// including when exponential growth would overflow (Req 6.4).
/// </summary>
/// <remarks>
/// <para>
/// This is the uniform policy surface only. The core does not run a bespoke redelivery engine; the
/// delay computed here <em>maps onto</em> the active provider's native retry/redelivery mechanism
/// (MassTransit redelivery, Kafka offset reprocessing, batch checkpoint re-read) in the later adapter
/// tasks. Delivery semantics are <b>at-least-once</b>.
/// </para>
/// <para>
/// Strategy shapes, where <c>base</c> is <see cref="RetryPolicyConfig.PerAttemptDelaySeconds"/> and
/// <c>n</c> is the 1-based attempt number:
/// </para>
/// <list type="bullet">
///   <item><description><see cref="BackoffStrategy.Fixed"/>: a constant <c>base</c> every attempt.</description></item>
///   <item><description><see cref="BackoffStrategy.Exponential"/>: <c>base * 2^(n-1)</c>, clamped at the max.</description></item>
///   <item><description><see cref="BackoffStrategy.ExponentialWithJitter"/>: a uniform random value in
///   <c>[1, exponential(n)]</c>, keeping the result inside the bound while spreading retries to avoid
///   stampedes.</description></item>
/// </list>
/// <para>
/// The computation is done with checked widening to <see cref="long"/>/<see cref="double"/> and a
/// saturating clamp, so a large attempt number saturates at the maximum rather than overflowing. The
/// result is a <see cref="TimeSpan"/> whose seconds component is always in [1, 300].
/// </para>
/// </remarks>
public sealed class BackoffCalculator
{
    private static readonly int MinDelaySeconds = ConfigurationBounds.PerAttemptDelaySeconds.Min;
    private static readonly int MaxDelaySeconds = ConfigurationBounds.PerAttemptDelaySeconds.Max;

    private readonly Func<int, int, int> _jitterSelector;

    /// <summary>
    /// Creates a calculator using a shared pseudo-random source for the jitter strategy.
    /// </summary>
    public BackoffCalculator()
        : this((minInclusive, maxInclusive) => Random.Shared.Next(minInclusive, maxInclusive + 1))
    {
    }

    /// <summary>
    /// Creates a calculator with an injected jitter selector, so a test can make the
    /// <see cref="BackoffStrategy.ExponentialWithJitter"/> strategy deterministic. The selector is
    /// given an inclusive <c>[min, max]</c> range and must return a value inside it.
    /// </summary>
    /// <param name="jitterSelector">
    /// A function returning a value within the inclusive <c>[min, max]</c> range it is passed.
    /// </param>
    /// <exception cref="ArgumentNullException">If <paramref name="jitterSelector"/> is null.</exception>
    public BackoffCalculator(Func<int, int, int> jitterSelector)
    {
        _jitterSelector = jitterSelector ?? throw new ArgumentNullException(nameof(jitterSelector));
    }

    /// <summary>
    /// Computes the clamped per-attempt backoff delay for the given policy and attempt number
    /// (Req 6.4). The returned delay is always within [1, 300] seconds inclusive.
    /// </summary>
    /// <param name="policy">The retry policy supplying the base delay and backoff strategy.</param>
    /// <param name="attemptNumber">
    /// The 1-based attempt number the delay precedes. Values below 1 are treated as 1.
    /// </param>
    /// <returns>The per-attempt delay, clamped to [1, 300] seconds.</returns>
    /// <exception cref="ArgumentNullException">If <paramref name="policy"/> is null.</exception>
    public TimeSpan ComputeDelay(RetryPolicyConfig policy, int attemptNumber)
    {
        ArgumentNullException.ThrowIfNull(policy);

        var attempt = attemptNumber < 1 ? 1 : attemptNumber;
        var baseDelay = Clamp(policy.PerAttemptDelaySeconds);

        var seconds = policy.Backoff switch
        {
            BackoffStrategy.Fixed => baseDelay,
            BackoffStrategy.Exponential => ExponentialSeconds(baseDelay, attempt),
            BackoffStrategy.ExponentialWithJitter => JitteredSeconds(baseDelay, attempt),
            _ => baseDelay,
        };

        return TimeSpan.FromSeconds(seconds);
    }

    /// <summary>
    /// <c>base * 2^(attempt-1)</c> computed in <see cref="long"/> with early saturation, then clamped.
    /// Using a widened accumulator and breaking once the max is reached avoids overflow for any
    /// attempt number.
    /// </summary>
    private static int ExponentialSeconds(int baseDelaySeconds, int attempt)
    {
        long value = baseDelaySeconds;
        for (var i = 1; i < attempt; i++)
        {
            value *= 2;
            if (value >= MaxDelaySeconds)
            {
                return MaxDelaySeconds;
            }
        }

        return Clamp((int)value);
    }

    /// <summary>
    /// A uniform random value in <c>[1, exponential(attempt)]</c>, which keeps every jittered delay
    /// inside the bound by construction (the upper bound is already clamped at the max).
    /// </summary>
    private int JitteredSeconds(int baseDelaySeconds, int attempt)
    {
        var ceiling = ExponentialSeconds(baseDelaySeconds, attempt);
        var selected = _jitterSelector(MinDelaySeconds, ceiling);
        return Clamp(selected);
    }

    /// <summary>Saturating clamp into [1, 300] seconds (Req 6.4).</summary>
    private static int Clamp(int seconds)
    {
        if (seconds < MinDelaySeconds)
        {
            return MinDelaySeconds;
        }

        return seconds > MaxDelaySeconds ? MaxDelaySeconds : seconds;
    }
}
