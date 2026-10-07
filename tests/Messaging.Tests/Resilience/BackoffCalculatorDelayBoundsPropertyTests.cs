using System;
using FsCheck;
using FsCheck.Fluent;
using FsCheck.Xunit;
using Messaging.Configuration;
using Messaging.Resilience;

namespace Messaging.Tests.Resilience;

// Feature: messaging-core, Property 13: Backoff delay stays within bounds.
// Validates: Requirement 6.4.
//
// For ANY valid RetryPolicyConfig (per-attempt base delay 1..300, any backoff strategy) and ANY
// attempt number — including attempt numbers large enough to overflow a naive exponential — the
// computed per-attempt delay is ALWAYS within [1, 300] seconds inclusive. The bound comes from the
// single source of truth, ConfigurationBounds.PerAttemptDelaySeconds, so the test and the
// implementation never drift apart.
[Trait("Feature", "messaging-core")]
[Trait("Property", "13")]
public class BackoffCalculatorDelayBoundsPropertyTests
{
    private static readonly double MinSeconds = ConfigurationBounds.PerAttemptDelaySeconds.Min;
    private static readonly double MaxSeconds = ConfigurationBounds.PerAttemptDelaySeconds.Max;

    /// <summary>A generated input: a valid base delay, a backoff strategy, and an attempt number.</summary>
    public sealed record BackoffInput(int BaseDelaySeconds, BackoffStrategy Strategy, int AttemptNumber);

    [Property(MaxTest = 500, QuietOnSuccess = true, Arbitrary = new[] { typeof(BackoffCalculatorDelayBoundsPropertyTests) })]
    public Property BackoffDelay_StaysWithinBounds(BackoffInput input)
    {
        // A deterministic jitter selector returns the upper end of the range — the worst case for the
        // upper bound — so the property proves the clamp holds even at the maximum jitter draw.
        var calculator = new BackoffCalculator(jitterSelector: static (_, maxInclusive) => maxInclusive);

        var policy = new RetryPolicyConfig
        {
            MaxAttempts = ConfigurationBounds.MaxAttempts.Max,
            Backoff = input.Strategy,
            PerAttemptDelaySeconds = input.BaseDelaySeconds,
        };

        var delay = calculator.ComputeDelay(policy, input.AttemptNumber);
        var seconds = delay.TotalSeconds;

        var withinBounds = seconds >= MinSeconds && seconds <= MaxSeconds;

        return withinBounds
            .ToProperty()
            .Label(
                $"base={input.BaseDelaySeconds}s, strategy={input.Strategy}, attempt={input.AttemptNumber} " +
                $"=> delay={seconds}s (permitted [{MinSeconds}, {MaxSeconds}])")
            .Classify(input.Strategy == BackoffStrategy.ExponentialWithJitter, "jitter")
            .Classify(input.AttemptNumber > 60, "overflow-prone attempt number");
    }

    // ----- Generators -------------------------------------------------------------------------

    public static Arbitrary<BackoffInput> BackoffInputArbitrary() => Arb.From(BackoffInputGen());

    /// <summary>
    /// Generates a valid base delay within [1, 300] (per <see cref="ConfigurationBounds.PerAttemptDelaySeconds"/>),
    /// any <see cref="BackoffStrategy"/>, and an attempt number spanning 1 up to a value far beyond
    /// where <c>base * 2^(n-1)</c> would overflow — ensuring the saturating clamp is exercised.
    /// </summary>
    private static Gen<BackoffInput> BackoffInputGen() =>
        from baseDelay in Gen.Choose(
            ConfigurationBounds.PerAttemptDelaySeconds.Min,
            ConfigurationBounds.PerAttemptDelaySeconds.Max)
        from strategy in Gen.Elements(
            BackoffStrategy.Fixed,
            BackoffStrategy.Exponential,
            BackoffStrategy.ExponentialWithJitter)
        from attempt in Gen.Choose(1, 1000)
        select new BackoffInput(baseDelay, strategy, attempt);
}
