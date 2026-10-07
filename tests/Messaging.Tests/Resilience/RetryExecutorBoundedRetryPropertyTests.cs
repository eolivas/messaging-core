using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using FsCheck;
using FsCheck.Fluent;
using FsCheck.Xunit;
using Messaging.Abstractions;
using Messaging.Configuration;
using Messaging.Contracts;
using Messaging.Resilience;

namespace Messaging.Tests.Resilience;

// Feature: messaging-core, Property 12: Retry is bounded and routes to dead-letter on exhaustion.
// Validates: Requirements 6.2, 6.3, 6.5.
//
// For any valid retry policy (max attempts 1..100) and any always-failing processing attempt:
//   TRANSIENT failure     — the attempt is retried at most MaxAttempts times (processed exactly
//                           MaxAttempts times, never more — Req 6.2), and on exhaustion the message
//                           is routed EXACTLY ONCE to the dead-letter sink (Req 6.5). The reported
//                           outcome is DeadLetteredExhausted with AttemptCount == MaxAttempts.
//   NON-TRANSIENT failure — the message is routed to the dead-letter sink IMMEDIATELY with ZERO
//                           retries (processed exactly once — Req 6.3), routed exactly once, outcome
//                           DeadLetteredNonTransient with AttemptCount == 1.
//
// Backoff waits are stubbed to a no-op so the property runs without wall-clock delay while still
// exercising the bounded-attempt and dead-letter routing policy.
[Trait("Feature", "messaging-core")]
[Trait("Property", "12")]
public class RetryExecutorBoundedRetryPropertyTests
{
    /// <summary>A generated scenario: how many attempts the policy allows and whether the failure is transient.</summary>
    public sealed record RetryScenario(int MaxAttempts, bool Transient);

    [Property(MaxTest = 200, QuietOnSuccess = true, Arbitrary = new[] { typeof(RetryExecutorBoundedRetryPropertyTests) })]
    public Property Retry_IsBounded_AndDeadLettersOnExhaustion(RetryScenario scenario)
    {
        // Arrange: a classifier over real failure categories, a recording sink wrapped in the
        // dead-letter router (with no-op observability), and a no-op delay.
        var sink = new RecordingDeadLetterSink();
        var router = new DeadLetterRouter(sink, new NoOpDeadLetterMetrics(), new NoOpDeadLetterLog());
        var executor = new RetryExecutor(
            new FailureClassifier(),
            new BackoffCalculator(),
            router,
            delay: static (_, _) => Task.CompletedTask);

        var policy = new RetryPolicyConfig
        {
            MaxAttempts = scenario.MaxAttempts,
            Backoff = BackoffStrategy.Exponential,
            PerAttemptDelaySeconds = 5,
        };

        Exception failure = scenario.Transient
            ? new TransportConnectionException("transient connection failure")
            : new MessageValidationException("non-transient validation failure");

        var processedCount = 0;
        Task AlwaysFail(CancellationToken _)
        {
            processedCount++;
            return Task.FromException(failure);
        }

        var envelope = BuildEnvelope();

        // Act.
        var result = executor
            .ExecuteAsync(envelope, policy, AlwaysFail, idempotencyKey: "idem-key", CancellationToken.None)
            .GetAwaiter()
            .GetResult();

        // Assert.
        bool holds;
        if (scenario.Transient)
        {
            holds =
                result.Outcome == RetryOutcome.DeadLetteredExhausted
                && result.AttemptCount == scenario.MaxAttempts
                && processedCount == scenario.MaxAttempts   // bounded: at most/exactly MaxAttempts (Req 6.2)
                && sink.Sent.Count == 1                     // routed exactly once on exhaustion (Req 6.5)
                && sink.Sent[0].AttemptCount == scenario.MaxAttempts
                && ReferenceEquals(sink.Sent[0].Envelope, envelope);
        }
        else
        {
            holds =
                result.Outcome == RetryOutcome.DeadLetteredNonTransient
                && result.AttemptCount == 1
                && processedCount == 1                      // zero retries (Req 6.3)
                && sink.Sent.Count == 1                     // routed exactly once
                && sink.Sent[0].AttemptCount == 1
                && ReferenceEquals(sink.Sent[0].Envelope, envelope);
        }

        return holds
            .ToProperty()
            .Label(
                $"transient={scenario.Transient}, maxAttempts={scenario.MaxAttempts} => " +
                $"outcome={result.Outcome}, attempts={result.AttemptCount}, " +
                $"processed={processedCount}, deadLettered={sink.Sent.Count}")
            .Classify(scenario.Transient, "transient (exhaustion path)")
            .Classify(!scenario.Transient, "non-transient (immediate path)")
            .Classify(scenario.MaxAttempts == 1, "single attempt");
    }

    private static MessageEnvelope BuildEnvelope() => new()
    {
        MessageId = Guid.NewGuid(),
        CorrelationId = "corr-abc",
        MessageType = "orders.created",
        CreatedAtUtc = DateTimeOffset.UtcNow,
        Headers = new Dictionary<string, string>(),
        Payload = new ReadOnlyMemory<byte>(new byte[] { 1, 2, 3 }),
        ContractVersion = 1,
    };

    // ----- Generators -------------------------------------------------------------------------

    public static Arbitrary<RetryScenario> RetryScenarioArbitrary() => Arb.From(RetryScenarioGen());

    /// <summary>
    /// Generates a scenario across the full valid max-attempts range (1..100, per
    /// <see cref="ConfigurationBounds.MaxAttempts"/>) and both failure classifications, so the single
    /// property exercises both the exhaustion path and the immediate-dead-letter path.
    /// </summary>
    private static Gen<RetryScenario> RetryScenarioGen() =>
        from maxAttempts in Gen.Choose(ConfigurationBounds.MaxAttempts.Min, ConfigurationBounds.MaxAttempts.Max)
        from transient in Gen.Elements(true, false)
        select new RetryScenario(maxAttempts, transient);
}
