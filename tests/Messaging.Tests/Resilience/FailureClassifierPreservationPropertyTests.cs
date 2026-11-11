using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using FsCheck;
using FsCheck.Fluent;
using FsCheck.Xunit;
using Messaging.Abstractions;
using Messaging.Configuration;
using Messaging.Consuming;
using Messaging.Contracts;
using Messaging.Resilience;

namespace Messaging.Tests.Resilience;

// Feature: missing-idempotency-key-classification-fix, Property 2: Preservation.
// Validates: Requirements 3.1, 3.2, 3.3, 3.4, 3.5, 3.6.
//
// Observed on UNFIXED code and kept as a regression guard:
//   transient (MessageTimeout, TimeoutException, TransportConnection, DownstreamUnavailable)
//       => Transient, retried MaxAttempts times, dead-lettered with AttemptCount == MaxAttempts
//   non-transient (MessageValidation, MalformedPayload, InvalidEnvelope, UnknownContract)
//       => NonTransient, dead-lettered with AttemptCount == 1
//   unrecognized (InvalidOperationException)
//       => Transient, uses all MaxAttempts
[Trait("Feature", "missing-idempotency-key-classification-fix")]
[Trait("Property", "2")]
public class FailureClassifierPreservationPropertyTests
{
    /// <summary>A generated scenario: which known failure to raise and the retry bound.</summary>
    public sealed record PreservationScenario(int FailureIndex, int MaxAttempts);

    // Fixed set of failures with their expected, previously observed classification.
    private static readonly (string Name, Func<Exception> Create, FailureKind Expected)[] Failures =
    {
        ("MessageTimeoutException", () => new MessageTimeoutException("timeout"), FailureKind.Transient),
        ("TimeoutException", () => new TimeoutException("timeout"), FailureKind.Transient),
        ("TransportConnectionException", () => new TransportConnectionException("connection"), FailureKind.Transient),
        ("DownstreamUnavailableException", () => new DownstreamUnavailableException("downstream"), FailureKind.Transient),
        ("MessageValidationException", () => new MessageValidationException("invalid"), FailureKind.NonTransient),
        ("MalformedPayloadException", () => new MalformedPayloadException("malformed"), FailureKind.NonTransient),
        ("InvalidEnvelopeException", () => new InvalidEnvelopeException("MessageId", "bad envelope"), FailureKind.NonTransient),
        ("UnknownContractException", () => new UnknownContractException("orders.unknown", "unknown contract"), FailureKind.NonTransient),
        ("InvalidOperationException", () => new InvalidOperationException("unrecognized"), FailureKind.Transient),
    };

    [Property(MaxTest = 100, QuietOnSuccess = true, Arbitrary = new[] { typeof(FailureClassifierPreservationPropertyTests) })]
    public Property Classify_OtherFailureKinds_KeepExistingClassification(PreservationScenario scenario)
    {
        // Arrange
        var (name, create, expected) = Failures[scenario.FailureIndex];
        var classifier = new FailureClassifier();

        // Act
        var kind = classifier.Classify(create());

        // Assert
        return (kind == expected)
            .ToProperty()
            .Label($"{name} => {kind}, expected {expected}");
    }

    [Property(MaxTest = 100, QuietOnSuccess = true, Arbitrary = new[] { typeof(FailureClassifierPreservationPropertyTests) })]
    public Property Execute_TransientFailure_RetriesUpToMaxAttempts(PreservationScenario scenario)
    {
        // Arrange
        var (name, create, expected) = Failures[scenario.FailureIndex];
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
        var failure = create();
        var processed = 0;
        Task AlwaysFail(CancellationToken _)
        {
            processed++;
            return Task.FromException(failure);
        }

        // Act
        var result = executor
            .ExecuteAsync(BuildEnvelope(), policy, AlwaysFail, idempotencyKey: "idem-key", CancellationToken.None)
            .GetAwaiter()
            .GetResult();

        // Assert
        var expectedAttempts = expected == FailureKind.Transient ? scenario.MaxAttempts : 1;
        var holds =
            result.AttemptCount == expectedAttempts
            && processed == expectedAttempts
            && sink.Sent.Count == 1
            && sink.Sent[0].AttemptCount == expectedAttempts;

        return holds
            .ToProperty()
            .Label($"{name}, maxAttempts={scenario.MaxAttempts} => attempts={result.AttemptCount}, " +
                   $"processed={processed}, deadLettered={sink.Sent.Count}, expectedAttempts={expectedAttempts}");
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

    public static Arbitrary<PreservationScenario> PreservationScenarioArbitrary() => Arb.From(ScenarioGen());

    private static Gen<PreservationScenario> ScenarioGen() =>
        from index in Gen.Choose(0, Failures.Length - 1)
        from maxAttempts in Gen.Choose(ConfigurationBounds.MaxAttempts.Min, ConfigurationBounds.MaxAttempts.Max)
        select new PreservationScenario(index, maxAttempts);
}
