using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using FsCheck;
using FsCheck.Fluent;
using FsCheck.Xunit;
using Messaging.Configuration;
using Messaging.Consuming;
using Messaging.Contracts;
using Messaging.Resilience;
using Messaging.Tests.Fakes;
using Messaging.Tests.Resilience;

namespace Messaging.Tests.Consuming;

// Feature: missing-idempotency-key-classification-fix, Property 1: Missing idempotency key is non-transient.
// Validates: Requirements 1.1, 1.2, 1.3, 1.4, 2.1, 2.2, 2.3.
//
// For any envelope whose idempotency key cannot be derived (empty MessageId or blank MessageType) and
// any MaxAttempts in 1..10, with idempotency ENABLED, the real ConsumePipeline + IdempotencyGate driven
// through the real RetryExecutor + FailureClassifier must dead-letter on the FIRST attempt:
//   AttemptCount == 1, zero handler invocations, zero idempotency store lookups and writes.
//
// EXPECTED to FAIL on unfixed code for MaxAttempts > 1 (the exception is classified Transient, so the
// executor retries MaxAttempts times). MaxAttempts == 1 passes on unfixed code.
[Trait("Feature", "missing-idempotency-key-classification-fix")]
[Trait("Property", "1")]
public class MissingIdempotencyKeyNonTransientPropertyTests
{
    /// <summary>An underivable-key envelope plus the retry bound under which it is consumed.</summary>
    public sealed record Scenario(MessageEnvelope Envelope, int MaxAttempts, string Reason);

    [Property(MaxTest = 200, QuietOnSuccess = true, Arbitrary = new[] { typeof(MissingIdempotencyKeyNonTransientPropertyTests) })]
    public Property UnderivableKey_AnyMaxAttempts_DeadLettersOnFirstAttempt(Scenario scenario)
    {
        // Arrange: real gate (enabled) + pipeline, real executor/classifier, recording sink.
        var invoker = new CountingHandlerInvoker();
        var store = new FakeIdempotencyStore();
        var gate = new IdempotencyGate(
            new DefaultIdempotencyKeyProvider(),
            store,
            new IdempotencyConfig { Enabled = true });
        var pipeline = new ConsumePipeline(
            new ConsumerHostStateMachine(),
            new LifecycleStubContractBinder(),
            invoker,
            gate);
        pipeline.StartAsync(CancellationToken.None).GetAwaiter().GetResult();

        var sink = new RecordingDeadLetterSink();
        var executor = new RetryExecutor(
            new FailureClassifier(),
            new BackoffCalculator(),
            new DeadLetterRouter(sink, new NoOpDeadLetterMetrics(), new NoOpDeadLetterLog()),
            delay: static (_, _) => Task.CompletedTask);
        var policy = new RetryPolicyConfig { MaxAttempts = scenario.MaxAttempts };

        // Act
        var result = executor
            .ExecuteAsync(
                scenario.Envelope,
                policy,
                ct => pipeline.DeliverAsync(scenario.Envelope, ct),
                idempotencyKey: null,
                CancellationToken.None)
            .GetAwaiter()
            .GetResult();

        // Assert
        var attemptCountIsOne = sink.Sent.Count == 1 && sink.Sent[0].AttemptCount == 1 && result.AttemptCount == 1;

        return (attemptCountIsOne
                && invoker.InvocationCount == 0
                && store.Lookups.Count == 0
                && store.Writes.Count == 0)
            .ToProperty()
            .Label(
                $"reason={scenario.Reason}, maxAttempts={scenario.MaxAttempts} => outcome={result.Outcome}, " +
                $"resultAttempts={result.AttemptCount}, deadLetters={sink.Sent.Count}, " +
                $"handlerInvocations={invoker.InvocationCount}, lookups={store.Lookups.Count}, writes={store.Writes.Count}");
    }

    // ----- Generators -------------------------------------------------------------------------

    public static Arbitrary<Scenario> ScenarioArbitrary() => Arb.From(ScenarioGen());

    private static Gen<Scenario> ScenarioGen()
    {
        var correlationIdGen = Gen.Choose(1, MessageEnvelope.MaxCorrelationIdLength)
            .SelectMany(length => Gen.ArrayOf(Gen.Elements("abcdefghijklmnopqrstuvwxyz0123456789".ToCharArray()), length))
            .Select(chars => new string(chars));

        var timestampGen = from offsetSeconds in Gen.Choose(-315_360_000, 315_360_000)
                           select new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero).AddSeconds(offsetSeconds);

        var byteGen = Gen.Choose(0, 255).Select(b => (byte)b);
        var payloadGen = Gen.Choose(0, 32)
            .SelectMany(len => Gen.ArrayOf(byteGen, len))
            .Select(bytes => new ReadOnlyMemory<byte>(bytes));

        var tokenCharGen = Gen.Elements("abcdefghijklmnopqrstuvwxyz.-_0123456789".ToCharArray());
        var nonBlankTypeGen =
            (from length in Gen.Choose(1, 24)
             from chars in Gen.ArrayOf(tokenCharGen, length)
             select new string(chars))
            .Where(messageType => !string.IsNullOrWhiteSpace(messageType));
        var blankTypeGen = Gen.Elements("", " ", "   ", "\t", "\n", " \t \n ");

        var maxAttemptsGen = Gen.Choose(1, 10);

        var emptyIdCase =
            from correlationId in correlationIdGen
            from messageType in nonBlankTypeGen
            from createdAtUtc in timestampGen
            from payload in payloadGen
            from maxAttempts in maxAttemptsGen
            select new Scenario(
                BuildEnvelope(Guid.Empty, messageType, correlationId, createdAtUtc, payload),
                maxAttempts,
                "empty-message-id");

        var blankTypeCase =
            from correlationId in correlationIdGen
            from messageType in blankTypeGen
            from createdAtUtc in timestampGen
            from payload in payloadGen
            from maxAttempts in maxAttemptsGen
            select new Scenario(
                BuildEnvelope(Guid.NewGuid(), messageType, correlationId, createdAtUtc, payload),
                maxAttempts,
                "blank-message-type");

        return Gen.OneOf(emptyIdCase, blankTypeCase);
    }

    private static MessageEnvelope BuildEnvelope(
        Guid messageId,
        string messageType,
        string correlationId,
        DateTimeOffset createdAtUtc,
        ReadOnlyMemory<byte> payload) => new()
    {
        MessageId = messageId,
        CorrelationId = correlationId,
        MessageType = messageType,
        CreatedAtUtc = createdAtUtc,
        Headers = new Dictionary<string, string>(),
        Payload = payload,
        ContractVersion = 1,
    };
}
