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
using Messaging.MassTransit;
using Messaging.MassTransit.Consuming;
using Messaging.Observability;
using Messaging.Resilience;

namespace Messaging.MassTransit.Tests;

// Feature: rabbitmq-provider, Property 3: Ack-versus-retain mapping.
// Validates: Requirements 1.9, 6.7, 7.2, 8.6, 8.7.
//
// CoreConsumeProcessor.ProcessAsync translates the core's terminal outcome onto MassTransit's
// acknowledge-vs-redeliver contract. For any generated terminal outcome the message can reach, the
// mapping is exactly:
//   - the processor RETURNS normally (MassTransit acknowledges / the message leaves the pipeline)
//     IFF the terminal outcome is one of
//       { handler success, idempotent duplicate / bypass, dead-letter routed-and-acknowledged }, AND
//   - the processor THROWS RetainForRedeliveryException (so MassTransit redelivers) IFF the
//     dead-letter sink retained the message unacknowledged (RetainedUnacknowledged).
//
// The property drives a REAL CoreConsumeProcessor wired to REAL core collaborators (ObservabilityContext,
// RetryExecutor with an injected no-op delay, ConsumePipeline, DeadLetterRouter). No broker is involved.
// The generated terminal-outcome category selects minimal in-test fakes for the substitutable seams
// (IContractBinder, IHandlerInvoker, IDeadLetterSink, and the idempotency ports) so the generated
// outcome flows deterministically through the production processor-to-ack mapping that is under test.
[Trait("Feature", "rabbitmq-provider")]
[Trait("Property", "3")]
public class AckVersusRetainMappingPropertyTests
{
    [Property(MaxTest = 100, QuietOnSuccess = true, Arbitrary = new[] { typeof(AckVersusRetainMappingPropertyTests) })]
    public Property ProcessAsync_AcrossEveryTerminalOutcome_AcksWhenMessageLeavesPipelineAndThrowsWhenRetained(
        TerminalOutcomeCategory category,
        MessageEnvelope envelope)
    {
        // Arrange: compose a real processor whose terminal outcome is driven by the generated category.
        CoreConsumeProcessor processor = BuildProcessor(category);

        // Act: capture whether the processor retained-for-redelivery (threw) or acknowledged (returned).
        bool threwRetain = InvokeAndObserveRetain(processor, envelope);

        // The one category that must retain-and-redeliver is the sink-retained dead-letter; every
        // other terminal category acknowledges by returning normally.
        bool expectedRetain = category == TerminalOutcomeCategory.DeadLetterRetainedUnacknowledged;

        bool holds = threwRetain == expectedRetain;

        return holds
            .ToProperty()
            .Classify(category == TerminalOutcomeCategory.HandlerSuccess, "handler success -> ack")
            .Classify(category == TerminalOutcomeCategory.IdempotentDuplicate, "idempotent duplicate -> ack")
            .Classify(category == TerminalOutcomeCategory.DeadLetterRoutedAndAcknowledged, "dead-letter routed+acked -> ack")
            .Classify(category == TerminalOutcomeCategory.DeadLetterRetainedUnacknowledged, "dead-letter retained -> retain")
            .Label($"category={category} expectedRetain={expectedRetain} actualRetain={threwRetain}");
    }

    /// <summary>
    /// Runs the processor synchronously and reports whether it threw <see cref="RetainForRedeliveryException"/>
    /// (retain-and-redeliver) or returned normally (acknowledge). Any other exception propagates and
    /// fails the test, since the processor's contract is that only a retained dead-letter throws.
    /// </summary>
    private static bool InvokeAndObserveRetain(CoreConsumeProcessor processor, MessageEnvelope envelope)
    {
        try
        {
            processor.ProcessAsync(envelope, CancellationToken.None).GetAwaiter().GetResult();
            return false;
        }
        catch (RetainForRedeliveryException)
        {
            return true;
        }
    }

    // ----- Composition of a real processor per generated outcome category ---------------------

    /// <summary>
    /// Builds a real <see cref="CoreConsumeProcessor"/> wired with real core collaborators, choosing
    /// minimal fakes for the substitutable seams so the generated <paramref name="category"/> produces
    /// the matching terminal outcome without a broker and without any wall-clock delay.
    /// </summary>
    private static CoreConsumeProcessor BuildProcessor(TerminalOutcomeCategory category)
    {
        var observability = BuildObservability();

        // MaxAttempts = 1 so a non-transient failure (used for the dead-letter categories) makes
        // exactly one attempt and no backoff delay is ever waited; the injected delay is also a no-op.
        var retryPolicy = new RetryPolicyConfig { MaxAttempts = 1 };

        var deadLetterSink = new ControllableDeadLetterSink(
            retain: category == TerminalOutcomeCategory.DeadLetterRetainedUnacknowledged);

        var deadLetterRouter = new DeadLetterRouter(
            deadLetterSink,
            new NoOpDeadLetterMetrics(),
            new NoOpDeadLetterLog());

        var retryExecutor = new RetryExecutor(
            new FailureClassifier(),
            new BackoffCalculator(static (min, _) => min),
            deadLetterRouter,
            delay: static (_, _) => Task.CompletedTask);

        ConsumePipeline consumePipeline = BuildRunningPipeline(category);

        return new CoreConsumeProcessor(
            observability,
            retryExecutor,
            consumePipeline,
            deadLetterRouter,
            retryPolicy);
    }

    /// <summary>
    /// Builds and starts a real <see cref="ConsumePipeline"/> whose per-message processing attempt
    /// resolves to the generated category:
    /// <list type="bullet">
    ///   <item><description><c>HandlerSuccess</c>: binder binds, handler returns -> pipeline succeeds.</description></item>
    ///   <item><description><c>IdempotentDuplicate</c>: a real idempotency gate reports the key already
    ///   processed -> pipeline short-circuits (duplicate) and returns, so the executor sees success.</description></item>
    ///   <item><description>both dead-letter categories: the handler throws a non-transient
    ///   <see cref="UnknownContractException"/> -> the executor routes straight to the sink with one
    ///   attempt; the sink's retain flag then decides routed-vs-retained.</description></item>
    /// </list>
    /// </summary>
    private static ConsumePipeline BuildRunningPipeline(TerminalOutcomeCategory category)
    {
        var stateMachine = new ConsumerHostStateMachine();
        var binder = new AlwaysBindContractBinder();

        IHandlerInvoker handlerInvoker = category is TerminalOutcomeCategory.DeadLetterRoutedAndAcknowledged
            or TerminalOutcomeCategory.DeadLetterRetainedUnacknowledged
            ? new ThrowingHandlerInvoker()
            : new SucceedingHandlerInvoker();

        ConsumePipeline pipeline;
        if (category == TerminalOutcomeCategory.IdempotentDuplicate)
        {
            // Real gate + ports so the duplicate short-circuit is the production path, not a fake.
            var gate = new IdempotencyGate(
                new FixedKeyProvider(),
                new AlwaysProcessedIdempotencyStore(),
                new IdempotencyConfig { Enabled = true });

            pipeline = new ConsumePipeline(stateMachine, binder, handlerInvoker, gate);
        }
        else
        {
            pipeline = new ConsumePipeline(stateMachine, binder, handlerInvoker);
        }

        // The pipeline only accepts deliveries while Running (Req 10.5).
        pipeline.StartAsync(CancellationToken.None).GetAwaiter().GetResult();
        return pipeline;
    }

    /// <summary>A real observability context wired to inert sinks; it is only driven, never asserted here.</summary>
    private static ObservabilityContext BuildObservability() =>
        new(
            new NoOpMessagingLog(),
            new NoOpMessagingMetrics(),
            new MessagingTraceSource(tracingEnabled: false),
            new PayloadRedactor());

    // ----- Generators -------------------------------------------------------------------------

    public static Arbitrary<TerminalOutcomeCategory> TerminalOutcomeCategoryArbitrary() =>
        Arb.From(Gen.Elements(
            TerminalOutcomeCategory.HandlerSuccess,
            TerminalOutcomeCategory.IdempotentDuplicate,
            TerminalOutcomeCategory.DeadLetterRoutedAndAcknowledged,
            TerminalOutcomeCategory.DeadLetterRetainedUnacknowledged));

    public static Arbitrary<MessageEnvelope> MessageEnvelopeArbitrary() =>
        Arb.From(EnvelopeGen());

    /// <summary>
    /// Generates an arbitrary valid envelope (correlation id 1..128 chars, 0..64 headers, arbitrary
    /// payload including empty), matching the envelope generator style used by the Property 1 test.
    /// The envelope's content does not steer the outcome — the category does — but it exercises the
    /// mapping across the full envelope input space.
    /// </summary>
    private static Gen<MessageEnvelope> EnvelopeGen() =>
        from messageId in Gen.Fresh(Guid.NewGuid).Where(id => id != Guid.Empty)
        from correlationId in CorrelationIdGen()
        from messageType in NonWhitespaceTokenGen()
        from createdAtUtc in UtcTimestampGen()
        from headers in HeadersGen()
        from payload in PayloadGen()
        from contractVersion in Gen.Choose(1, 50)
        select new MessageEnvelope
        {
            MessageId = messageId,
            CorrelationId = correlationId,
            MessageType = messageType,
            CreatedAtUtc = createdAtUtc,
            Headers = headers,
            Payload = payload,
            ContractVersion = contractVersion,
        };

    private static Gen<string> CorrelationIdGen() =>
        from length in Gen.Choose(1, MessageEnvelope.MaxCorrelationIdLength)
        from chars in Gen.ArrayOf(TokenCharGen(), length)
        select new string(chars);

    private static Gen<string> NonWhitespaceTokenGen() =>
        from length in Gen.Choose(1, 32)
        from chars in Gen.ArrayOf(TokenCharGen(), length)
        select new string(chars);

    private static Gen<char> TokenCharGen() => Gen.Elements(
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.:".ToCharArray());

    private static Gen<DateTimeOffset> UtcTimestampGen() =>
        from ticks in Gen.Choose(0, int.MaxValue)
        select new DateTimeOffset(
            DateTimeOffset.UnixEpoch.Ticks + ((long)ticks * TimeSpan.TicksPerSecond),
            TimeSpan.Zero);

    private static Gen<IReadOnlyDictionary<string, string>> HeadersGen() =>
        from count in Gen.Choose(0, MessageEnvelope.MaxHeaderCount)
        from keys in Gen.ArrayOf(NonWhitespaceTokenGen(), count)
        from values in Gen.ArrayOf(NonWhitespaceTokenGen(), count)
        select (IReadOnlyDictionary<string, string>)System.Linq.Enumerable
            .Range(0, count)
            .ToDictionary(i => $"{keys[i]}-{i}", i => values[i], StringComparer.Ordinal);

    private static Gen<ReadOnlyMemory<byte>> PayloadGen() =>
        from length in Gen.Choose(0, 256)
        from bytes in Gen.ArrayOf(ByteGen(), length)
        select new ReadOnlyMemory<byte>(bytes);

    private static Gen<byte> ByteGen() =>
        from value in Gen.Choose(0, 255)
        select (byte)value;

    // ----- Generated categories ---------------------------------------------------------------

    /// <summary>
    /// The terminal-outcome categories a consumed message can reach. The first three must acknowledge
    /// (the message leaves the pipeline); only the last must retain-and-redeliver.
    /// </summary>
    public enum TerminalOutcomeCategory
    {
        /// <summary>The handler ran and succeeded -> <see cref="RetryOutcome.Succeeded"/> -> ack.</summary>
        HandlerSuccess,

        /// <summary>The idempotency gate short-circuited a duplicate -> pipeline returns -> success -> ack.</summary>
        IdempotentDuplicate,

        /// <summary>Non-transient failure dead-lettered and the sink accepted it -> routed+acked -> ack.</summary>
        DeadLetterRoutedAndAcknowledged,

        /// <summary>Non-transient failure dead-lettered but the sink retained it -> throw to redeliver.</summary>
        DeadLetterRetainedUnacknowledged,
    }

    // ----- Minimal in-test fakes for the substitutable seams ----------------------------------

    /// <summary>Binds any envelope to a trivial contract object; binding never fails here.</summary>
    private sealed class AlwaysBindContractBinder : IContractBinder
    {
        public bool IsKnown(string messageType) => true;

        public object Bind(MessageEnvelope envelope) => new object();
    }

    /// <summary>A handler invoker whose attempt completes normally (the success path).</summary>
    private sealed class SucceedingHandlerInvoker : IHandlerInvoker
    {
        public Task InvokeAsync(
            MessageEnvelope envelope,
            object boundMessage,
            ConsumeContext context,
            CancellationToken cancellationToken) => Task.CompletedTask;
    }

    /// <summary>
    /// A handler invoker that throws a non-transient failure, so the retry executor dead-letters with
    /// exactly one attempt (no retry, no delay) and the sink decides the terminal routing result.
    /// </summary>
    private sealed class ThrowingHandlerInvoker : IHandlerInvoker
    {
        public Task InvokeAsync(
            MessageEnvelope envelope,
            object boundMessage,
            ConsumeContext context,
            CancellationToken cancellationToken) =>
            throw new UnknownContractException(
                envelope.MessageType,
                $"No contract is registered for message type '{envelope.MessageType}'.");
    }

    /// <summary>A dead-letter sink that either accepts (routed+acked) or throws to force retention.</summary>
    private sealed class ControllableDeadLetterSink : IDeadLetterSink
    {
        private readonly bool _retain;

        public ControllableDeadLetterSink(bool retain) => _retain = retain;

        public Task SendAsync(DeadLetterMessage message, CancellationToken cancellationToken)
        {
            if (_retain)
            {
                // Sink failure -> DeadLetterRouter returns RetainedUnacknowledged (Req 8.6).
                throw new InvalidOperationException("Dead-letter sink refused the message.");
            }

            return Task.CompletedTask;
        }
    }

    /// <summary>Derives a fixed, always-available idempotency key so the gate reaches the store lookup.</summary>
    private sealed class FixedKeyProvider : IIdempotencyKeyProvider
    {
        public bool TryDerive(MessageEnvelope envelope, out string idempotencyKey)
        {
            idempotencyKey = "fixed-idempotency-key";
            return true;
        }
    }

    /// <summary>A store that reports every key as already processed, forcing the duplicate short-circuit.</summary>
    private sealed class AlwaysProcessedIdempotencyStore : IIdempotencyStore
    {
        public Task<bool> IsProcessedAsync(string idempotencyKey, CancellationToken cancellationToken) =>
            Task.FromResult(true);

        public Task RecordProcessedAsync(string idempotencyKey, TimeSpan retention, CancellationToken cancellationToken) =>
            Task.CompletedTask;
    }

    private sealed class NoOpMessagingLog : IMessagingLog
    {
        public void MessageEvent(MessagingEventType eventType, string correlationId, DateTimeOffset timestampUtc)
        {
        }
    }

    private sealed class NoOpMessagingMetrics : IMessagingMetrics
    {
        public void RecordPublished()
        {
        }

        public void RecordConsumed()
        {
        }

        public void RecordRetry()
        {
        }

        public void RecordDeadLetter()
        {
        }
    }

    private sealed class NoOpDeadLetterMetrics : IDeadLetterMetrics
    {
        public void RecordDeadLetter()
        {
        }
    }

    private sealed class NoOpDeadLetterLog : IDeadLetterLog
    {
        public void DeadLettered(string correlationId, string failureReason, int attemptCount)
        {
        }

        public void DeadLetterRoutingFailed(string correlationId, string failureReason, int attemptCount, Exception error)
        {
        }
    }
}
