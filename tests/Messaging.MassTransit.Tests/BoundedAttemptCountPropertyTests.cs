using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FsCheck;
using FsCheck.Fluent;
using FsCheck.Xunit;
using Messaging.Abstractions;
using Messaging.Configuration;
using Messaging.Consuming;
using Messaging.Contracts;
using Messaging.MassTransit.Consuming;
using Messaging.Observability;
using Messaging.Resilience;

namespace Messaging.MassTransit.Tests;

// Feature: rabbitmq-provider, Property 4: Bounded attempt count by failure kind.
// Validates: Requirements 8.1, 8.3, 8.8.
//
// A message whose processing ALWAYS fails is driven through a REAL CoreConsumeProcessor wired to the
// REAL core collaborators (ObservabilityContext, RetryExecutor, ConsumePipeline, DeadLetterRouter).
// The core Retry_Policy is the single source of retry/backoff behaviour, so the total number of
// processing attempts for any delivered envelope is bounded exactly by the failure classification:
//
//   - a NON-TRANSIENT failure is dead-lettered on the first attempt with ZERO retries, so the handler
//     runs EXACTLY ONCE regardless of MaxAttempts (Req 8.3); and
//   - a TRANSIENT failure is retried under the bounded policy until it exhausts MaxAttempts, so the
//     handler runs EXACTLY MaxAttempts times (Req 8.1);
//
// and in neither case does the attempt count EXCEED MaxAttempts — no transport-level redelivery
// multiplies it, because the core policy is the only retry mechanism (Req 8.8).
//
// MaxAttempts is generated across its full validated range [1, 100] and the failure kind is generated
// (transient vs non-transient). The RetryExecutor's backoff delay is injected as a no-op so there is
// NO wall-clock wait across the up-to-100 transient retries, and the dead-letter sink is a mock that
// accepts every message. The handler invoker counts how many times processing was attempted — that
// count is the observable "total processing attempts" the property bounds.
[Trait("Feature", "rabbitmq-provider")]
[Trait("Property", "4")]
public class BoundedAttemptCountPropertyTests
{
    [Property(MaxTest = 100, QuietOnSuccess = true, Arbitrary = new[] { typeof(BoundedAttemptCountPropertyTests) })]
    public Property ProcessAsync_AlwaysFailingMessage_AttemptsExactlyOnceWhenNonTransientAndMaxAttemptsWhenTransient(
        int maxAttempts,
        FailureKind failureKind,
        MessageEnvelope envelope)
    {
        // Arrange: a handler that always fails with the generated failure kind, counting each attempt.
        var countingHandler = new CountingAlwaysFailingHandlerInvoker(failureKind);
        CoreConsumeProcessor processor = BuildProcessor(maxAttempts, countingHandler);

        // Act: drive the always-failing message through the real processor. Every terminal path here
        // is a dead-letter the mock sink accepts, so ProcessAsync returns normally (acknowledge) and
        // never throws RetainForRedeliveryException.
        processor.ProcessAsync(envelope, CancellationToken.None).GetAwaiter().GetResult();

        int attempts = countingHandler.InvocationCount;

        // A non-transient failure dead-letters on the first attempt with zero retries (exactly 1);
        // a transient failure exhausts the bounded policy (exactly MaxAttempts).
        int expectedAttempts = failureKind == FailureKind.NonTransient ? 1 : maxAttempts;

        bool holds = attempts == expectedAttempts && attempts <= maxAttempts;

        return holds
            .ToProperty()
            .Classify(failureKind == FailureKind.NonTransient, "non-transient -> 1 attempt")
            .Classify(failureKind == FailureKind.Transient, "transient -> MaxAttempts attempts")
            .Classify(maxAttempts == 1, "MaxAttempts = 1 (min)")
            .Classify(maxAttempts == ConfigurationBounds.MaxAttempts.Max, "MaxAttempts = 100 (max)")
            .Label(
                $"failureKind={failureKind} maxAttempts={maxAttempts} " +
                $"expectedAttempts={expectedAttempts} actualAttempts={attempts}");
    }

    // ----- Composition of a real processor driven by the counting handler ---------------------

    /// <summary>
    /// Builds a real <see cref="CoreConsumeProcessor"/> wired with the real core collaborators and the
    /// generated <paramref name="maxAttempts"/>. The retry executor's delay function is injected as a
    /// no-op so the up-to-100 transient retries incur no wall-clock wait, the backoff jitter selector
    /// is made deterministic, and the dead-letter sink is a mock that accepts every message so every
    /// terminal path acknowledges. Processing is the real <see cref="ConsumePipeline"/> fronting the
    /// supplied always-failing handler.
    /// </summary>
    private static CoreConsumeProcessor BuildProcessor(int maxAttempts, IHandlerInvoker handlerInvoker)
    {
        var observability = BuildObservability();

        var retryPolicy = new RetryPolicyConfig { MaxAttempts = maxAttempts };

        var deadLetterRouter = new DeadLetterRouter(
            new AcceptingDeadLetterSink(),
            new NoOpDeadLetterMetrics(),
            new NoOpDeadLetterLog());

        var retryExecutor = new RetryExecutor(
            new FailureClassifier(),
            // Deterministic jitter (lowest value) keeps the backoff computation exercised without
            // randomness; the delay below is a no-op so the value never actually waits.
            new BackoffCalculator(static (min, _) => min),
            deadLetterRouter,
            // Injected delay: NO wall-clock wait, so retrying up to MaxAttempts times is instant.
            delay: static (_, _) => Task.CompletedTask);

        ConsumePipeline consumePipeline = BuildRunningPipeline(handlerInvoker);

        return new CoreConsumeProcessor(
            observability,
            retryExecutor,
            consumePipeline,
            deadLetterRouter,
            retryPolicy);
    }

    /// <summary>
    /// Builds and starts a real <see cref="ConsumePipeline"/> whose binder always binds (so binding
    /// never short-circuits the failure) and whose handler is the supplied always-failing invoker, so
    /// every processing attempt reaches the handler and throws.
    /// </summary>
    private static ConsumePipeline BuildRunningPipeline(IHandlerInvoker handlerInvoker)
    {
        var pipeline = new ConsumePipeline(
            new ConsumerHostStateMachine(),
            new AlwaysBindContractBinder(),
            handlerInvoker);

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

    /// <summary>MaxAttempts across its full validated inclusive range [1, 100].</summary>
    public static Arbitrary<int> MaxAttemptsArbitrary() =>
        Arb.From(Gen.Choose(ConfigurationBounds.MaxAttempts.Min, ConfigurationBounds.MaxAttempts.Max));

    /// <summary>The generated failure classification: transient (retried) or non-transient (dead-lettered now).</summary>
    public static Arbitrary<FailureKind> FailureKindArbitrary() =>
        Arb.From(Gen.Elements(FailureKind.Transient, FailureKind.NonTransient));

    public static Arbitrary<MessageEnvelope> MessageEnvelopeArbitrary() =>
        Arb.From(EnvelopeGen());

    /// <summary>
    /// Generates an arbitrary valid envelope (correlation id 1..128 chars, 0..64 headers, arbitrary
    /// payload including empty), matching the envelope generator style used by the sibling property
    /// tests. The envelope content does not steer the attempt count — the failure kind and
    /// MaxAttempts do — but it exercises the bound across the full envelope input space.
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
        select (IReadOnlyDictionary<string, string>)Enumerable
            .Range(0, count)
            .ToDictionary(i => $"{keys[i]}-{i}", i => values[i], StringComparer.Ordinal);

    private static Gen<ReadOnlyMemory<byte>> PayloadGen() =>
        from length in Gen.Choose(0, 256)
        from bytes in Gen.ArrayOf(ByteGen(), length)
        select new ReadOnlyMemory<byte>(bytes);

    private static Gen<byte> ByteGen() =>
        from value in Gen.Choose(0, 255)
        select (byte)value;

    // ----- Minimal in-test fakes for the substitutable seams ----------------------------------

    /// <summary>Binds any envelope to a trivial contract object; binding never fails here.</summary>
    private sealed class AlwaysBindContractBinder : IContractBinder
    {
        public bool IsKnown(string messageType) => true;

        public object Bind(MessageEnvelope envelope) => new object();
    }

    /// <summary>
    /// A handler invoker whose attempt ALWAYS fails, throwing an exception classified as the given
    /// failure kind, and counting how many times processing was attempted. A transient failure uses
    /// <see cref="DownstreamUnavailableException"/> (retried); a non-transient failure uses
    /// <see cref="MessageValidationException"/> (dead-lettered immediately). The count is the
    /// observable total number of processing attempts the property bounds.
    /// </summary>
    private sealed class CountingAlwaysFailingHandlerInvoker : IHandlerInvoker
    {
        private readonly FailureKind _failureKind;
        private int _invocationCount;

        public CountingAlwaysFailingHandlerInvoker(FailureKind failureKind) => _failureKind = failureKind;

        public int InvocationCount => _invocationCount;

        public Task InvokeAsync(
            MessageEnvelope envelope,
            object boundMessage,
            ConsumeContext context,
            CancellationToken cancellationToken)
        {
            _invocationCount++;

            throw _failureKind == FailureKind.NonTransient
                ? new MessageValidationException("Message failed validation; retrying cannot succeed.")
                : new DownstreamUnavailableException("Downstream dependency is unavailable; retry under policy.");
        }
    }

    /// <summary>A dead-letter sink that accepts every message, so dead-letter routing always acknowledges.</summary>
    private sealed class AcceptingDeadLetterSink : IDeadLetterSink
    {
        public Task SendAsync(DeadLetterMessage message, CancellationToken cancellationToken) =>
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
