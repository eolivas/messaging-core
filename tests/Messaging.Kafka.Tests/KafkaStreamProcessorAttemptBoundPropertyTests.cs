using FsCheck;
using FsCheck.Fluent;
using FsCheck.Xunit;
using Messaging.Abstractions;
using Messaging.Configuration;
using Messaging.Consuming;
using Messaging.Contracts;
using Messaging.Kafka.Streaming;
using Messaging.Observability;
using Messaging.Resilience;

namespace Messaging.Kafka.Tests;

// Feature: kafka-provider, Property 4: Bounded attempt count by failure kind.
// Validates: Requirements 7.1, 7.6.
//
// A message whose processing ALWAYS fails is driven through the REAL KafkaStreamMessageProcessor wired
// to the REAL core collaborators (ObservabilityContext, RetryExecutor, ConsumePipeline,
// DeadLetterRouter). The core retry policy is the only retry mechanism (no client-side redelivery), so:
//   - a TRANSIENT failure is attempted exactly MaxAttempts times, then dead-lettered (Req 7.1); and
//   - a NON-TRANSIENT failure (generic non-transient, missing required header, unknown contract) gets
//     exactly ONE attempt and zero retries (Req 7.6);
// and in no case does the attempt count exceed MaxAttempts. The retry delay is injected as a no-op and
// the dead-letter sink accepts every message, so ProcessAsync returns normally.
[Trait("Feature", "kafka-provider")]
[Trait("Property", "4")]
public class KafkaStreamProcessorAttemptBoundPropertyTests
{
    public enum AlwaysFailingKind
    {
        Transient,
        NonTransient,
        MissingRequiredHeader,
        UnknownContract,
    }

    [Property(MaxTest = 100, QuietOnSuccess = true, Arbitrary = new[] { typeof(KafkaStreamProcessorAttemptBoundPropertyTests) })]
    public Property ProcessAsync_AlwaysFailingMessage_AttemptsOnceWhenNonTransientAndMaxAttemptsWhenTransient(
        int maxAttempts,
        AlwaysFailingKind kind,
        string correlationId)
    {
        var binder = new CountingBinder(knownContract: kind != AlwaysFailingKind.UnknownContract);
        var handler = new AlwaysFailingHandlerInvoker(kind);
        var sink = new CountingAcceptingSink();
        KafkaStreamMessageProcessor processor = BuildProcessor(maxAttempts, binder, handler, sink);

        MessageEnvelope envelope = BuildEnvelope(correlationId);

        // Act: every terminal path is an accepted dead-letter, so this returns normally (commit).
        processor.ProcessAsync(envelope, CancellationToken.None).GetAwaiter().GetResult();

        int attempts = binder.Calls;
        int expected = kind == AlwaysFailingKind.Transient ? maxAttempts : 1;

        bool holds =
            attempts == expected
            && attempts <= maxAttempts
            && sink.Count == 1
            && (kind != AlwaysFailingKind.UnknownContract || handler.Calls == 0);

        return holds
            .ToProperty()
            .Classify(kind == AlwaysFailingKind.Transient, "transient -> MaxAttempts")
            .Classify(kind != AlwaysFailingKind.Transient, "non-transient -> 1 attempt")
            .Label($"kind={kind} maxAttempts={maxAttempts} expected={expected} actual={attempts} deadLetters={sink.Count} handlerCalls={handler.Calls}");
    }

    // ----- Composition ------------------------------------------------------------------------

    private static KafkaStreamMessageProcessor BuildProcessor(
        int maxAttempts,
        IContractBinder binder,
        IHandlerInvoker handler,
        IDeadLetterSink sink)
    {
        var observability = new ObservabilityContext(
            new NoOpMessagingLog(),
            new NoOpMessagingMetrics(),
            new MessagingTraceSource(tracingEnabled: false),
            new PayloadRedactor());

        var policy = new RetryPolicyConfig { MaxAttempts = maxAttempts };

        var router = new DeadLetterRouter(sink, new NoOpDeadLetterMetrics(), new NoOpDeadLetterLog());

        var executor = new RetryExecutor(
            new FailureClassifier(),
            new BackoffCalculator(static (min, _) => min),
            router,
            delay: static (_, _) => Task.CompletedTask);

        var pipeline = new ConsumePipeline(new ConsumerHostStateMachine(), binder, handler);
        pipeline.StartAsync(CancellationToken.None).GetAwaiter().GetResult();

        return new KafkaStreamMessageProcessor(observability, executor, pipeline, router, policy);
    }

    private static MessageEnvelope BuildEnvelope(string correlationId) =>
        new()
        {
            MessageId = Guid.NewGuid(),
            CorrelationId = correlationId,
            MessageType = "order.created",
            CreatedAtUtc = DateTimeOffset.UnixEpoch,
            Headers = new Dictionary<string, string>(),
            Payload = new ReadOnlyMemory<byte>(new byte[] { 1, 2, 3 }),
            ContractVersion = 1,
        };

    // ----- Generators -------------------------------------------------------------------------

    public static Arbitrary<int> MaxAttemptsArbitrary() =>
        Arb.From(Gen.Choose(ConfigurationBounds.MaxAttempts.Min, ConfigurationBounds.MaxAttempts.Max));

    public static Arbitrary<AlwaysFailingKind> KindArbitrary() =>
        Arb.From(Gen.Elements(
            AlwaysFailingKind.Transient,
            AlwaysFailingKind.NonTransient,
            AlwaysFailingKind.MissingRequiredHeader,
            AlwaysFailingKind.UnknownContract));

    public static Arbitrary<string> CorrelationIdArbitrary() =>
        Arb.From(
            from length in Gen.Choose(1, MessageEnvelope.MaxCorrelationIdLength)
            from chars in Gen.ArrayOf(
                Gen.Elements("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.:".ToCharArray()),
                length)
            select new string(chars));

    // ----- Fakes ------------------------------------------------------------------------------

    /// <summary>Counts binder consultations: one per processing attempt, since binding precedes the handler.</summary>
    private sealed class CountingBinder : IContractBinder
    {
        private readonly bool _knownContract;
        private int _calls;

        public CountingBinder(bool knownContract) => _knownContract = knownContract;

        public int Calls => _calls;

        public bool IsKnown(string messageType)
        {
            // An unknown contract may be rejected here without reaching Bind; count that attempt.
            if (!_knownContract)
            {
                _calls++;
            }

            return _knownContract;
        }

        public object Bind(MessageEnvelope envelope)
        {
            _calls++;
            return _knownContract
                ? new object()
                : throw new UnknownContractException(envelope.MessageType, "Unknown contract.");
        }
    }

    private sealed class AlwaysFailingHandlerInvoker : IHandlerInvoker
    {
        private readonly AlwaysFailingKind _kind;
        private int _calls;

        public AlwaysFailingHandlerInvoker(AlwaysFailingKind kind) => _kind = kind;

        public int Calls => _calls;

        public Task InvokeAsync(
            MessageEnvelope envelope,
            object boundMessage,
            ConsumeContext context,
            CancellationToken cancellationToken)
        {
            _calls++;
            throw _kind switch
            {
                AlwaysFailingKind.Transient => new DownstreamUnavailableException("Downstream unavailable."),
                AlwaysFailingKind.MissingRequiredHeader => new MissingRequiredHeaderException("x-header"),
                _ => new MessageValidationException("Message failed validation."),
            };
        }
    }

    private sealed class CountingAcceptingSink : IDeadLetterSink
    {
        private int _count;

        public int Count => _count;

        public Task SendAsync(DeadLetterMessage message, CancellationToken cancellationToken)
        {
            _count++;
            return Task.CompletedTask;
        }
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
