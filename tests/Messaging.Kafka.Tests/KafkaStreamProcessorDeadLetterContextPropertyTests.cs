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

// Feature: kafka-provider, Property 5: Dead-letter preserves context.
// Validates: Requirements 7.5.
//
// For any envelope that is dead-lettered by KafkaStreamMessageProcessor, the DeadLetterMessage captured
// by the sink preserves the message id, the correlation id (char-for-char), the payload bytes (length
// and every byte) and the exact total attempt count (1 when routed without retry; the policy bound when
// retries were exhausted).
//
// A REAL processor is wired to REAL core collaborators (RetryExecutor with a no-op delay,
// ConsumePipeline, DeadLetterRouter, ObservabilityContext). No broker is involved.
[Trait("Feature", "kafka-provider")]
[Trait("Property", "5")]
public class KafkaStreamProcessorDeadLetterContextPropertyTests
{
    [Property(MaxTest = 100, QuietOnSuccess = true)]
    public Property ProcessAsync_WhenDeadLettered_PreservesIdCorrelationPayloadAndAttemptCount(
        NonEmptyString correlationSeed,
        byte[] payloadSeed,
        bool exhausted,
        byte attemptSeed)
    {
        // Arrange
        string correlationId = correlationSeed.Get.Length > MessageEnvelope.MaxCorrelationIdLength
            ? correlationSeed.Get[..MessageEnvelope.MaxCorrelationIdLength]
            : correlationSeed.Get;
        byte[] payload = payloadSeed ?? Array.Empty<byte>();
        int maxAttempts = 2 + (attemptSeed % 3); // 2..4, only relevant when retries are exhausted.
        int expectedAttempts = exhausted ? maxAttempts : 1;

        var envelope = new MessageEnvelope
        {
            MessageId = Guid.NewGuid(),
            CorrelationId = correlationId,
            MessageType = "test.contract",
            CreatedAtUtc = DateTimeOffset.UtcNow,
            Headers = new Dictionary<string, string>(),
            Payload = new ReadOnlyMemory<byte>((byte[])payload.Clone()),
        };

        var sink = new CapturingDeadLetterSink();
        KafkaStreamMessageProcessor processor = BuildProcessor(sink, exhausted, maxAttempts);

        // Act
        processor.ProcessAsync(envelope, CancellationToken.None).GetAwaiter().GetResult();

        // Assert
        DeadLetterMessage? captured = sink.Captured.SingleOrDefault();
        if (captured is null)
        {
            return false.ToProperty().Label($"expected exactly one dead-letter, got {sink.Captured.Count}");
        }

        bool idOk = captured.Envelope.MessageId == envelope.MessageId;
        bool correlationOk = string.Equals(captured.CorrelationId, correlationId, StringComparison.Ordinal)
            && string.Equals(captured.Envelope.CorrelationId, correlationId, StringComparison.Ordinal);
        bool payloadOk = captured.Envelope.Payload.Span.SequenceEqual(payload);
        bool attemptsOk = captured.AttemptCount == expectedAttempts;

        return (idOk && correlationOk && payloadOk && attemptsOk)
            .ToProperty()
            .Label($"exhausted={exhausted} idOk={idOk} correlationOk={correlationOk} payloadOk={payloadOk} " +
                   $"attempts expected={expectedAttempts} actual={captured.AttemptCount}");
    }

    private static KafkaStreamMessageProcessor BuildProcessor(
        CapturingDeadLetterSink sink,
        bool exhausted,
        int maxAttempts)
    {
        var observability = new ObservabilityContext(
            new NoOpMessagingLog(),
            new NoOpMessagingMetrics(),
            new MessagingTraceSource(tracingEnabled: false),
            new PayloadRedactor());

        // Exhausted: transient failure on every attempt up to the bound. Otherwise non-transient: one attempt.
        var retryPolicy = new RetryPolicyConfig { MaxAttempts = exhausted ? maxAttempts : 1 };

        var router = new DeadLetterRouter(sink, new NoOpDeadLetterMetrics(), new NoOpDeadLetterLog());

        var retryExecutor = new RetryExecutor(
            new FailureClassifier(),
            new BackoffCalculator(static (min, _) => min),
            router,
            delay: static (_, _) => Task.CompletedTask);

        IHandlerInvoker invoker = exhausted
            ? new ThrowingHandlerInvoker(new TimeoutException("transient"))
            : new ThrowingHandlerInvoker(new UnknownContractException("x", "unknown contract"));

        var pipeline = new ConsumePipeline(new ConsumerHostStateMachine(), new AlwaysBindContractBinder(), invoker);
        pipeline.StartAsync(CancellationToken.None).GetAwaiter().GetResult();

        return new KafkaStreamMessageProcessor(observability, retryExecutor, pipeline, router, retryPolicy);
    }

    private sealed class AlwaysBindContractBinder : IContractBinder
    {
        public bool IsKnown(string messageType) => true;

        public object Bind(MessageEnvelope envelope) => new object();
    }

    private sealed class ThrowingHandlerInvoker : IHandlerInvoker
    {
        private readonly Exception _failure;

        public ThrowingHandlerInvoker(Exception failure) => _failure = failure;

        public Task InvokeAsync(
            MessageEnvelope envelope,
            object boundMessage,
            ConsumeContext context,
            CancellationToken cancellationToken) => throw _failure;
    }

    private sealed class CapturingDeadLetterSink : IDeadLetterSink
    {
        public List<DeadLetterMessage> Captured { get; } = new();

        public Task SendAsync(DeadLetterMessage message, CancellationToken cancellationToken)
        {
            Captured.Add(message);
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
