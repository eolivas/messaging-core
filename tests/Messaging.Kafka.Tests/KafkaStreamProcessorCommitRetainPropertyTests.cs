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

// Feature: kafka-provider, Property 3: Commit iff terminal success or routed-and-acknowledged.
// Validates: Requirements 4.2, 4.3, 6.1, 6.2, 6.3, 6.5.
//
// KafkaStreamMessageProcessor.ProcessAsync maps the core's terminal outcome onto the offset-commit
// contract: it RETURNS normally (host commits the offset) iff the outcome is handler success or a
// dead-letter that was routed and acknowledged (non-transient or retries exhausted), and it THROWS
// RetainOffsetException (offset not committed) iff the sink retained the dead-letter unacknowledged.
//
// A REAL processor is wired to REAL core collaborators (RetryExecutor with a no-op delay,
// ConsumePipeline, DeadLetterRouter, ObservabilityContext). No broker is involved; the generated
// category only selects the in-test handler/sink fakes that steer the terminal outcome.
[Trait("Feature", "kafka-provider")]
[Trait("Property", "3")]
public class KafkaStreamProcessorCommitRetainPropertyTests
{
    public enum TerminalOutcomeCategory
    {
        HandlerSuccess,
        NonTransientRoutedAndAcknowledged,
        ExhaustedRoutedAndAcknowledged,
        NonTransientRetainedUnacknowledged,
        ExhaustedRetainedUnacknowledged,
    }

    [Property(MaxTest = 100, QuietOnSuccess = true, Arbitrary = new[] { typeof(KafkaStreamProcessorCommitRetainPropertyTests) })]
    public Property ProcessAsync_AcrossEveryTerminalOutcome_ReturnsUnlessRetainedThenThrowsRetainOffset(
        TerminalOutcomeCategory category,
        NonEmptyString correlationId)
    {
        // Arrange
        MessageEnvelope envelope = KafkaTestSupport.ValidEnvelope(
            correlationId.Get.Trim().Length == 0 ? "corr" : correlationId.Get.Trim());
        KafkaStreamMessageProcessor processor = BuildProcessor(category);

        // Act
        bool threwRetain = InvokeAndObserveRetain(processor, envelope);

        // Assert
        bool expectedRetain = category is TerminalOutcomeCategory.NonTransientRetainedUnacknowledged
            or TerminalOutcomeCategory.ExhaustedRetainedUnacknowledged;

        return (threwRetain == expectedRetain)
            .ToProperty()
            .Label($"category={category} expectedRetain={expectedRetain} actualRetain={threwRetain}");
    }

    public static Arbitrary<TerminalOutcomeCategory> TerminalOutcomeCategoryArbitrary() =>
        Arb.From(Gen.Elements(
            TerminalOutcomeCategory.HandlerSuccess,
            TerminalOutcomeCategory.NonTransientRoutedAndAcknowledged,
            TerminalOutcomeCategory.ExhaustedRoutedAndAcknowledged,
            TerminalOutcomeCategory.NonTransientRetainedUnacknowledged,
            TerminalOutcomeCategory.ExhaustedRetainedUnacknowledged));

    private static bool InvokeAndObserveRetain(KafkaStreamMessageProcessor processor, MessageEnvelope envelope)
    {
        try
        {
            processor.ProcessAsync(envelope, CancellationToken.None).GetAwaiter().GetResult();
            return false;
        }
        catch (RetainOffsetException)
        {
            return true;
        }
    }

    private static KafkaStreamMessageProcessor BuildProcessor(TerminalOutcomeCategory category)
    {
        var observability = new ObservabilityContext(
            new NoOpMessagingLog(),
            new NoOpMessagingMetrics(),
            new MessagingTraceSource(tracingEnabled: false),
            new PayloadRedactor());

        bool exhausted = category is TerminalOutcomeCategory.ExhaustedRoutedAndAcknowledged
            or TerminalOutcomeCategory.ExhaustedRetainedUnacknowledged;
        bool retain = category is TerminalOutcomeCategory.NonTransientRetainedUnacknowledged
            or TerminalOutcomeCategory.ExhaustedRetainedUnacknowledged;

        // Exhausted: a transient failure on every attempt until the (small) bound is reached.
        // Non-transient: a single attempt, straight to the sink.
        var retryPolicy = new RetryPolicyConfig { MaxAttempts = exhausted ? 2 : 1 };

        var router = new DeadLetterRouter(
            new ControllableDeadLetterSink(retain),
            new NoOpDeadLetterMetrics(),
            new NoOpDeadLetterLog());

        var retryExecutor = new RetryExecutor(
            new FailureClassifier(),
            new BackoffCalculator(static (min, _) => min),
            router,
            delay: static (_, _) => Task.CompletedTask);

        IHandlerInvoker invoker = category switch
        {
            TerminalOutcomeCategory.HandlerSuccess => new ScriptedHandlerInvoker(null),
            TerminalOutcomeCategory.ExhaustedRoutedAndAcknowledged
                or TerminalOutcomeCategory.ExhaustedRetainedUnacknowledged
                => new ScriptedHandlerInvoker(new TimeoutException("transient")),
            _ => new ScriptedHandlerInvoker(new UnknownContractException("x", "unknown contract")),
        };

        var pipeline = new ConsumePipeline(new ConsumerHostStateMachine(), new AlwaysBindContractBinder(), invoker);
        pipeline.StartAsync(CancellationToken.None).GetAwaiter().GetResult();

        return new KafkaStreamMessageProcessor(observability, retryExecutor, pipeline, router, retryPolicy);
    }

    private sealed class AlwaysBindContractBinder : IContractBinder
    {
        public bool IsKnown(string messageType) => true;

        public object Bind(MessageEnvelope envelope) => new object();
    }

    private sealed class ScriptedHandlerInvoker : IHandlerInvoker
    {
        private readonly Exception? _failure;

        public ScriptedHandlerInvoker(Exception? failure) => _failure = failure;

        public Task InvokeAsync(
            MessageEnvelope envelope,
            object boundMessage,
            ConsumeContext context,
            CancellationToken cancellationToken) =>
            _failure is null ? Task.CompletedTask : throw _failure;
    }

    private sealed class ControllableDeadLetterSink : IDeadLetterSink
    {
        private readonly bool _retain;

        public ControllableDeadLetterSink(bool retain) => _retain = retain;

        public Task SendAsync(DeadLetterMessage message, CancellationToken cancellationToken) =>
            _retain
                ? throw new InvalidOperationException("Dead-letter sink refused the message.")
                : Task.CompletedTask;
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
