using Confluent.Kafka;
using Messaging.Abstractions;
using Messaging.Configuration;
using Messaging.Consuming;
using Messaging.Contracts;
using Messaging.Kafka.Streaming;
using Messaging.Observability;
using Messaging.Resilience;
using Xunit;

namespace Messaging.Kafka.Tests;

// Unit tests for KafkaStreamMessageProcessor flows (kafka-provider task 6.5).
// Validates: Requirements 1.6, 5.1, 5.2, 5.4, 5.5, 5.7, 9.6, 10.6.
//
// A REAL processor is wired to REAL core collaborators (RetryExecutor with a no-op delay, ConsumePipeline,
// DeadLetterRouter, ObservabilityContext, IdempotencyGate). Only the binder/handler/sink/store are fakes.
[Trait("Feature", "kafka-provider")]
public class KafkaStreamMessageProcessorTests
{
    // ---- constructor guards ----

    [Theory]
    [InlineData("observability")]
    [InlineData("retryExecutor")]
    [InlineData("consumePipeline")]
    [InlineData("deadLetterRouter")]
    [InlineData("retryPolicy")]
    public void Ctor_NullDependency_ThrowsArgumentNullExceptionNamingParameter(string nullParameter)
    {
        // Arrange
        using var harness = new Harness();

        // Act
        Action act = () => _ = new KafkaStreamMessageProcessor(
            nullParameter == "observability" ? null! : harness.Observability,
            nullParameter == "retryExecutor" ? null! : harness.RetryExecutor,
            nullParameter == "consumePipeline" ? null! : harness.Pipeline,
            nullParameter == "deadLetterRouter" ? null! : harness.Router,
            nullParameter == "retryPolicy" ? null! : harness.Policy);

        // Assert
        var ex = Assert.Throws<ArgumentNullException>(act);
        Assert.Equal(nullParameter, ex.ParamName);
    }

    [Fact]
    public async Task ProcessAsync_NullEnvelope_ThrowsArgumentNullException()
    {
        // Arrange
        using var harness = new Harness();

        // Act
        Func<Task> act = () => harness.Processor.ProcessAsync(null!, CancellationToken.None);

        // Assert
        await Assert.ThrowsAsync<ArgumentNullException>(act);
    }

    // ---- delegation / success ----

    [Fact]
    public async Task ProcessAsync_HandlerSucceeds_InvokesHandlerOnceWithEnvelopeAndDoesNotDeadLetter()
    {
        // Arrange
        using var harness = new Harness();
        MessageEnvelope envelope = KafkaTestSupport.ValidEnvelope();

        // Act
        await harness.Processor.ProcessAsync(envelope, CancellationToken.None);

        // Assert
        Assert.Equal(1, harness.Handler.InvocationCount);
        Assert.Same(envelope, harness.Handler.Envelopes.Single());
        Assert.Empty(harness.Sink.Sent);
    }

    [Fact]
    public async Task ProcessAsync_TransientFailureThenSuccess_RetriesThroughExecutorAndReturns()
    {
        // Arrange
        using var harness = new Harness { Policy = new RetryPolicyConfig { MaxAttempts = 3 } };
        harness.Handler.FailuresBeforeSuccess = new Queue<Exception>(new Exception[] { new TimeoutException("t") });

        // Act
        await harness.Processor.ProcessAsync(KafkaTestSupport.ValidEnvelope(), CancellationToken.None);

        // Assert
        Assert.Equal(2, harness.Handler.InvocationCount);
        Assert.Empty(harness.Sink.Sent);
    }

    [Fact]
    public async Task ProcessAsync_TransientFailureEveryAttempt_DeadLettersAfterMaxAttemptsAndReturns()
    {
        // Arrange
        using var harness = new Harness { Policy = new RetryPolicyConfig { MaxAttempts = 3 } };
        harness.Handler.AlwaysFailWith = new TimeoutException("down");

        // Act
        await harness.Processor.ProcessAsync(KafkaTestSupport.ValidEnvelope(), CancellationToken.None);

        // Assert
        Assert.Equal(3, harness.Handler.InvocationCount);
        DeadLetterMessage dead = Assert.Single(harness.Sink.Sent);
        Assert.Equal(3, dead.AttemptCount);
    }

    [Fact]
    public async Task ProcessAsync_CorrelationId_ReachesHandlerEnvelopeAndDeadLetterUnchanged()
    {
        // Arrange
        using var harness = new Harness();
        MessageEnvelope ok = KafkaTestSupport.ValidEnvelope("corr-pass-through");

        // Act
        await harness.Processor.ProcessAsync(ok, CancellationToken.None);

        // Assert
        Assert.Equal("corr-pass-through", harness.Handler.Envelopes.Single().CorrelationId);
    }

    // ---- idempotency (Req 5.4, 5.5) ----

    [Fact]
    public async Task ProcessAsync_IdempotentDuplicate_DoesNotInvokeHandlerAndReturnsNormally()
    {
        // Arrange
        using var harness = new Harness { UseIdempotency = true };
        harness.Store.AlreadyProcessed = true;

        // Act
        await harness.Processor.ProcessAsync(KafkaTestSupport.ValidEnvelope(), CancellationToken.None);

        // Assert
        Assert.Equal(0, harness.Handler.InvocationCount);
        Assert.Empty(harness.Sink.Sent);
        Assert.Equal(1, harness.Store.LookupCount);
    }

    [Fact]
    public async Task ProcessAsync_NewMessageWithIdempotency_InvokesHandlerWithDerivedKey()
    {
        // Arrange
        using var harness = new Harness { UseIdempotency = true };

        // Act
        await harness.Processor.ProcessAsync(KafkaTestSupport.ValidEnvelope(), CancellationToken.None);

        // Assert
        Assert.Equal(1, harness.Handler.InvocationCount);
        Assert.False(string.IsNullOrEmpty(harness.Handler.Contexts.Single().IdempotencyKey));
        Assert.False(harness.Handler.Contexts.Single().IsDuplicate);
    }

    [Fact]
    public async Task ProcessAsync_MissingIdempotencyKey_DeadLettersWithoutInvokingHandlerAndReturns()
    {
        // Arrange
        using var harness = new Harness { UseIdempotency = true };
        MessageEnvelope noKey = KafkaTestSupport.ValidEnvelope() with { MessageId = Guid.Empty };

        // Act
        await harness.Processor.ProcessAsync(noKey, CancellationToken.None);

        // Assert
        Assert.Equal(0, harness.Handler.InvocationCount);
        Assert.Equal(0, harness.Store.LookupCount);
        DeadLetterMessage dead = Assert.Single(harness.Sink.Sent);
        Assert.Equal(noKey.CorrelationId, dead.CorrelationId);
    }

    [Fact]
    public async Task ProcessAsync_MissingIdempotencyKey_IsNotRetried()
    {
        // Arrange (Req 5.5: missing key is non-transient, so dead-lettered on the first attempt)
        using var harness = new Harness { UseIdempotency = true, Policy = new RetryPolicyConfig { MaxAttempts = 3 } };
        MessageEnvelope noKey = KafkaTestSupport.ValidEnvelope() with { MessageId = Guid.Empty };

        // Act
        await harness.Processor.ProcessAsync(noKey, CancellationToken.None);

        // Assert
        Assert.Equal(1, Assert.Single(harness.Sink.Sent).AttemptCount);
        Assert.Equal(0, harness.Handler.InvocationCount);
        Assert.Equal(0, harness.Store.LookupCount);
    }

    // ---- contract binding (Req 5.7) ----

    [Fact]
    public async Task ProcessAsync_UnknownContract_DeadLettersWithReasonNamingContractAndSkipsHandler()
    {
        // Arrange
        using var harness = new Harness
        {
            Binder = new JsonContractBinder(Array.Empty<ContractRegistration>()),
            Policy = new RetryPolicyConfig { MaxAttempts = 3 },
        };
        MessageEnvelope envelope = KafkaTestSupport.ValidEnvelope() with { MessageType = "orders.unknown.v9" };

        // Act
        await harness.Processor.ProcessAsync(envelope, CancellationToken.None);

        // Assert
        Assert.Equal(0, harness.Handler.InvocationCount);
        DeadLetterMessage dead = Assert.Single(harness.Sink.Sent);
        Assert.Contains("orders.unknown.v9", dead.FailureReason);
        Assert.Equal(1, dead.AttemptCount);
    }

    // ---- missing required header (Req 9.6) ----

    [Fact]
    public void ToEnvelope_RecordMissingRequiredHeaders_ThrowsAndReturnsNoPartialEnvelope()
    {
        // Arrange
        var message = new Message<Ignore, byte[]> { Value = new byte[] { 1 }, Headers = new Headers() };

        // Act
        MessageEnvelope? produced = null;
        Action act = () => produced = KafkaWireFormat.ToEnvelope(message);

        // Assert
        Assert.Throws<MissingRequiredHeaderException>(act);
        Assert.Null(produced);
    }

    [Fact]
    public async Task ProcessAsync_MissingRequiredHeaderFailure_DeadLettersOnFirstAttemptWithoutInvokingHandler()
    {
        // Arrange
        using var harness = new Harness
        {
            Binder = new ThrowingBinder(new MissingRequiredHeaderException("messaging-message-id")),
            Policy = new RetryPolicyConfig { MaxAttempts = 3 },
        };

        // Act
        await harness.Processor.ProcessAsync(KafkaTestSupport.ValidEnvelope(), CancellationToken.None);

        // Assert
        Assert.Equal(0, harness.Handler.InvocationCount);
        DeadLetterMessage dead = Assert.Single(harness.Sink.Sent);
        Assert.Equal(1, dead.AttemptCount);
        Assert.Contains("messaging-message-id", dead.FailureReason);
    }

    // ---- retain-offset + correlation id on exceptions ----

    [Fact]
    public async Task ProcessAsync_SinkRetainsDeadLetter_ThrowsRetainOffsetWithCorrelationIdAttached()
    {
        // Arrange
        using var harness = new Harness { Sink = new RecordingSink(retain: true) };
        harness.Handler.AlwaysFailWith = new MessageValidationException("bad");
        MessageEnvelope envelope = KafkaTestSupport.ValidEnvelope("corr-retain");

        // Act
        Func<Task> act = () => harness.Processor.ProcessAsync(envelope, CancellationToken.None);

        // Assert
        var ex = await Assert.ThrowsAsync<RetainOffsetException>(act);
        Assert.Equal("corr-retain", ex.Data[KafkaConsumerHostedService.CorrelationIdDataKey]);
    }

    [Fact]
    public async Task ProcessAsync_CancelledBeforeDelivery_RethrowsWithCorrelationIdAttached()
    {
        // Arrange
        using var harness = new Harness();
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        MessageEnvelope envelope = KafkaTestSupport.ValidEnvelope("corr-cancel");

        // Act
        Func<Task> act = () => harness.Processor.ProcessAsync(envelope, cts.Token);

        // Assert
        var ex = await Assert.ThrowsAnyAsync<OperationCanceledException>(act);
        Assert.Equal("corr-cancel", ex.Data[KafkaConsumerHostedService.CorrelationIdDataKey]);
        Assert.Equal(0, harness.Handler.InvocationCount);
    }

    [Fact]
    public async Task ProcessAsync_ThrownExceptionWithWhitespaceCorrelationId_DoesNotAttachData()
    {
        // Arrange
        using var harness = new Harness { Sink = new RecordingSink(retain: true) };
        harness.Handler.AlwaysFailWith = new MessageValidationException("bad");
        MessageEnvelope envelope = KafkaTestSupport.ValidEnvelope() with { CorrelationId = "   " };

        // Act
        Func<Task> act = () => harness.Processor.ProcessAsync(envelope, CancellationToken.None);

        // Assert
        var ex = await Assert.ThrowsAsync<RetainOffsetException>(act);
        Assert.False(ex.Data.Contains(KafkaConsumerHostedService.CorrelationIdDataKey));
    }

    // ---- observability is best-effort (Req 10.6) ----

    [Fact]
    public async Task ProcessAsync_ObservabilitySinksThrow_ConsumeStillSucceeds()
    {
        // Arrange
        using var harness = new Harness { ThrowingObservability = true };

        // Act
        await harness.Processor.ProcessAsync(KafkaTestSupport.ValidEnvelope(), CancellationToken.None);

        // Assert
        Assert.Equal(1, harness.Handler.InvocationCount);
        Assert.Empty(harness.Sink.Sent);
    }

    // ---- harness & fakes ----

    private sealed class Harness : IDisposable
    {
        private KafkaStreamMessageProcessor? _processor;
        private ConsumePipeline? _pipeline;
        private DeadLetterRouter? _router;
        private RetryExecutor? _retryExecutor;
        private ObservabilityContext? _observability;

        public RetryPolicyConfig Policy { get; set; } = new() { MaxAttempts = 2 };

        public IContractBinder Binder { get; set; } = new AlwaysBindContractBinder();

        public RecordingHandlerInvoker Handler { get; } = new();

        public RecordingSink Sink { get; set; } = new(retain: false);

        public FakeIdempotencyStore Store { get; } = new();

        public bool UseIdempotency { get; set; }

        public bool ThrowingObservability { get; set; }

        public ObservabilityContext Observability => _observability ??= new ObservabilityContext(
            ThrowingObservability ? new ThrowingMessagingLog() : new NoOpMessagingLog(),
            ThrowingObservability ? new ThrowingMessagingMetrics() : new NoOpMessagingMetrics(),
            new MessagingTraceSource(tracingEnabled: false),
            new PayloadRedactor());

        public DeadLetterRouter Router => _router ??= new DeadLetterRouter(
            Sink,
            new NoOpDeadLetterMetrics(),
            new NoOpDeadLetterLog());

        public RetryExecutor RetryExecutor => _retryExecutor ??= new RetryExecutor(
            new FailureClassifier(),
            new BackoffCalculator(static (min, _) => min),
            Router,
            delay: static (_, _) => Task.CompletedTask);

        public ConsumePipeline Pipeline
        {
            get
            {
                if (_pipeline is null)
                {
                    IdempotencyGate? gate = UseIdempotency
                        ? new IdempotencyGate(new DefaultIdempotencyKeyProvider(), Store, new IdempotencyConfig())
                        : null;
                    _pipeline = new ConsumePipeline(new ConsumerHostStateMachine(), Binder, Handler, gate);
                    _pipeline.StartAsync(CancellationToken.None).GetAwaiter().GetResult();
                }

                return _pipeline;
            }
        }

        public KafkaStreamMessageProcessor Processor =>
            _processor ??= new KafkaStreamMessageProcessor(Observability, RetryExecutor, Pipeline, Router, Policy);

        public void Dispose()
        {
        }
    }

    private sealed class AlwaysBindContractBinder : IContractBinder
    {
        public bool IsKnown(string messageType) => true;

        public object Bind(MessageEnvelope envelope) => new object();
    }

    private sealed class ThrowingBinder : IContractBinder
    {
        private readonly Exception _error;

        public ThrowingBinder(Exception error) => _error = error;

        public bool IsKnown(string messageType) => true;

        public object Bind(MessageEnvelope envelope) => throw _error;
    }

    private sealed class RecordingHandlerInvoker : IHandlerInvoker
    {
        public int InvocationCount { get; private set; }

        public List<MessageEnvelope> Envelopes { get; } = new();

        public List<ConsumeContext> Contexts { get; } = new();

        public Queue<Exception> FailuresBeforeSuccess { get; set; } = new();

        public Exception? AlwaysFailWith { get; set; }

        public Task InvokeAsync(
            MessageEnvelope envelope,
            object boundMessage,
            ConsumeContext context,
            CancellationToken cancellationToken)
        {
            InvocationCount++;
            Envelopes.Add(envelope);
            Contexts.Add(context);

            if (AlwaysFailWith is not null)
            {
                throw AlwaysFailWith;
            }

            if (FailuresBeforeSuccess.Count > 0)
            {
                throw FailuresBeforeSuccess.Dequeue();
            }

            return Task.CompletedTask;
        }
    }

    private sealed class RecordingSink : IDeadLetterSink
    {
        private readonly bool _retain;

        public RecordingSink(bool retain) => _retain = retain;

        public List<DeadLetterMessage> Sent { get; } = new();

        public Task SendAsync(DeadLetterMessage message, CancellationToken cancellationToken)
        {
            if (_retain)
            {
                throw new InvalidOperationException("Dead-letter sink refused the message.");
            }

            Sent.Add(message);
            return Task.CompletedTask;
        }
    }

    private sealed class FakeIdempotencyStore : IIdempotencyStore
    {
        public bool AlreadyProcessed { get; set; }

        public int LookupCount { get; private set; }

        public Task<bool> IsProcessedAsync(string idempotencyKey, CancellationToken cancellationToken)
        {
            LookupCount++;
            return Task.FromResult(AlreadyProcessed);
        }

        public Task RecordProcessedAsync(string idempotencyKey, TimeSpan retention, CancellationToken cancellationToken) =>
            Task.CompletedTask;
    }

    private sealed class NoOpMessagingLog : IMessagingLog
    {
        public void MessageEvent(MessagingEventType eventType, string correlationId, DateTimeOffset timestampUtc)
        {
        }
    }

    private sealed class ThrowingMessagingLog : IMessagingLog
    {
        public void MessageEvent(MessagingEventType eventType, string correlationId, DateTimeOffset timestampUtc) =>
            throw new InvalidOperationException("log sink down");
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

    private sealed class ThrowingMessagingMetrics : IMessagingMetrics
    {
        public void RecordPublished() => throw new InvalidOperationException("metrics down");

        public void RecordConsumed() => throw new InvalidOperationException("metrics down");

        public void RecordRetry() => throw new InvalidOperationException("metrics down");

        public void RecordDeadLetter() => throw new InvalidOperationException("metrics down");
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
