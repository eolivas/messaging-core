using System.Diagnostics;
using System.Reflection;
using MassTransit;
using Messaging.Abstractions;
using Messaging.Configuration;
using Messaging.Consuming;
using Messaging.Contracts;
using Messaging.Lifecycle;
using Messaging.MassTransit.AmazonSqs;
using Messaging.MassTransit.Consuming;
using Messaging.MassTransit.Transport;
using Messaging.Observability;
using Messaging.Resilience;

namespace Messaging.MassTransit.Tests;

/// <summary>
/// Unit tests for the SNS/SQS consume wiring: <see cref="SqsEnvelopeConsumer"/> feeding
/// <see cref="InFlightTrackingConsumeProcessor"/> over the real <see cref="CoreConsumeProcessor"/> and
/// <see cref="SqsDeadLetterSink"/>. Only leaf seams (binder, handler, DLQ sender) are substituted, so
/// no broker is needed. "Returns normally" is the delete signal; an exception means redelivery.
/// </summary>
public class SqsConsumeWiringTests
{
    private const string ContractType = "orders.created.v1";
    private const string DeadLetterQueue = "orders-dlq";

    // ----- Req 6.11, 7.4: unknown or unbindable contract ----------------------------------------

    [Fact]
    public async Task Consume_UnknownContract_DeadLettersWithoutInvokingHandlerAndDeletes()
    {
        var wiring = new Wiring(binder: new ThrowingBinder(new UnknownContractException(ContractType, "unknown")));

        await wiring.Consumer.Consume(Context(Message()));

        Assert.Equal(0, wiring.Handler.InvocationCount);
        var sent = Assert.Single(wiring.Sender.Calls);
        Assert.Equal(DeadLetterQueue, sent.QueueName);
        Assert.Equal("1", sent.Attributes[SqsDeadLetterSink.AttemptCountAttribute]);
    }

    [Fact]
    public async Task Consume_UnbindablePayload_DeadLettersWithoutInvokingHandler()
    {
        var wiring = new Wiring(
            binder: new ThrowingBinder(new MalformedPayloadException("payload does not match contract")),
            maxAttempts: 3);

        await wiring.Consumer.Consume(Context(Message()));

        Assert.Equal(0, wiring.Handler.InvocationCount);
        var sent = Assert.Single(wiring.Sender.Calls);
        Assert.Equal("1", sent.Attributes[SqsDeadLetterSink.AttemptCountAttribute]);
    }

    // ----- Req 6.12, 10.8: sink failure retains --------------------------------------------------

    [Fact]
    public async Task Consume_WhenDeadLetterSendFails_ThrowsRetainForRedelivery()
    {
        var wiring = new Wiring(
            binder: new ThrowingBinder(new UnknownContractException(ContractType, "unknown")),
            sendFailure: new InvalidOperationException("sqs unavailable"));

        await Assert.ThrowsAsync<RetainForRedeliveryException>(
            () => wiring.Consumer.Consume(Context(Message())));

        Assert.Equal(0, wiring.Handler.InvocationCount);
    }

    // ----- Req 1.9: missing correlation id dead-letters ------------------------------------------

    [Fact]
    public async Task Consume_MissingCorrelationId_DeadLettersWithoutInvokingHandler()
    {
        // Envelope validation rejects a blank correlation id with a non-transient failure.
        var wiring = new Wiring(binder: new ValidatingBinder());

        await wiring.Consumer.Consume(Context(Message(correlationId: string.Empty)));

        Assert.Equal(0, wiring.Handler.InvocationCount);
        var sent = Assert.Single(wiring.Sender.Calls);
        Assert.Equal("1", sent.Attributes[SqsDeadLetterSink.AttemptCountAttribute]);
    }

    // ----- Success deletes -----------------------------------------------------------------------

    [Fact]
    public async Task Consume_Success_ReturnsNormallyAndInvokesHandlerOnce()
    {
        var wiring = new Wiring();

        await wiring.Consumer.Consume(Context(Message()));

        Assert.Equal(1, wiring.Handler.InvocationCount);
        Assert.Empty(wiring.Sender.Calls);
        Assert.Equal(0, wiring.Tracker.InFlightCount);
    }

    // ----- Req 12.6: shutdown rejection ----------------------------------------------------------

    [Fact]
    public async Task Consume_WhenShuttingDown_ThrowsDeliveryRejectedWithoutInvokingHandler()
    {
        var wiring = new Wiring();
        wiring.Tracker.StopAccepting();

        await Assert.ThrowsAsync<MessageDeliveryRejectedException>(
            () => wiring.Consumer.Consume(Context(Message())));

        Assert.Equal(0, wiring.Handler.InvocationCount);
        Assert.Equal(0, wiring.Tracker.InFlightCount);
    }

    // ----- Req 15.5: trace link ------------------------------------------------------------------

    [Fact]
    public async Task Consume_WithTraceParentHeader_ReceiveActivityIsChildOfPublishSpan()
    {
        var publishContext = new ActivityContext(
            ActivityTraceId.CreateRandom(), ActivitySpanId.CreateRandom(), ActivityTraceFlags.Recorded);
        string traceParent = $"00-{publishContext.TraceId}-{publishContext.SpanId}-01";
        var captured = new List<Activity>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = s => s.Name == MessagingTraceSource.ActivitySourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStarted = a =>
            {
                if (a.OperationName == SqsEnvelopeConsumer.ReceiveActivityName)
                {
                    captured.Add(a);
                }
            },
        };
        ActivitySource.AddActivityListener(listener);
        var processor = new FakeMassTransitConsumeProcessor();
        var consumer = new SqsEnvelopeConsumer(processor);
        var headers = new Dictionary<string, object> { [MessagingTraceSource.TraceParentHeader] = traceParent };

        await consumer.Consume(Context(Message(), headers));

        Activity receive = Assert.Single(captured);
        Assert.Equal(publishContext.TraceId, receive.TraceId);
        Assert.Equal(publishContext.SpanId, receive.ParentSpanId);
        Assert.Single(processor.Processed);
    }

    // ----- Req 17.5: failure after retries -------------------------------------------------------

    [Fact]
    public async Task Consume_TransientFailureExhaustsRetries_DeadLettersWithMaxAttemptsAndDeletes()
    {
        var handler = new RecordingHandler(new DownstreamUnavailableException("downstream down"));
        var wiring = new Wiring(handler: handler, maxAttempts: 3);

        await wiring.Consumer.Consume(Context(Message()));

        Assert.Equal(3, handler.InvocationCount);
        var sent = Assert.Single(wiring.Sender.Calls);
        Assert.Equal("3", sent.Attributes[SqsDeadLetterSink.AttemptCountAttribute]);
    }

    [Fact]
    public async Task Consume_NonTransientHandlerFailure_IsNotRetried()
    {
        var handler = new RecordingHandler(new MessageValidationException("invalid"));
        var wiring = new Wiring(handler: handler, maxAttempts: 3);

        await wiring.Consumer.Consume(Context(Message()));

        Assert.Equal(1, handler.InvocationCount);
        var sent = Assert.Single(wiring.Sender.Calls);
        Assert.Equal("1", sent.Attributes[SqsDeadLetterSink.AttemptCountAttribute]);
    }

    // ===== Helpers =============================================================================

    private static EnvelopeMessage Message(string correlationId = "corr-1") => new()
    {
        MessageId = Guid.NewGuid(),
        CorrelationId = correlationId,
        MessageType = ContractType,
        CreatedAtUtc = DateTimeOffset.UtcNow,
        Headers = new Dictionary<string, string>(StringComparer.Ordinal),
        Payload = new byte[] { 1, 2, 3 },
    };

    private static ConsumeContext<EnvelopeMessage> Context(
        EnvelopeMessage message, IDictionary<string, object>? headers = null)
    {
        var context = DispatchProxy.Create<ConsumeContext<EnvelopeMessage>, ContextProxy>();
        var proxy = (ContextProxy)(object)context;
        proxy.Message = message;
        proxy.HeaderValues = headers ?? new Dictionary<string, object>();
        return context;
    }

    /// <summary>Minimal ConsumeContext double: exposes Message, Headers.GetAll and CancellationToken.</summary>
    public class ContextProxy : DispatchProxy
    {
        public EnvelopeMessage Message { get; set; } = new();

        public IDictionary<string, object> HeaderValues { get; set; } = new Dictionary<string, object>();

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            switch (targetMethod?.Name)
            {
                case "get_Message":
                    return Message;
                case "get_CancellationToken":
                    return CancellationToken.None;
                case "get_Headers":
                    var headers = DispatchProxy.Create<Headers, HeadersProxy>();
                    ((HeadersProxy)(object)headers).Values = HeaderValues;
                    return headers;
                default:
                    throw new NotSupportedException(targetMethod?.Name);
            }
        }
    }

    /// <summary>Minimal Headers double: only GetAll is supported.</summary>
    public class HeadersProxy : DispatchProxy
    {
        public IDictionary<string, object> Values { get; set; } = new Dictionary<string, object>();

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args) =>
            targetMethod?.Name == "GetAll"
                ? Values.ToList()
                : throw new NotSupportedException(targetMethod?.Name);
    }

    /// <summary>Real consumer over real core processor, in-flight decorator and DLQ sink.</summary>
    private sealed class Wiring
    {
        public Wiring(
            IContractBinder? binder = null,
            RecordingHandler? handler = null,
            Exception? sendFailure = null,
            int maxAttempts = 1)
        {
            Handler = handler ?? new RecordingHandler();
            Sender = new CapturingSender(sendFailure);
            Tracker = new InFlightTracker();

            var observability = new ObservabilityContext(
                new NoOpLog(), new NoOpMetrics(), new MessagingTraceSource(tracingEnabled: false), new PayloadRedactor());
            var sink = new SqsDeadLetterSink(Sender, DeadLetterQueue, fifo: false, FifoGroupSelection.CorrelationId);
            var router = new DeadLetterRouter(sink, new NoOpDeadLetterMetrics(), new NoOpDeadLetterLog());
            var executor = new RetryExecutor(
                new FailureClassifier(),
                new BackoffCalculator(static (min, _) => min),
                router,
                delay: static (_, _) => Task.CompletedTask);
            var pipeline = new ConsumePipeline(
                new ConsumerHostStateMachine(), binder ?? new AlwaysBindBinder(), Handler);
            pipeline.StartAsync(CancellationToken.None).GetAwaiter().GetResult();

            var core = new CoreConsumeProcessor(
                observability, executor, pipeline, router, new RetryPolicyConfig { MaxAttempts = maxAttempts });
            Consumer = new SqsEnvelopeConsumer(new InFlightTrackingConsumeProcessor(core, Tracker));
        }

        public SqsEnvelopeConsumer Consumer { get; }

        public RecordingHandler Handler { get; }

        public CapturingSender Sender { get; }

        public InFlightTracker Tracker { get; }
    }

    private sealed class AlwaysBindBinder : IContractBinder
    {
        public bool IsKnown(string messageType) => true;

        public object Bind(MessageEnvelope envelope) => new object();
    }

    private sealed class ThrowingBinder : IContractBinder
    {
        private readonly Exception _error;

        public ThrowingBinder(Exception error) => _error = error;

        public bool IsKnown(string messageType) => false;

        public object Bind(MessageEnvelope envelope) => throw _error;
    }

    private sealed class ValidatingBinder : IContractBinder
    {
        public bool IsKnown(string messageType) => true;

        public object Bind(MessageEnvelope envelope)
        {
            envelope.Validate();
            return new object();
        }
    }

    private sealed class RecordingHandler : IHandlerInvoker
    {
        private readonly Exception? _failure;

        public RecordingHandler(Exception? failure = null) => _failure = failure;

        public int InvocationCount { get; private set; }

        public Task InvokeAsync(
            MessageEnvelope envelope,
            object boundMessage,
            Messaging.Consuming.ConsumeContext context,
            CancellationToken cancellationToken)
        {
            InvocationCount++;
            return _failure is null ? Task.CompletedTask : Task.FromException(_failure);
        }
    }

    private sealed record SendCall(
        string QueueName, EnvelopeMessage Message, IReadOnlyDictionary<string, string> Attributes);

    private sealed class CapturingSender : IDeadLetterQueueSender
    {
        private readonly Exception? _failure;

        public CapturingSender(Exception? failure) => _failure = failure;

        public List<SendCall> Calls { get; } = new();

        public Task SendToDeadLetterQueueAsync(
            string queueName,
            EnvelopeMessage message,
            IReadOnlyDictionary<string, string> attributes,
            string? messageGroupId,
            string? deduplicationId,
            CancellationToken cancellationToken)
        {
            if (_failure is not null)
            {
                return Task.FromException(_failure);
            }

            Calls.Add(new SendCall(queueName, message, attributes));
            return Task.CompletedTask;
        }
    }

    private sealed class NoOpLog : IMessagingLog
    {
        public void MessageEvent(MessagingEventType eventType, string correlationId, DateTimeOffset timestampUtc)
        {
        }
    }

    private sealed class NoOpMetrics : IMessagingMetrics
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
