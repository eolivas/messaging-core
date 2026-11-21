using System.Collections.Concurrent;
using Messaging.Configuration;
using Messaging.Consuming;
using Messaging.Contracts;
using Messaging.MassTransit.AmazonSqs;
using Messaging.MassTransit.Consuming;
using Messaging.MassTransit.Transport;
using Messaging.Observability;
using Messaging.Resilience;
namespace Messaging.MassTransit.Tests;

/// <summary>
/// Test support for tasks 14.5 and 14.11: builds the production <see cref="CoreConsumeProcessor"/>
/// (real <see cref="RetryExecutor"/>, <see cref="DeadLetterRouter"/>, <see cref="SqsDeadLetterSink"/>)
/// around a test handler. The sink sends through <see cref="DeferredDeadLetterQueueSender"/>, which is
/// pointed at the bus adapter once the adapter exists (the adapter needs the processor first).
/// </summary>
internal static class SnsSqsRetryTestSupport
{
    public static CoreConsumeProcessor CreateProcessor(
        RetryPolicyConfig policy,
        string deadLetterQueueName,
        DeferredDeadLetterQueueSender sender,
        Func<MessageEnvelope, Task> handler)
    {
        var observability = new ObservabilityContext(
            new NoOpMessagingLog(),
            new NoOpMessagingMetrics(),
            new MessagingTraceSource(tracingEnabled: false),
            new PayloadRedactor());
        var sink = new SqsDeadLetterSink(sender, deadLetterQueueName, fifo: false, FifoGroupSelection.CorrelationId);
        var router = new DeadLetterRouter(sink, new NoOpDeadLetterMetrics(), new NoOpDeadLetterLog());
        var executor = new RetryExecutor(new FailureClassifier(), new BackoffCalculator(), router);
        var pipeline = new ConsumePipeline(
            new ConsumerHostStateMachine(), new PassThroughContractBinder(), new DelegateHandlerInvoker(handler));
        pipeline.StartAsync(CancellationToken.None).GetAwaiter().GetResult();
        return new CoreConsumeProcessor(observability, executor, pipeline, router, policy);
    }

    private sealed class PassThroughContractBinder : IContractBinder
    {
        public bool IsKnown(string messageType) => true;

        public object Bind(MessageEnvelope envelope) => new object();
    }

    private sealed class DelegateHandlerInvoker : IHandlerInvoker
    {
        private readonly Func<MessageEnvelope, Task> _handler;

        public DelegateHandlerInvoker(Func<MessageEnvelope, Task> handler) => _handler = handler;

        public Task InvokeAsync(
            MessageEnvelope envelope, object boundMessage, ConsumeContext context, CancellationToken cancellationToken) =>
            _handler(envelope);
    }
}

/// <summary>
/// An <see cref="IDeadLetterQueueSender"/> bound to the adapter after construction. It records the UTC
/// instant of every send attempt and can fail the first N sends to simulate a broken dead-letter sink.
/// </summary>
internal sealed class DeferredDeadLetterQueueSender : IDeadLetterQueueSender
{
    private readonly ConcurrentQueue<DateTimeOffset> _attempts = new();
    private int _failuresRemaining;

    public DeferredDeadLetterQueueSender(int failFirst = 0) => _failuresRemaining = failFirst;

    /// <summary>The adapter that performs the real send; set before the bus starts.</summary>
    public IDeadLetterQueueSender? Inner { get; set; }

    /// <summary>The UTC instant of every send attempt, failed or not, in order.</summary>
    public IReadOnlyList<DateTimeOffset> Attempts => _attempts.ToArray();

    public async Task SendToDeadLetterQueueAsync(
        string queueName,
        EnvelopeMessage message,
        IReadOnlyDictionary<string, string> attributes,
        string? messageGroupId,
        string? deduplicationId,
        CancellationToken cancellationToken)
    {
        _attempts.Enqueue(DateTimeOffset.UtcNow);
        if (Interlocked.Decrement(ref _failuresRemaining) >= 0)
        {
            throw new InvalidOperationException("Simulated dead-letter sink failure.");
        }

        await Inner!
            .SendToDeadLetterQueueAsync(queueName, message, attributes, messageGroupId, deduplicationId, cancellationToken)
            .ConfigureAwait(false);
    }
}
