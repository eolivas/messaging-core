using System.Collections.Concurrent;
using Messaging.Abstractions;
using Messaging.Configuration;
using Messaging.Consuming;
using Messaging.Contracts;
using Messaging.MassTransit.Configuration;
using Messaging.MassTransit.Consuming;
using Messaging.Observability;
using Messaging.Resilience;
namespace Messaging.MassTransit.Tests;

/// <summary>
/// Task 14.10: the same message id delivered twice to one queue runs the handler once while
/// idempotency is enabled (Req 9.4, 9.8, 18.13).
/// </summary>
[Collection(LocalStackCollection.Name)]
[Trait("Category", "Integration")]
public sealed class SnsSqsIdempotentConsumptionIntegrationTests : IAsyncLifetime
{
    private readonly LocalStackContainerFixture _fixture;
    private SnsSqsIntegrationHarness _harness = null!;

    public SnsSqsIdempotentConsumptionIntegrationTests(LocalStackContainerFixture fixture) => _fixture = fixture;

    public async Task InitializeAsync() => _harness = await SnsSqsIntegrationHarness.CreateAsync(_fixture);

    public async Task DisposeAsync() => await _harness.DisposeAsync();

    [Fact]
    public async Task Publish_SameMessageIdTwice_InvokesHandlerOnce()
    {
        using var cts = new CancellationTokenSource(SnsSqsIntegrationHarness.WaitTimeout);
        var handler = new CountingHandlerInvoker();
        var deliveries = new CountingProcessor(BuildProcessor(handler, idempotencyEnabled: true));
        var bus = _harness.CreateBus("idempotent", MassTransitRole.Both, MessagingMode.PublishSubscribe, deliveries);
        await bus.StartBusAsync(cts.Token);
        var envelope = SnsSqsIntegrationHarness.CreateEnvelope("once");

        // Check-then-record is not atomic, so publish the duplicate only after the first delivery
        // has fully completed (handler ran and the key was recorded).
        await bus.PublishAsync(envelope, cts.Token);
        await deliveries.WaitForCountAsync(1);
        Assert.Equal(1, handler.InvocationCount);

        await bus.PublishAsync(envelope, cts.Token);
        await deliveries.WaitForCountAsync(1);
        Assert.Equal(1, handler.InvocationCount);
        await Task.Delay(TimeSpan.FromSeconds(3));
        Assert.Equal(1, handler.InvocationCount);
        Assert.All(deliveries.MessageIds, id => Assert.Equal(envelope.MessageId, id));
    }

    private static CoreConsumeProcessor BuildProcessor(IHandlerInvoker handler, bool idempotencyEnabled)
    {
        var observability = new ObservabilityContext(
            new NoOpMessagingLog(),
            new NoOpMessagingMetrics(),
            new MessagingTraceSource(tracingEnabled: false),
            new PayloadRedactor());
        var router = new DeadLetterRouter(
            new DiscardingDeadLetterSink(), new NoOpDeadLetterMetrics(), new NoOpDeadLetterLog());
        var retry = new RetryExecutor(
            new FailureClassifier(),
            new BackoffCalculator(static (min, _) => min),
            router,
            delay: static (_, _) => Task.CompletedTask);
        var gate = new IdempotencyGate(
            new DefaultIdempotencyKeyProvider(),
            new InMemoryIdempotencyStore(),
            new IdempotencyConfig { Enabled = idempotencyEnabled });
        var pipeline = new ConsumePipeline(
            new ConsumerHostStateMachine(), new AnyContractBinder(), handler, gate);
        pipeline.StartAsync(CancellationToken.None).GetAwaiter().GetResult();
        return new CoreConsumeProcessor(
            observability, retry, pipeline, router, new RetryPolicyConfig { MaxAttempts = 1 });
    }

    /// <summary>Delegates to the real processor and counts deliveries that reach it.</summary>
    private sealed class CountingProcessor : IMassTransitConsumeProcessor
    {
        private readonly IMassTransitConsumeProcessor _inner;
        private readonly ConcurrentQueue<Guid> _ids = new();
        private readonly SemaphoreSlim _signal = new(0);

        public CountingProcessor(IMassTransitConsumeProcessor inner) => _inner = inner;

        public IReadOnlyList<Guid> MessageIds => _ids.ToArray();

        public async Task ProcessAsync(MessageEnvelope envelope, CancellationToken cancellationToken)
        {
            try
            {
                await _inner.ProcessAsync(envelope, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                _ids.Enqueue(envelope.MessageId);
                _signal.Release();
            }
        }

        public async Task WaitForCountAsync(int count)
        {
            using var cts = new CancellationTokenSource(SnsSqsIntegrationHarness.WaitTimeout);
            try
            {
                for (var i = 0; i < count; i++)
                {
                    await _signal.WaitAsync(cts.Token).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException)
            {
                throw new TimeoutException($"Expected {count} deliveries; got {_ids.Count}.");
            }
        }
    }

    private sealed class CountingHandlerInvoker : IHandlerInvoker
    {
        private int _count;

        public int InvocationCount => Volatile.Read(ref _count);

        public Task InvokeAsync(
            MessageEnvelope envelope, object boundMessage, ConsumeContext context, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _count);
            return Task.CompletedTask;
        }
    }

    private sealed class AnyContractBinder : IContractBinder
    {
        public bool IsKnown(string messageType) => true;

        public object Bind(MessageEnvelope envelope) => new object();
    }

    private sealed class DiscardingDeadLetterSink : IDeadLetterSink
    {
        public Task SendAsync(DeadLetterMessage message, CancellationToken cancellationToken) => Task.CompletedTask;
    }

    private sealed class InMemoryIdempotencyStore : IIdempotencyStore
    {
        private readonly ConcurrentDictionary<string, byte> _keys = new(StringComparer.Ordinal);

        public Task<bool> IsProcessedAsync(string idempotencyKey, CancellationToken cancellationToken) =>
            Task.FromResult(_keys.ContainsKey(idempotencyKey));

        public Task RecordProcessedAsync(string idempotencyKey, TimeSpan retention, CancellationToken cancellationToken)
        {
            _keys[idempotencyKey] = 0;
            return Task.CompletedTask;
        }
    }
}
