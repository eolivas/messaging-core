using System.Collections.Concurrent;
using Messaging.Abstractions;
using Messaging.Configuration;
using Messaging.Lifecycle;
using Messaging.MassTransit.AmazonSqs;
using Messaging.MassTransit.Configuration;
namespace Messaging.MassTransit.Tests;

/// <summary>Task 14.5: retry exhaustion and dead-lettering (Req 10.1, 10.3, 10.5, 10.7, 10.8, 10.9, 18.8).</summary>
[Collection(LocalStackCollection.Name)]
[Trait("Category", "Integration")]
public sealed class SnsSqsRetryExhaustionIntegrationTests : IAsyncLifetime
{
    private readonly LocalStackContainerFixture _fixture;
    private SnsSqsIntegrationHarness _harness = null!;

    public SnsSqsRetryExhaustionIntegrationTests(LocalStackContainerFixture fixture) => _fixture = fixture;

    public async Task InitializeAsync() => _harness = await SnsSqsIntegrationHarness.CreateAsync(_fixture);

    public async Task DisposeAsync() => await _harness.DisposeAsync();

    [Fact]
    public async Task Consume_TransientFailureEveryAttempt_DeadLettersAfterMaxAttemptsWithoutRedelivery()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(90));
        const int maxAttempts = 3;
        var attempts = new AttemptLog();
        var sender = new DeferredDeadLetterQueueSender();
        var tracker = new InFlightTracker();
        var healthy = new CapturingConsumeProcessor();
        var failing = BuildFailingBus("exhaust-a", maxAttempts, delaySeconds: 1, visibilitySeconds: 30, attempts, sender, tracker);
        var other = _harness.CreateBus("exhaust-b", MassTransitRole.Both, MessagingMode.PublishSubscribe, healthy);
        await failing.StartBusAsync(cts.Token);
        await other.StartBusAsync(cts.Token);

        var envelope = SnsSqsIntegrationHarness.CreateEnvelope("poison");
        await failing.PublishAsync(envelope, cts.Token);

        var deadLetterQueue = DeadLetterQueueName("exhaust-a");
        var deadLettered = await _harness.ReceiveRawAsync(deadLetterQueue, SnsSqsIntegrationHarness.WaitTimeout);
        await healthy.WaitForCountAsync(1);

        Assert.NotNull(deadLettered);
        Assert.Contains(envelope.MessageId.ToString("D"), deadLettered!.Body, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(maxAttempts, attempts.Count);
        var sentAt = Assert.Single(sender.Attempts);
        Assert.InRange(sentAt - attempts.Last, TimeSpan.Zero, TimeSpan.FromSeconds(5));
        Assert.Equal(maxAttempts.ToString(), deadLettered.MessageAttributes[SqsDeadLetterSink.AttemptCountAttribute].StringValue);

        // Deleted from the source queue (not in flight on SQS, not in flight in the process), no redelivery.
        await AssertQueueDrainedAsync(_harness.QueueName("exhaust-a"));
        await Task.Delay(TimeSpan.FromSeconds(3));
        Assert.Equal(maxAttempts, attempts.Count);
        Assert.Equal(0, tracker.InFlightCount);
        SnsSqsIntegrationHarness.AssertDeliveredEqualsPublished(envelope, Assert.Single(healthy.Received));
        Assert.Equal((0, 0), await _harness.GetMessageCountsAsync(DeadLetterQueueName("exhaust-b")));
    }

    [Fact]
    public async Task Consume_RetryTimeExceedsVisibilityTimeout_RenewsLockSoMessageIsNotRedelivered()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(90));
        const int maxAttempts = 3;
        var attempts = new AttemptLog();
        var sender = new DeferredDeadLetterQueueSender();
        // Visibility 2s against about 6s of backoff (two 3s delays): only lock renewal prevents redelivery.
        var bus = BuildFailingBus("renew", maxAttempts, delaySeconds: 3, visibilitySeconds: 2, attempts, sender, new InFlightTracker());
        await bus.StartBusAsync(cts.Token);

        await bus.PublishAsync(SnsSqsIntegrationHarness.CreateEnvelope("slow-poison"), cts.Token);

        var deadLettered = await _harness.ReceiveRawAsync(DeadLetterQueueName("renew"), SnsSqsIntegrationHarness.WaitTimeout);
        await Task.Delay(TimeSpan.FromSeconds(5));

        Assert.NotNull(deadLettered);
        Assert.Equal(maxAttempts, attempts.Count);
        Assert.Single(sender.Attempts);
    }

    [Fact]
    public async Task Consume_DeadLetterSinkFails_RetainsMessageAndDeadLettersOnRedelivery()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(90));
        const int maxAttempts = 2;
        var attempts = new AttemptLog();
        var sender = new DeferredDeadLetterQueueSender(failFirst: 1);
        var bus = BuildFailingBus("sinkfail", maxAttempts, delaySeconds: 1, visibilitySeconds: 2, attempts, sender, new InFlightTracker());
        await bus.StartBusAsync(cts.Token);

        var envelope = SnsSqsIntegrationHarness.CreateEnvelope("sink-fails");
        await bus.PublishAsync(envelope, cts.Token);

        var deadLettered = await _harness.ReceiveRawAsync(DeadLetterQueueName("sinkfail"), SnsSqsIntegrationHarness.WaitTimeout);

        Assert.NotNull(deadLettered);
        Assert.Contains(envelope.MessageId.ToString("D"), deadLettered!.Body, StringComparison.OrdinalIgnoreCase);
        // First delivery: 2 attempts then a failed send (retained). Redelivery: 2 attempts then a good send.
        Assert.Equal(2, sender.Attempts.Count);
        Assert.Equal(2 * maxAttempts, attempts.Count);
        await AssertQueueDrainedAsync(_harness.QueueName("sinkfail"));
    }

    private Messaging.MassTransit.AmazonSqs.AmazonSqsBusAdapter BuildFailingBus(
        string endpoint,
        int maxAttempts,
        int delaySeconds,
        int visibilitySeconds,
        AttemptLog attempts,
        DeferredDeadLetterQueueSender sender,
        InFlightTracker tracker)
    {
        var policy = new RetryPolicyConfig
        {
            MaxAttempts = maxAttempts,
            Backoff = BackoffStrategy.Fixed,
            PerAttemptDelaySeconds = delaySeconds,
        };
        var processor = SnsSqsRetryTestSupport.CreateProcessor(
            policy,
            DeadLetterQueueName(endpoint),
            sender,
            _ =>
            {
                attempts.Record();
                throw new InvalidOperationException("transient boom");
            });
        var bus = _harness.CreateBus(
            endpoint,
            MassTransitRole.Both,
            MessagingMode.PublishSubscribe,
            processor,
            s => s with { VisibilityTimeoutSeconds = visibilitySeconds },
            tracker: tracker);
        sender.Inner = bus;
        return bus;
    }

    private string DeadLetterQueueName(string endpoint) =>
        SnsSqsNamingConvention.DeadLetterQueueName(
            _harness.QueueName(endpoint), AmazonSqsProviderSettings.DefaultDeadLetterQueueSuffix);

    private async Task AssertQueueDrainedAsync(string queueName)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(15);
        (int Visible, int NotVisible) counts;
        do
        {
            counts = await _harness.GetMessageCountsAsync(queueName);
            if (counts == (0, 0))
            {
                return;
            }

            await Task.Delay(500);
        }
        while (DateTime.UtcNow < deadline);

        Assert.Equal((0, 0), counts);
    }

    private sealed class AttemptLog
    {
        private readonly ConcurrentQueue<DateTimeOffset> _times = new();

        public int Count => _times.Count;

        public DateTimeOffset Last => _times.ToArray()[^1];

        public void Record() => _times.Enqueue(DateTimeOffset.UtcNow);
    }
}
