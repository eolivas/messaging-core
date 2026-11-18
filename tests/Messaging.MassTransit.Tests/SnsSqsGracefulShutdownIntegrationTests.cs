using System.Collections.Concurrent;
using Messaging.Abstractions;
using Messaging.Contracts;
using Messaging.MassTransit.Configuration;
using Messaging.MassTransit.Consuming;
namespace Messaging.MassTransit.Tests;

/// <summary>
/// Task 14.11: stopping with in-flight messages deletes completed messages and leaves unfinished
/// messages on the queue for redelivery after restart (Req 12.3, 12.4, 18.14).
/// </summary>
[Collection(LocalStackCollection.Name)]
[Trait("Category", "Integration")]
public sealed class SnsSqsGracefulShutdownIntegrationTests : IAsyncLifetime
{
    private const string Endpoint = "shutdown";
    private readonly LocalStackContainerFixture _fixture;
    private SnsSqsIntegrationHarness _harness = null!;

    public SnsSqsGracefulShutdownIntegrationTests(LocalStackContainerFixture fixture) => _fixture = fixture;

    public async Task InitializeAsync() => _harness = await SnsSqsIntegrationHarness.CreateAsync(_fixture);

    public async Task DisposeAsync() => await _harness.DisposeAsync();

    [Fact]
    public async Task Stop_WithInFlightMessage_DeletesCompletedAndRedeliversUnfinishedAfterRestart()
    {
        using var cts = new CancellationTokenSource(SnsSqsIntegrationHarness.WaitTimeout);
        var completed = SnsSqsIntegrationHarness.CreateEnvelope("completed");
        var unfinished = SnsSqsIntegrationHarness.CreateEnvelope("unfinished");
        var first = new FirstRunProcessor(unfinished.MessageId);
        var bus1 = _harness.CreateBus(
            Endpoint,
            MassTransitRole.Both,
            MessagingMode.PublishSubscribe,
            first,
            s => s with { VisibilityTimeoutSeconds = 5 });
        await bus1.StartBusAsync(cts.Token);

        await bus1.PublishAsync(completed, cts.Token);
        await first.WaitForCompletedAsync(completed.MessageId);
        await bus1.PublishAsync(unfinished, cts.Token);
        await first.WaitForBlockedAsync();
        await WaitForQueueAsync(visible: 0, notVisible: 1);

        await bus1.StopBusAsync(CancellationToken.None);
        Assert.NotNull(bus1.LastShutdownResult);

        var second = new RecordingProcessor();
        var bus2 = _harness.CreateBus(
            Endpoint,
            MassTransitRole.Both,
            MessagingMode.PublishSubscribe,
            second,
            s => s with { VisibilityTimeoutSeconds = 5 });
        await bus2.StartBusAsync(cts.Token);

        await second.WaitForAsync(unfinished.MessageId);
        await Task.Delay(TimeSpan.FromSeconds(3));
        Assert.DoesNotContain(completed.MessageId, second.Ids);
    }

    private async Task WaitForQueueAsync(int visible, int notVisible)
    {
        var queue = _harness.QueueName(Endpoint);
        var deadline = DateTime.UtcNow + SnsSqsIntegrationHarness.WaitTimeout;
        (int Visible, int NotVisible) counts = default;
        while (DateTime.UtcNow < deadline)
        {
            counts = await _harness.GetMessageCountsAsync(queue);
            if (counts == (visible, notVisible))
            {
                return;
            }

            await Task.Delay(250);
        }

        throw new TimeoutException($"Queue counts were {counts}; expected ({visible}, {notVisible}).");
    }

    /// <summary>Completes every message except the target, which blocks until cancelled.</summary>
    private sealed class FirstRunProcessor : IMassTransitConsumeProcessor
    {
        private readonly Guid _blockedId;
        private readonly ConcurrentDictionary<Guid, TaskCompletionSource> _done = new();
        private readonly TaskCompletionSource _blocked = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public FirstRunProcessor(Guid blockedId) => _blockedId = blockedId;

        public async Task ProcessAsync(MessageEnvelope envelope, CancellationToken cancellationToken)
        {
            if (envelope.MessageId == _blockedId)
            {
                _blocked.TrySetResult();
                await Task.Delay(Timeout.Infinite, cancellationToken).ConfigureAwait(false);
                return;
            }

            Signal(envelope.MessageId).TrySetResult();
        }

        public Task WaitForCompletedAsync(Guid id) => WaitAsync(Signal(id).Task);

        public Task WaitForBlockedAsync() => WaitAsync(_blocked.Task);

        private TaskCompletionSource Signal(Guid id) =>
            _done.GetOrAdd(id, _ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));

        private static Task WaitAsync(Task task) => task.WaitAsync(SnsSqsIntegrationHarness.WaitTimeout);
    }

    private sealed class RecordingProcessor : IMassTransitConsumeProcessor
    {
        private readonly ConcurrentQueue<Guid> _ids = new();

        public IReadOnlyCollection<Guid> Ids => _ids.ToArray();

        public Task ProcessAsync(MessageEnvelope envelope, CancellationToken cancellationToken)
        {
            _ids.Enqueue(envelope.MessageId);
            return Task.CompletedTask;
        }

        public async Task WaitForAsync(Guid id)
        {
            var deadline = DateTime.UtcNow + SnsSqsIntegrationHarness.WaitTimeout;
            while (!_ids.Contains(id))
            {
                if (DateTime.UtcNow >= deadline)
                {
                    throw new TimeoutException($"Message {id} was not redelivered.");
                }

                await Task.Delay(100);
            }
        }
    }
}
