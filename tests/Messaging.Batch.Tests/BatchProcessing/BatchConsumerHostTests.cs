using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Messaging.Abstractions;
using Messaging.Batch.BatchProcessing;
using Messaging.Consuming;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Messaging.Batch.Tests.BatchProcessing;

public class BatchConsumerHostTests
{
    private static BatchConsumerHost CreateHost(
        FakeBatchSource source,
        IBatchItemProcessor processor,
        FakeBatchCheckpointStore checkpointStore,
        int windowSize = 100) =>
        new(
            source,
            processor,
            checkpointStore,
            BatchTestData.Settings(windowSize),
            NullLogger<BatchConsumerHost>.Instance);

    [Fact]
    public void State_BeforeStart_IsStopped()
    {
        var host = CreateHost(new FakeBatchSource(), new FakeBatchItemProcessor(), new FakeBatchCheckpointStore());

        Assert.Equal(ConsumerHostState.Stopped, host.State);
    }

    [Fact]
    public async Task Lifecycle_StartThenStop_FollowsStoppedRunningStoppedSequence()
    {
        var host = CreateHost(new FakeBatchSource(), new FakeBatchItemProcessor(), new FakeBatchCheckpointStore());
        var observed = new List<ConsumerHostState> { host.State };

        await host.StartAsync(CancellationToken.None);
        observed.Add(host.State);

        await host.StopAsync(CancellationToken.None);
        observed.Add(host.State);

        Assert.Equal(
            new[] { ConsumerHostState.Stopped, ConsumerHostState.Running, ConsumerHostState.Stopped },
            observed);
    }

    [Fact]
    public async Task RunPullCycleAsync_WhenNotRunning_Throws()
    {
        var host = CreateHost(new FakeBatchSource(), new FakeBatchItemProcessor(), new FakeBatchCheckpointStore());

        await Assert.ThrowsAsync<InvalidConsumerHostStateException>(
            () => host.RunPullCycleAsync(CancellationToken.None));
    }

    [Fact]
    public async Task RunPullCycleAsync_AfterStop_Throws()
    {
        var host = CreateHost(new FakeBatchSource(), new FakeBatchItemProcessor(), new FakeBatchCheckpointStore());
        await host.StartAsync(CancellationToken.None);
        await host.StopAsync(CancellationToken.None);

        await Assert.ThrowsAsync<InvalidConsumerHostStateException>(
            () => host.RunPullCycleAsync(CancellationToken.None));
    }

    [Fact]
    public async Task RunPullCycleAsync_WithWindow_PullsChunksProcessesThenCheckpoints()
    {
        var source = new FakeBatchSource();
        var window = BatchTestData.Window(5);
        source.EnqueueWindow(window);
        var processor = new FakeBatchItemProcessor();
        var checkpoint = new FakeBatchCheckpointStore();
        var host = CreateHost(source, processor, checkpoint);
        await host.StartAsync(CancellationToken.None);

        var processedCount = await host.RunPullCycleAsync(CancellationToken.None);

        Assert.Equal(5, processedCount);
        Assert.Equal(1, source.PullCount);
        Assert.Equal(window, processor.Processed);
        Assert.Equal(new[] { 5 }, checkpoint.Checkpoints);
    }

    [Fact]
    public async Task RunPullCycleAsync_RequestsWindowSizedPull()
    {
        var source = new FakeBatchSource();
        source.EnqueueWindow(BatchTestData.Window(1));
        var host = CreateHost(source, new FakeBatchItemProcessor(), new FakeBatchCheckpointStore(), windowSize: 250);
        await host.StartAsync(CancellationToken.None);

        await host.RunPullCycleAsync(CancellationToken.None);

        Assert.Equal(250, source.LastRequestedWindowSize);
    }

    [Fact]
    public async Task RunPullCycleAsync_WithEmptyWindow_ProcessesNothingAndWritesNoCheckpoint()
    {
        var source = new FakeBatchSource(); // no windows enqueued -> empty window
        var processor = new FakeBatchItemProcessor();
        var checkpoint = new FakeBatchCheckpointStore();
        var host = CreateHost(source, processor, checkpoint);
        await host.StartAsync(CancellationToken.None);

        var processedCount = await host.RunPullCycleAsync(CancellationToken.None);

        Assert.Equal(0, processedCount);
        Assert.Empty(processor.Processed);
        Assert.Empty(checkpoint.Checkpoints);
    }

    [Fact]
    public async Task RunPullCycleAsync_CheckpointsOnlyAfterEveryItemProcessed()
    {
        var source = new FakeBatchSource();
        source.EnqueueWindow(BatchTestData.Window(3));
        var checkpoint = new FakeBatchCheckpointStore();
        var orderTrackingProcessor = new OrderTrackingProcessor(checkpoint);
        var host = CreateHost(source, orderTrackingProcessor, checkpoint);
        await host.StartAsync(CancellationToken.None);

        await host.RunPullCycleAsync(CancellationToken.None);

        // Every process call happened before the single checkpoint call.
        Assert.Equal(3, orderTrackingProcessor.ProcessedWhenNoCheckpointYet);
        Assert.Equal(new[] { 3 }, checkpoint.Checkpoints);
    }

    /// <summary>
    /// Records how many items were processed while the checkpoint store still had zero checkpoints,
    /// to assert the checkpoint lands only after the whole window is processed.
    /// </summary>
    private sealed class OrderTrackingProcessor : IBatchItemProcessor
    {
        private readonly FakeBatchCheckpointStore _checkpointStore;

        public OrderTrackingProcessor(FakeBatchCheckpointStore checkpointStore) => _checkpointStore = checkpointStore;

        public int ProcessedWhenNoCheckpointYet { get; private set; }

        public Task ProcessAsync(Messaging.Contracts.MessageEnvelope envelope, CancellationToken cancellationToken)
        {
            if (_checkpointStore.Checkpoints.Count == 0)
            {
                ProcessedWhenNoCheckpointYet++;
            }

            return Task.CompletedTask;
        }
    }
}
