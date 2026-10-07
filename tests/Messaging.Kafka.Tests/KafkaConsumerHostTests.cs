using Messaging.Abstractions;
using Messaging.Consuming;
using Messaging.Kafka.Streaming;
using Microsoft.Extensions.Logging.Abstractions;

namespace Messaging.Kafka.Tests;

public class KafkaConsumerHostTests
{
    private static KafkaConsumerHost CreateHost(
        FakeKafkaConsumer consumer,
        FakeStreamMessageProcessor processor) =>
        new(consumer, processor, KafkaTestSupport.ValidSettings(), NullLogger<KafkaConsumerHost>.Instance);

    [Fact]
    public void State_BeforeStart_IsStopped()
    {
        var host = CreateHost(new FakeKafkaConsumer(), new FakeStreamMessageProcessor());

        Assert.Equal(ConsumerHostState.Stopped, host.State);
    }

    [Fact]
    public async Task StartThenStop_DrivesStoppedToRunningToStopped()
    {
        var host = CreateHost(new FakeKafkaConsumer(), new FakeStreamMessageProcessor());

        await host.StartAsync(CancellationToken.None);
        Assert.Equal(ConsumerHostState.Running, host.State);

        await host.StopAsync(CancellationToken.None);
        Assert.Equal(ConsumerHostState.Stopped, host.State);
    }

    [Fact]
    public async Task StartAsync_SubscribesToTheConfiguredTopic()
    {
        var consumer = new FakeKafkaConsumer();
        var host = CreateHost(consumer, new FakeStreamMessageProcessor());

        await host.StartAsync(CancellationToken.None);
        await host.StopAsync(CancellationToken.None);

        Assert.Equal("test-topic", consumer.SubscribedTopic);
    }

    [Fact]
    public async Task StopAsync_ClosesTheConsumerToLeaveTheGroup()
    {
        var consumer = new FakeKafkaConsumer();
        var host = CreateHost(consumer, new FakeStreamMessageProcessor());

        await host.StartAsync(CancellationToken.None);
        await host.StopAsync(CancellationToken.None);

        Assert.True(consumer.Closed);
    }

    [Fact]
    public async Task ConsumeOnce_BeforeStart_Throws()
    {
        var host = CreateHost(new FakeKafkaConsumer(), new FakeStreamMessageProcessor());

        await Assert.ThrowsAsync<InvalidConsumerHostStateException>(
            () => host.ConsumeOnceAsync(CancellationToken.None));
    }

    [Fact]
    public async Task ConsumeOnce_WithAvailableRecord_ProcessesThenCommits()
    {
        var envelope = KafkaTestSupport.ValidEnvelope();
        var record = new KafkaConsumeResult(envelope, Partition: 0, Offset: 42);
        var consumer = new FakeKafkaConsumer(record);
        var processor = new FakeStreamMessageProcessor();
        var host = CreateHost(consumer, processor);

        // Record the global order of process and commit events to prove the ordering (Req 6.7).
        var order = new List<string>();
        processor.OnProcess = _ => order.Add("process");
        consumer.OnCommit = _ => order.Add("commit");

        await host.StartAsync(CancellationToken.None);
        try
        {
            var consumed = await host.ConsumeOnceAsync(CancellationToken.None);

            Assert.True(consumed);
            Assert.Single(processor.Processed);
            Assert.Single(consumer.Committed);
            Assert.Equal(record, consumer.Committed[0]);
            // The commit must happen strictly AFTER the pipeline processed the record (at-least-once).
            Assert.Equal(new[] { "process", "commit" }, order);
        }
        finally
        {
            await host.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task ConsumeOnce_WhenPipelineThrows_DoesNotCommit()
    {
        var envelope = KafkaTestSupport.ValidEnvelope();
        var record = new KafkaConsumeResult(envelope, Partition: 0, Offset: 7);
        var consumer = new FakeKafkaConsumer(record);
        var processor = new FakeStreamMessageProcessor(new InvalidOperationException("pipeline boom"));
        var host = CreateHost(consumer, processor);

        await host.StartAsync(CancellationToken.None);
        try
        {
            await Assert.ThrowsAsync<InvalidOperationException>(
                () => host.ConsumeOnceAsync(CancellationToken.None));

            // Pipeline failed before the commit, so the offset is NOT committed: the record will be
            // reprocessed on restart (at-least-once, Req 6.7).
            Assert.Empty(consumer.Committed);
        }
        finally
        {
            await host.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task ConsumeOnce_WithEmptyPoll_IsNoOp()
    {
        var consumer = new FakeKafkaConsumer();
        var processor = new FakeStreamMessageProcessor();
        var host = CreateHost(consumer, processor);

        await host.StartAsync(CancellationToken.None);
        try
        {
            var consumed = await host.ConsumeOnceAsync(CancellationToken.None);

            Assert.False(consumed);
            Assert.Empty(processor.Processed);
            Assert.Empty(consumer.Committed);
        }
        finally
        {
            await host.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task StartAsync_WhenAlreadyRunning_Throws()
    {
        var host = CreateHost(new FakeKafkaConsumer(), new FakeStreamMessageProcessor());

        await host.StartAsync(CancellationToken.None);
        try
        {
            await Assert.ThrowsAsync<InvalidConsumerHostStateException>(
                () => host.StartAsync(CancellationToken.None));
        }
        finally
        {
            await host.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public void Constructor_WithNullConsumer_Throws()
    {
        Assert.Throws<ArgumentNullException>(() => new KafkaConsumerHost(
            null!,
            new FakeStreamMessageProcessor(),
            KafkaTestSupport.ValidSettings(),
            NullLogger<KafkaConsumerHost>.Instance));
    }
}
