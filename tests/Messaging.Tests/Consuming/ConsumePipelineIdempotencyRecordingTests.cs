using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Messaging.Abstractions;
using Messaging.Configuration;
using Messaging.Consuming;
using Messaging.Contracts;
using Messaging.Tests.Fakes;
using Xunit;

namespace Messaging.Tests.Consuming;

/// <summary>
/// Record-after-success behavior of <see cref="ConsumePipeline"/> with <see cref="IdempotencyGate"/>
/// (Req 7.3, 7.5): no containers, in-memory store.
/// </summary>
public class ConsumePipelineIdempotencyRecordingTests
{
    [Fact]
    public async Task DeliverAsync_HandlerSucceeds_RecordsKeyAndSkipsHandlerOnDuplicate()
    {
        var store = new FakeIdempotencyStore();
        var handler = new TestHandlerInvoker();
        var pipeline = await BuildAsync(store, handler, enabled: true);
        var envelope = CreateEnvelope();

        await pipeline.DeliverAsync(envelope, CancellationToken.None);
        await pipeline.DeliverAsync(envelope, CancellationToken.None);

        Assert.Equal(1, handler.Invocations);
        Assert.Single(store.Writes);
    }

    [Fact]
    public async Task DeliverAsync_HandlerThrows_RecordsNothing()
    {
        var store = new FakeIdempotencyStore();
        var handler = new TestHandlerInvoker { Failure = new InvalidOperationException("boom") };
        var pipeline = await BuildAsync(store, handler, enabled: true);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => pipeline.DeliverAsync(CreateEnvelope(), CancellationToken.None));

        Assert.Empty(store.Writes);
    }

    [Fact]
    public async Task DeliverAsync_RecordFails_PropagatesException()
    {
        var store = new ThrowingStore();
        var handler = new TestHandlerInvoker();
        var pipeline = await BuildAsync(store, handler, enabled: true);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => pipeline.DeliverAsync(CreateEnvelope(), CancellationToken.None));

        Assert.Equal(1, handler.Invocations);
    }

    [Fact]
    public async Task DeliverAsync_IdempotencyDisabled_DoesNotRecord()
    {
        var store = new FakeIdempotencyStore();
        var handler = new TestHandlerInvoker();
        var pipeline = await BuildAsync(store, handler, enabled: false);

        await pipeline.DeliverAsync(CreateEnvelope(), CancellationToken.None);

        Assert.Equal(1, handler.Invocations);
        Assert.Empty(store.Writes);
    }

    private static async Task<ConsumePipeline> BuildAsync(IIdempotencyStore store, IHandlerInvoker handler, bool enabled)
    {
        var gate = new IdempotencyGate(
            new DefaultIdempotencyKeyProvider(), store, new IdempotencyConfig { Enabled = enabled });
        var pipeline = new ConsumePipeline(new ConsumerHostStateMachine(), new AnyBinder(), handler, gate);
        await pipeline.StartAsync(CancellationToken.None);
        return pipeline;
    }

    private static MessageEnvelope CreateEnvelope() => new()
    {
        MessageId = Guid.NewGuid(),
        CorrelationId = "corr-1",
        MessageType = "test.message",
        CreatedAtUtc = DateTimeOffset.UtcNow,
        Headers = new Dictionary<string, string>(),
        Payload = new ReadOnlyMemory<byte>(new byte[] { 1 }),
        ContractVersion = 1,
    };

    private sealed class AnyBinder : IContractBinder
    {
        public bool IsKnown(string messageType) => true;

        public object Bind(MessageEnvelope envelope) => new object();
    }

    private sealed class TestHandlerInvoker : IHandlerInvoker
    {
        public int Invocations { get; private set; }

        public Exception? Failure { get; init; }

        public Task InvokeAsync(
            MessageEnvelope envelope, object boundMessage, ConsumeContext context, CancellationToken cancellationToken)
        {
            Invocations++;
            return Failure is null ? Task.CompletedTask : Task.FromException(Failure);
        }
    }

    private sealed class ThrowingStore : IIdempotencyStore
    {
        public Task<bool> IsProcessedAsync(string idempotencyKey, CancellationToken cancellationToken) =>
            Task.FromResult(false);

        public Task RecordProcessedAsync(string idempotencyKey, TimeSpan retention, CancellationToken cancellationToken) =>
            Task.FromException(new InvalidOperationException("record failed"));
    }
}
