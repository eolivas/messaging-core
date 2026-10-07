using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Messaging.Abstractions;
using Messaging.Batch.BatchProcessing;
using Messaging.Contracts;

namespace Messaging.Batch.Tests.BatchProcessing;

/// <summary>
/// A batch source that returns pre-seeded windows in order, then empty windows (source drained),
/// recording how many pulls were requested.
/// </summary>
internal sealed class FakeBatchSource : IBatchSource
{
    private readonly Queue<IReadOnlyList<MessageEnvelope>> _windows = new();

    public int PullCount { get; private set; }

    public int LastRequestedWindowSize { get; private set; }

    public void EnqueueWindow(IReadOnlyList<MessageEnvelope> window) => _windows.Enqueue(window);

    public Task<IReadOnlyList<MessageEnvelope>> PullWindowAsync(int windowSize, CancellationToken cancellationToken)
    {
        PullCount++;
        LastRequestedWindowSize = windowSize;
        IReadOnlyList<MessageEnvelope> window = _windows.Count > 0 ? _windows.Dequeue() : Array.Empty<MessageEnvelope>();
        return Task.FromResult(window);
    }
}

/// <summary>A processor that records every envelope it processed in order.</summary>
internal sealed class FakeBatchItemProcessor : IBatchItemProcessor
{
    public List<MessageEnvelope> Processed { get; } = new();

    public Task ProcessAsync(MessageEnvelope envelope, CancellationToken cancellationToken)
    {
        Processed.Add(envelope);
        return Task.CompletedTask;
    }
}

/// <summary>A checkpoint store that records each checkpoint's processed count in order.</summary>
internal sealed class FakeBatchCheckpointStore : IBatchCheckpointStore
{
    public List<int> Checkpoints { get; } = new();

    public Task CheckpointAsync(int processedCount, CancellationToken cancellationToken)
    {
        Checkpoints.Add(processedCount);
        return Task.CompletedTask;
    }
}

/// <summary>A destination writer that records written envelopes or throws a configured failure.</summary>
internal sealed class FakeBatchDestinationWriter : IBatchDestinationWriter
{
    private readonly Exception? _failure;

    public FakeBatchDestinationWriter(Exception? failure = null) => _failure = failure;

    public List<MessageEnvelope> Written { get; } = new();

    public Task WriteAsync(MessageEnvelope envelope, CancellationToken cancellationToken)
    {
        if (_failure is not null)
        {
            throw _failure;
        }

        Written.Add(envelope);
        return Task.CompletedTask;
    }
}

/// <summary>A transport probe that returns a fixed health, for happy/unhappy readiness paths.</summary>
internal sealed class FakeBatchTransportProbe : IBatchTransportProbe
{
    private readonly TransportHealth _health;

    public FakeBatchTransportProbe(TransportHealth health) => _health = health;

    public Task<TransportHealth> ProbeAsync(CancellationToken cancellationToken) => Task.FromResult(_health);
}
