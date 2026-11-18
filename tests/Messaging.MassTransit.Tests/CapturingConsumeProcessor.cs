using System.Collections.Concurrent;
using Messaging.Contracts;
using Messaging.MassTransit.Consuming;
namespace Messaging.MassTransit.Tests;

/// <summary>A consume processor that records every envelope it receives and lets tests await counts.</summary>
internal sealed class CapturingConsumeProcessor : IMassTransitConsumeProcessor
{
    private readonly ConcurrentQueue<MessageEnvelope> _received = new();
    private readonly SemaphoreSlim _signal = new(0);
    private readonly Func<MessageEnvelope, Task>? _behavior;

    /// <param name="behavior">Optional per-message behavior, e.g. throw to simulate a failing handler.</param>
    public CapturingConsumeProcessor(Func<MessageEnvelope, Task>? behavior = null) =>
        _behavior = behavior;

    /// <summary>Every envelope received so far, in arrival order.</summary>
    public IReadOnlyList<MessageEnvelope> Received => _received.ToArray();

    public async Task ProcessAsync(MessageEnvelope envelope, CancellationToken cancellationToken)
    {
        _received.Enqueue(envelope);
        _signal.Release();
        if (_behavior is not null)
        {
            await _behavior(envelope).ConfigureAwait(false);
        }
    }

    /// <summary>Waits until <paramref name="count"/> envelopes arrived; throws on timeout (default 30s).</summary>
    public async Task WaitForCountAsync(int count, TimeSpan? timeout = null)
    {
        var budget = timeout ?? SnsSqsIntegrationHarness.WaitTimeout;
        using var cts = new CancellationTokenSource(budget);
        try
        {
            for (var i = 0; i < count; i++)
            {
                await _signal.WaitAsync(cts.Token).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            throw new TimeoutException(
                $"Expected {count} envelope(s) within {budget.TotalSeconds:0}s; received {_received.Count}.");
        }
    }
}
