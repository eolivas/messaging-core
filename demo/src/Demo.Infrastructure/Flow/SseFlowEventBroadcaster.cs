using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Demo.Application.Flow;
using Demo.Contracts;

namespace Demo.Infrastructure.Flow;

/// <summary>
/// Fans events out to SSE subscribers through one bounded channel each. Appending to the
/// <see cref="SessionFlowLog"/> and writing to subscribers happen under one lock, so delivery
/// order equals sequence order. A subscriber whose channel is full is completed and removed;
/// its client reconnects and replays from the log.
/// </summary>
public sealed class SseFlowEventBroadcaster : IFlowEventBroadcaster, IFlowStepSink
{
    public const int SubscriberCapacity = 1_000;

    private readonly SessionFlowLog _flowLog;
    private readonly object _gate = new();
    private readonly List<Channel<SessionEvent>> _subscribers = new();

    public SseFlowEventBroadcaster(SessionFlowLog flowLog)
    {
        _flowLog = flowLog ?? throw new ArgumentNullException(nameof(flowLog));
    }

    /// <summary>Number of active subscribers.</summary>
    public int SubscriberCount
    {
        get
        {
            lock (_gate)
            {
                return _subscribers.Count;
            }
        }
    }

    /// <summary>Appends the step to the flow log, then pushes the step and counters events.</summary>
    public void Emit(FlowStep step)
    {
        ArgumentNullException.ThrowIfNull(step);

        lock (_gate)
        {
            var entry = _flowLog.Append(step);
            WriteToSubscribers(entry);
            WriteToSubscribers(_flowLog.GetCounters());
        }
    }

    public void Publish(SessionEvent sessionEvent)
    {
        ArgumentNullException.ThrowIfNull(sessionEvent);

        lock (_gate)
        {
            WriteToSubscribers(sessionEvent);
        }
    }

    /// <summary>
    /// Registers the subscriber on first enumeration and seeds its channel with the retained log
    /// entries under the same lock, so no step is missed or duplicated between replay and live.
    /// </summary>
    public async IAsyncEnumerable<SessionEvent> SubscribeAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var channel = Channel.CreateBounded<SessionEvent>(new BoundedChannelOptions(SubscriberCapacity)
        {
            SingleReader = true,
            SingleWriter = false,
            FullMode = BoundedChannelFullMode.Wait
        });

        lock (_gate)
        {
            foreach (var entry in _flowLog.Replay())
            {
                channel.Writer.TryWrite(entry);
            }

            _subscribers.Add(channel);
        }

        try
        {
            await foreach (var item in channel.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
            {
                yield return item;
            }
        }
        finally
        {
            lock (_gate)
            {
                _subscribers.Remove(channel);
            }

            channel.Writer.TryComplete();
        }
    }

    // Caller must hold _gate.
    private void WriteToSubscribers(SessionEvent sessionEvent)
    {
        for (var i = _subscribers.Count - 1; i >= 0; i--)
        {
            var channel = _subscribers[i];
            if (!channel.Writer.TryWrite(sessionEvent))
            {
                // Full (or already completed): drop the subscriber; the client reconnects and replays.
                channel.Writer.TryComplete();
                _subscribers.RemoveAt(i);
            }
        }
    }
}
