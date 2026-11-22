namespace Demo.Application.Flow;

/// <summary>Port: fans session and flow events out to SSE subscribers.</summary>
public interface IFlowEventBroadcaster
{
    void Publish(SessionEvent sessionEvent);

    IAsyncEnumerable<SessionEvent> SubscribeAsync(CancellationToken cancellationToken);
}
