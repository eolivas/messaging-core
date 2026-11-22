using System.Runtime.CompilerServices;
using Demo.Application.Flow;

namespace Demo.Application.Tests.Fakes;

internal sealed class RecordingFlowEventBroadcaster : IFlowEventBroadcaster
{
    public List<SessionEvent> Published { get; } = new();

    public void Publish(SessionEvent sessionEvent) => Published.Add(sessionEvent);

    public async IAsyncEnumerable<SessionEvent> SubscribeAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await Task.CompletedTask;
        yield break;
    }
}
