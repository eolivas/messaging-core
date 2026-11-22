using Demo.Application.Flow;
using Demo.Application.Providers;
using Demo.Application.Sessions;
using Microsoft.Extensions.Logging.Abstractions;

namespace Demo.Application.Tests.Fakes;

/// <summary>Wires a coordinator with fake ports so handler tests share one setup.</summary>
internal sealed class SessionWorld : IDisposable
{
    public SessionWorld()
    {
        Status = new SessionStatusProvider(
            Coordinator,
            Cluster,
            Catalog,
            TimeProvider.System,
            NullLogger<SessionStatusProvider>.Instance);
    }

    public SessionCoordinator Coordinator { get; } = new();

    public FakeMicroserviceCluster Cluster { get; } = new();

    public FakeProviderCatalog Catalog { get; } = new();

    public RecordingFlowEventBroadcaster Broadcaster { get; } = new();

    public SessionFlowLog FlowLog { get; } = new();

    public SessionStatusProvider Status { get; }

    public async Task MakeActiveAsync()
    {
        await Coordinator.BeginStartAsync(ProviderId.RabbitMq, CancellationToken.None);
        await Coordinator.CompleteStartAsync(CancellationToken.None);
    }

    public void Dispose() => Coordinator.Dispose();
}
