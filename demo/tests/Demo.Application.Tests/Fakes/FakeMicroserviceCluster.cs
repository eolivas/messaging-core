using Demo.Application.Sessions;

namespace Demo.Application.Tests.Fakes;

internal sealed class FakeMicroserviceCluster : IMicroserviceCluster
{
    public FakeSalesLeadsGateway Gateway { get; } = new();

    public ISalesLeadsGateway SalesLeads => Gateway;

    public Exception? StartFailure { get; set; }

    public Exception? DescribeFailure { get; set; }

    public int StartCalls { get; private set; }

    public int StopCalls { get; private set; }

    public ClusterStartRequest? LastStart { get; private set; }

    public List<(ConsumerId Consumer, FailureMode Mode)> FailureModes { get; } = new();

    public IntegrationInfo Info { get; set; } =
        new("Queue", "RabbitMQ", true, 3, "Exponential");

    public Task StartAsync(ClusterStartRequest request, CancellationToken cancellationToken)
    {
        StartCalls++;
        LastStart = request;
        return StartFailure is null ? Task.CompletedTask : Task.FromException(StartFailure);
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        StopCalls++;
        return Task.CompletedTask;
    }

    public Task<ProviderHealth> CheckHealthAsync(CancellationToken cancellationToken) =>
        Task.FromResult(new ProviderHealth(true, "ok"));

    public void SetFailureMode(ConsumerId consumer, FailureMode mode) =>
        FailureModes.Add((consumer, mode));

    public IntegrationInfo Describe() =>
        DescribeFailure is null ? Info : throw DescribeFailure;
}
