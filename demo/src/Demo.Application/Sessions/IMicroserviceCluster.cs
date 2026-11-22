namespace Demo.Application.Sessions;

/// <summary>Port: the three isolated microservice hosts of one Session.</summary>
public interface IMicroserviceCluster
{
    ISalesLeadsGateway SalesLeads { get; }

    Task StartAsync(ClusterStartRequest request, CancellationToken cancellationToken);

    Task StopAsync(CancellationToken cancellationToken);

    Task<ProviderHealth> CheckHealthAsync(CancellationToken cancellationToken);

    void SetFailureMode(ConsumerId consumer, FailureMode mode);

    IntegrationInfo Describe();
}
