using Demo.Application.Sessions;
using Demo.Contracts;
using Demo.Infrastructure.Composition;

namespace Demo.Infrastructure.Hosting;

/// <summary>Builds one microservice host for a provider composition.</summary>
public interface IMicroserviceHostFactory
{
    /// <param name="service">The microservice to host.</param>
    /// <param name="composition">The active provider composition.</param>
    /// <param name="request">The validated effective settings.</param>
    /// <param name="failureMode">Read on the first delivery of each event (consumers only).</param>
    IMicroserviceHost Create(
        MicroserviceId service,
        IProviderComposition composition,
        ClusterStartRequest request,
        Func<FailureMode> failureMode);
}
