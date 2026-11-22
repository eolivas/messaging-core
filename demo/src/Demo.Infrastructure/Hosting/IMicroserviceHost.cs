using Demo.Application.Sessions;
using Demo.Contracts;
using Messaging.Abstractions;
using Messaging.Configuration;
using Messaging.Lifecycle;

namespace Demo.Infrastructure.Hosting;

/// <summary>One isolated microservice host. A seam so the cluster can be tested with fakes.</summary>
public interface IMicroserviceHost : IAsyncDisposable
{
    MicroserviceId Id { get; }

    /// <summary>The messaging configuration this host applied.</summary>
    MessagingConfiguration Configuration { get; }

    Task StartAsync(CancellationToken cancellationToken);

    /// <summary>Stops the host, draining in-flight work within the shutdown timeout.</summary>
    Task StopAsync(CancellationToken cancellationToken);

    Task<TransportHealth> CheckTransportAsync(CancellationToken cancellationToken);

    Task<ReadinessResult> CheckReadinessAsync(CancellationToken cancellationToken);

    /// <summary>The Sales Leads service. Only the publisher host has one.</summary>
    Task<AcceptQuoteOutcome> AcceptQuoteAsync(CancellationToken cancellationToken);
}
