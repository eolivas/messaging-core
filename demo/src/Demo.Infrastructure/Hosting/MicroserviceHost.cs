using Demo.Application.Sessions;
using Demo.Contracts;
using Demo.SalesLeads.Application;
using Messaging.Abstractions;
using Messaging.Configuration;
using Messaging.Consuming;
using Messaging.Lifecycle;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Demo.Infrastructure.Hosting;

/// <summary>
/// Wraps one <see cref="IHost"/> built with <see cref="Host.CreateApplicationBuilder(HostApplicationBuilderSettings?)"/>.
/// The parent <see cref="ILoggerFactory"/> is registered as an instance, so every host logs through
/// one structured pipeline and the child container does not dispose it.
/// </summary>
public sealed class MicroserviceHost : IMicroserviceHost
{
    public static readonly TimeSpan ShutdownTimeout = TimeSpan.FromSeconds(10);

    private readonly IHost _host;

    private MicroserviceHost(MicroserviceId id, IHost host, MessagingConfiguration configuration)
    {
        Id = id;
        _host = host;
        Configuration = configuration;
    }

    public MicroserviceId Id { get; }

    public MessagingConfiguration Configuration { get; }

    /// <summary>Builds the host. Nothing starts and no network connection opens until <see cref="StartAsync"/>.</summary>
    public static MicroserviceHost Create(
        MicroserviceId id,
        ILoggerFactory parentLoggerFactory,
        Action<IServiceCollection> configure)
    {
        ArgumentNullException.ThrowIfNull(parentLoggerFactory);
        ArgumentNullException.ThrowIfNull(configure);

        // No appsettings or environment probing: all settings come from the Session request.
        var builder = Host.CreateApplicationBuilder(new HostApplicationBuilderSettings { DisableDefaults = true });
        builder.Services.AddLogging();
        builder.Services.AddSingleton(parentLoggerFactory);
        builder.Services.Configure<HostOptions>(o => o.ShutdownTimeout = ShutdownTimeout);
        configure(builder.Services);

        var host = builder.Build();
        return new MicroserviceHost(id, host, host.Services.GetRequiredService<MessagingConfiguration>());
    }

    /// <summary>
    /// Resolves the services this host's role needs, without starting anything. Used by the startup
    /// self-check. For RabbitMQ and Amazon SQS this builds the bus adapter, which performs no
    /// network I/O (spike: demo/docs/spike-no-network-io.md).
    /// </summary>
    public void VerifyRegistrations()
    {
        var services = _host.Services;
        _ = services.GetRequiredService<IMessagingProvider>();
        if (Id == MicroserviceId.SalesLeads)
        {
            _ = services.GetRequiredService<ISalesLeadsService>();
            return;
        }

        _ = services.GetRequiredService<IMessageConsumerHost>();
        _ = services.GetRequiredService<IHandlerInvoker>();
    }

    public Task StartAsync(CancellationToken cancellationToken) => _host.StartAsync(cancellationToken);

    public Task StopAsync(CancellationToken cancellationToken) => _host.StopAsync(cancellationToken);

    public Task<TransportHealth> CheckTransportAsync(CancellationToken cancellationToken) =>
        _host.Services.GetRequiredService<IMessagingProvider>().CheckTransportAsync(cancellationToken);

    public async Task<ReadinessResult> CheckReadinessAsync(CancellationToken cancellationToken) =>
        await _host.Services.GetRequiredService<MessagingHealthChecks>()
            .ReadinessAsync(cancellationToken).ConfigureAwait(false);

    public async Task<AcceptQuoteOutcome> AcceptQuoteAsync(CancellationToken cancellationToken)
    {
        var service = _host.Services.GetService<ISalesLeadsService>()
            ?? throw new InvalidOperationException($"{Id} does not run the Sales Leads service.");
        var result = await service.AcceptQuoteAsync(cancellationToken).ConfigureAwait(false);
        return new AcceptQuoteOutcome(result.Succeeded, result.CorrelationId, result.LeadId, result.Error);
    }

    public ValueTask DisposeAsync()
    {
        _host.Dispose();
        return ValueTask.CompletedTask;
    }
}
