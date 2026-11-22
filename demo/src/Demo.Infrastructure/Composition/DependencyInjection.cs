using Demo.Application.Flow;
using Demo.Application.Providers;
using Demo.Application.Sessions;
using Demo.Contracts;
using Demo.Infrastructure.Flow;
using Demo.Infrastructure.Hosting;
using Microsoft.Extensions.DependencyInjection;

namespace Demo.Infrastructure.Composition;

/// <summary>Registers the Infrastructure services of the BFF.</summary>
public static class DependencyInjection
{
    public static IServiceCollection AddDemoInfrastructure(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddSingleton(TimeProvider.System);

        services.AddSingleton<IProviderComposition, RabbitMqComposition>();
        services.AddSingleton<IProviderComposition, AmazonSnsSqsComposition>();
        services.AddSingleton<IProviderComposition, KafkaComposition>();
        services.AddSingleton<IProviderCatalog>(sp =>
            new ProviderCatalog(sp.GetServices<IProviderComposition>()));

        services.AddSingleton<SseFlowEventBroadcaster>(sp =>
            new SseFlowEventBroadcaster(sp.GetRequiredService<SessionFlowLog>()));
        services.AddSingleton<IFlowEventBroadcaster>(sp => sp.GetRequiredService<SseFlowEventBroadcaster>());
        services.AddSingleton<IFlowStepSink>(sp => sp.GetRequiredService<SseFlowEventBroadcaster>());

        services.AddSingleton<DependencyPreflight>();
        services.AddSingleton<IDependencyProbe>(sp => sp.GetRequiredService<DependencyPreflight>());
        services.AddSingleton<IMicroserviceHostFactory, MicroserviceHostFactory>();
        services.AddSingleton<IMicroserviceCluster, MicroserviceCluster>();
        services.AddSingleton<ProviderCompositionSelfCheck>();

        return services;
    }
}
