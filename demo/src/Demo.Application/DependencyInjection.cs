using Demo.Application.Common;
using Demo.Application.Cqrs;
using Demo.Application.Flow;
using Demo.Application.Providers;
using Demo.Application.Providers.Queries;
using Demo.Application.Sessions;
using Demo.Application.Sessions.Commands;
using Demo.Application.Sessions.Queries;
using FluentValidation;
using Microsoft.Extensions.DependencyInjection;

namespace Demo.Application;

/// <summary>Registers every Application handler, validator and session service.</summary>
public static class DependencyInjection
{
    public static IServiceCollection AddDemoApplication(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddSingleton<SessionFlowLog>();
        services.AddSingleton<SessionCoordinator>();
        services.AddSingleton<SessionStatusProvider>();

        services.AddSingleton<IValidator<ListProvidersQuery>, ListProvidersQueryValidator>();
        services.AddSingleton<IValidator<GetProviderDefaultsQuery>, GetProviderDefaultsQueryValidator>();
        services.AddSingleton<IValidator<ValidateProviderSettingsQuery>, ValidateProviderSettingsQueryValidator>();
        services.AddSingleton<IValidator<StartSessionCommand>, StartSessionCommandValidator>();

        services.AddSingleton<IQueryHandler<ListProvidersQuery, IReadOnlyList<ProviderDto>>, ListProvidersQueryHandler>();
        services.AddSingleton<IQueryHandler<GetProviderDefaultsQuery, ProviderDefaultsDto>, GetProviderDefaultsQueryHandler>();
        services.AddSingleton<IQueryHandler<ValidateProviderSettingsQuery, ValidateSettingsResponse>, ValidateProviderSettingsQueryHandler>();
        services.AddSingleton<IQueryHandler<GetSessionStatusQuery, SessionStatusDto>, GetSessionStatusQueryHandler>();
        services.AddSingleton<IQueryHandler<GetIntegrationDetailsQuery, IntegrationDetailsDto>, GetIntegrationDetailsQueryHandler>();

        services.AddSingleton<ICommandHandler<StartSessionCommand, SessionStatusDto>, StartSessionCommandHandler>();
        services.AddSingleton<ICommandHandler<StopSessionCommand, SessionStatusDto>, StopSessionCommandHandler>();
        services.AddSingleton<ICommandHandler<TriggerQuoteAcceptedCommand, TriggerResultDto>, TriggerQuoteAcceptedCommandHandler>();
        services.AddSingleton<ICommandHandler<SetFailureModeCommand, Unit>, SetFailureModeCommandHandler>();
        services.AddSingleton<ICommandHandler<ClearFlowCommand, Unit>, ClearFlowCommandHandler>();

        return services;
    }
}
