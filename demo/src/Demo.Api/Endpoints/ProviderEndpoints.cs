using Demo.Api.Http;
using Demo.Application.Cqrs;
using Demo.Application.Providers;
using Demo.Application.Providers.Queries;

namespace Demo.Api.Endpoints;

public static class ProviderEndpoints
{
    public static RouteGroupBuilder MapProviderEndpoints(this RouteGroupBuilder api)
    {
        var group = api.MapGroup("/providers");

        group.MapGet("/", async (
            HttpContext http,
            IQueryHandler<ListProvidersQuery, IReadOnlyList<ProviderDto>> handler,
            CancellationToken ct) =>
            (await handler.HandleAsync(new ListProvidersQuery(), ct)).ToResult(http, Results.Ok));

        group.MapGet("/{id}/defaults", async (
            string id,
            HttpContext http,
            IQueryHandler<GetProviderDefaultsQuery, ProviderDefaultsDto> handler,
            CancellationToken ct) =>
            (await handler.HandleAsync(new GetProviderDefaultsQuery(id), ct)).ToResult(http, Results.Ok));

        group.MapPost("/{id}/settings/validate", async (
            string id,
            StrictJsonBody<ValidateSettingsRequest> body,
            HttpContext http,
            IQueryHandler<ValidateProviderSettingsQuery, ValidateSettingsResponse> handler,
            CancellationToken ct) =>
            (await handler.HandleAsync(new ValidateProviderSettingsQuery(id, body.Value.Overrides), ct))
                .ToResult(http, Results.Ok));

        return api;
    }
}
