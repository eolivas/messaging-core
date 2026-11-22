using Demo.Api.Http;
using Demo.Application.Common;
using Demo.Application.Cqrs;
using Demo.Application.Sessions;
using Demo.Application.Sessions.Commands;
using Demo.Application.Sessions.Queries;

namespace Demo.Api.Endpoints;

public static class SessionEndpoints
{
    public static RouteGroupBuilder MapSessionEndpoints(this RouteGroupBuilder api)
    {
        var group = api.MapGroup("/session");

        group.MapPost("/", async (
            StrictJsonBody<StartSessionRequest> body,
            HttpContext http,
            ICommandHandler<StartSessionCommand, SessionStatusDto> handler,
            CancellationToken ct) =>
            (await handler.HandleAsync(new StartSessionCommand(body.Value.ProviderId, body.Value.Overrides), ct))
                .ToResult(http, status => Results.Json(status, statusCode: StatusCodes.Status202Accepted)));

        group.MapDelete("/", async (
            HttpContext http,
            ICommandHandler<StopSessionCommand, SessionStatusDto> handler,
            CancellationToken ct) =>
            (await handler.HandleAsync(new StopSessionCommand(), ct)).ToResult(http, Results.Ok));

        group.MapGet("/", async (
            HttpContext http,
            IQueryHandler<GetSessionStatusQuery, SessionStatusDto> handler,
            CancellationToken ct) =>
            (await handler.HandleAsync(new GetSessionStatusQuery(), ct)).ToResult(http, Results.Ok));

        group.MapPost("/quotes", async (
            HttpContext http,
            ICommandHandler<TriggerQuoteAcceptedCommand, TriggerResultDto> handler,
            CancellationToken ct) =>
            (await handler.HandleAsync(new TriggerQuoteAcceptedCommand(), ct)).ToResult(http, result =>
            {
                http.Response.Headers[CorrelationIdMiddleware.HeaderName] = result.CorrelationId;
                return Results.Ok(result);
            }));

        group.MapPut("/consumers/{consumerId}/failure-mode", async (
            string consumerId,
            StrictJsonBody<SetFailureModeRequest> body,
            HttpContext http,
            ICommandHandler<SetFailureModeCommand, Unit> handler,
            CancellationToken ct) =>
            (await handler.HandleAsync(new SetFailureModeCommand(consumerId, body.Value.Mode), ct))
                .ToResult(http, _ => Results.Ok()));

        group.MapDelete("/flow-steps", async (
            HttpContext http,
            ICommandHandler<ClearFlowCommand, Unit> handler,
            CancellationToken ct) =>
            (await handler.HandleAsync(new ClearFlowCommand(), ct)).ToResult(http, _ => Results.NoContent()));

        group.MapGet("/integration-details", async (
            HttpContext http,
            IQueryHandler<GetIntegrationDetailsQuery, IntegrationDetailsDto> handler,
            CancellationToken ct) =>
            (await handler.HandleAsync(new GetIntegrationDetailsQuery(), ct)).ToResult(http, Results.Ok));

        return api;
    }
}
