using Demo.Application.Sessions;

namespace Demo.Api.Endpoints;

/// <summary>
/// Liveness and readiness. With no Session there is no provider, so readiness is 200 with
/// <c>state: idle</c>. With an active Session readiness reflects the library transport check and
/// returns 503 when the transport is unreachable, while liveness stays 200 (Req 11.3, 11.4).
/// </summary>
public static class HealthEndpoints
{
    public static IEndpointRouteBuilder MapHealthEndpoints(this IEndpointRouteBuilder app)
    {
        app.MapGet("/health/live", () => Results.Ok(new { status = "live" }));

        app.MapGet("/health/ready", async (
            SessionCoordinator coordinator,
            IMicroserviceCluster cluster,
            TimeProvider timeProvider,
            ILoggerFactory loggerFactory,
            CancellationToken ct) =>
        {
            var logger = loggerFactory.CreateLogger("Demo.Api.Health");
            var state = coordinator.State;
            if (state != SessionState.Active)
            {
                return Results.Ok(new
                {
                    mode = (string?)null,
                    state = state.ToString().ToLowerInvariant(),
                    ready = state == SessionState.Idle,
                });
            }

            var ready = false;
            using var timeout = new CancellationTokenSource(
                SessionStatusProvider.HealthTimeout, timeProvider);
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, timeout.Token);
            try
            {
                ready = (await cluster.CheckHealthAsync(linked.Token).ConfigureAwait(false)).Ready;
            }
            catch (OperationCanceledException) when (!ct.IsCancellationRequested)
            {
                logger.LogWarning(
                    "Readiness check exceeded {Timeout}.", SessionStatusProvider.HealthTimeout);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogWarning(ex, "Readiness check failed; reporting not ready.");
            }

            string? mode = null;
            try
            {
                mode = cluster.Describe().MessagingMode;
            }
            catch (InvalidOperationException ex)
            {
                logger.LogDebug(ex, "No integration info available while reading readiness.");
            }

            return Results.Json(
                new { mode, state = "active", ready },
                statusCode: ready
                    ? StatusCodes.Status200OK
                    : StatusCodes.Status503ServiceUnavailable);
        });

        return app;
    }
}
