using System.Text.Json;
using System.Text.Json.Serialization;
using Demo.Application.Flow;
using Demo.Application.Sessions;
using Microsoft.AspNetCore.Http.Features;

namespace Demo.Api.Endpoints;

/// <summary>
/// <c>GET /api/v1/events</c>: Server-Sent Events written by hand (net8 has no SSE helper).
/// Sends a session snapshot, a counters snapshot, the replay of retained steps, then live
/// events, plus a heartbeat every 500 ms (design D2; Req 9.5, 9.10, 9.11).
/// </summary>
public static class EventStreamEndpoints
{
    public static readonly TimeSpan HeartbeatInterval = TimeSpan.FromMilliseconds(500);

    private static readonly JsonSerializerOptions Json = CreateJsonOptions();

    public static RouteGroupBuilder MapEventStreamEndpoints(this RouteGroupBuilder api)
    {
        api.MapGet("/events", StreamAsync);
        return api;
    }

    private static async Task StreamAsync(
        HttpContext http,
        SessionStatusProvider status,
        SessionFlowLog flowLog,
        IFlowEventBroadcaster broadcaster,
        ILoggerFactory loggerFactory)
    {
        var logger = loggerFactory.CreateLogger("Demo.Api.EventStream");

        http.Response.StatusCode = StatusCodes.Status200OK;
        http.Response.ContentType = "text/event-stream";
        http.Response.Headers.CacheControl = "no-cache";
        http.Response.Headers["X-Accel-Buffering"] = "no";
        http.Features.Get<IHttpResponseBodyFeature>()?.DisableBuffering();

        using var linked = CancellationTokenSource.CreateLinkedTokenSource(http.RequestAborted);
        var token = linked.Token;
        using var timer = new PeriodicTimer(HeartbeatInterval);
        await using var stream = broadcaster
            .SubscribeAsync(token)
            .GetAsyncEnumerator(token);
        Task<bool>? next = null;

        try
        {
            var snapshot = await status.GetAsync(token).ConfigureAwait(false);
            await WriteAsync(http, new SessionChangedEvent(snapshot), token)
                .ConfigureAwait(false);
            await WriteAsync(http, flowLog.GetCounters(), token).ConfigureAwait(false);

            next = stream.MoveNextAsync().AsTask();
            var tick = timer.WaitForNextTickAsync(token).AsTask();

            while (!token.IsCancellationRequested)
            {
                // Check the timer first so a busy stream cannot starve the heartbeat.
                if (tick.IsCompleted)
                {
                    if (!await tick.ConfigureAwait(false))
                    {
                        break;
                    }

                    await WriteAsync(http, new HeartbeatEvent(), token).ConfigureAwait(false);
                    tick = timer.WaitForNextTickAsync(token).AsTask();
                    continue;
                }

                var done = await Task.WhenAny(next, tick).ConfigureAwait(false);
                if (done != next)
                {
                    continue;
                }

                if (!await next.ConfigureAwait(false))
                {
                    // Subscriber completed by the broadcaster (overflow); client reconnects.
                    break;
                }

                await WriteAsync(http, stream.Current, token).ConfigureAwait(false);
                next = stream.MoveNextAsync().AsTask();
            }
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            // Expected: the client disconnected or the host is stopping.
        }
        catch (IOException ex)
        {
            logger.LogDebug(ex, "SSE client connection closed while writing.");
        }
        finally
        {
            await linked.CancelAsync().ConfigureAwait(false);
            await DrainAsync(next, logger).ConfigureAwait(false);
        }
    }

    // The pending read must finish before the async enumerator can be disposed.
    private static async Task DrainAsync(Task<bool>? pending, ILogger logger)
    {
        if (pending is null)
        {
            return;
        }

        try
        {
            await pending.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Expected: the read was cancelled by the caller.
        }
        catch (IOException ex)
        {
            logger.LogDebug(ex, "SSE subscription ended with an I/O error.");
        }
    }

    private static async Task WriteAsync(
        HttpContext http, SessionEvent sessionEvent, CancellationToken token)
    {
        var frame = Format(sessionEvent);
        await http.Response.WriteAsync(frame, token).ConfigureAwait(false);
        await http.Response.Body.FlushAsync(token).ConfigureAwait(false);
    }

    internal static string Format(SessionEvent sessionEvent) => sessionEvent switch
    {
        FlowStepEvent e =>
            $"id: {e.Sequence}\nevent: flowStep\ndata: {Serialize(ToWire(e))}\n\n",
        SessionChangedEvent e =>
            $"event: session\ndata: {Serialize(e.Status)}\n\n",
        CountersEvent e =>
            $"event: counters\ndata: {Serialize(ToWire(e))}\n\n",
        HeartbeatEvent => "event: heartbeat\ndata: {}\n\n",
        _ => throw new InvalidOperationException("Unknown session event type."),
    };

    private static string Serialize<T>(T value) => JsonSerializer.Serialize(value, Json);

    private static object ToWire(CountersEvent e) => new
    {
        published = e.Published,
        customersCreated = e.CustomersCreated,
        emailsSent = e.EmailsSent,
    };

    private static object ToWire(FlowStepEvent e) => new
    {
        sequence = e.Sequence,
        service = e.Step.Service,
        kind = e.Step.Kind,
        correlationId = e.Step.CorrelationId,
        leadId = e.Step.LeadId,
        occurredAtUtc = e.Step.OccurredAtUtc,
        attempt = e.Step.Attempt,
        failureKind = e.Step.FailureKind,
        reason = e.Step.Reason,
        completed = e.Completed,
    };

    private static JsonSerializerOptions CreateJsonOptions()
    {
        var options = new JsonSerializerOptions(JsonSerializerDefaults.Web);
        options.Converters.Add(new JsonStringEnumConverter());
        return options;
    }
}
