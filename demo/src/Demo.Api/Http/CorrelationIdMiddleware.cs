namespace Demo.Api.Http;

/// <summary>
/// Sets <c>X-Correlation-Id</c> on every response (Req 11.9). A handler may set the header first
/// (the trigger endpoint uses the event's correlation id); the middleware never overwrites it.
/// The request-scoped id is available to problem responses through <see cref="ItemKey"/>.
/// </summary>
public sealed class CorrelationIdMiddleware
{
    public const string HeaderName = "X-Correlation-Id";
    public const string ItemKey = "Demo.CorrelationId";
    private const int MaxIncomingLength = 64;

    private readonly RequestDelegate _next;

    public CorrelationIdMiddleware(RequestDelegate next)
    {
        _next = next ?? throw new ArgumentNullException(nameof(next));
    }

    public Task InvokeAsync(HttpContext context)
    {
        ArgumentNullException.ThrowIfNull(context);

        var id = Accept(context.Request.Headers[HeaderName].ToString()) ?? Guid.NewGuid().ToString("N");
        context.Items[ItemKey] = id;
        context.Response.OnStarting(static state =>
        {
            var (response, value) = ((HttpResponse, string))state;
            if (!response.Headers.ContainsKey(HeaderName))
            {
                response.Headers[HeaderName] = value;
            }

            return Task.CompletedTask;
        }, (context.Response, id));

        return _next(context);
    }

    public static string Current(HttpContext context) =>
        context.Items.TryGetValue(ItemKey, out var value) && value is string id ? id : context.TraceIdentifier;

    // Incoming ids are echoed into headers and logs, so only a conservative character set is accepted.
    private static string? Accept(string incoming)
    {
        if (string.IsNullOrEmpty(incoming) || incoming.Length > MaxIncomingLength)
        {
            return null;
        }

        foreach (var c in incoming)
        {
            if (!char.IsAsciiLetterOrDigit(c) && c != '-' && c != '_')
            {
                return null;
            }
        }

        return incoming;
    }
}
