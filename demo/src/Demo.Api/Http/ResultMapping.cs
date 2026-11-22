using Demo.Application.Common;

namespace Demo.Api.Http;

/// <summary>Maps handler <see cref="Result{T}"/> values to HTTP responses (design endpoint table).</summary>
public static class ResultMapping
{
    public static int StatusFor(ErrorCategory category) => category switch
    {
        ErrorCategory.Validation => StatusCodes.Status400BadRequest,
        ErrorCategory.Conflict => StatusCodes.Status409Conflict,
        ErrorCategory.NotFound => StatusCodes.Status404NotFound,
        ErrorCategory.Unavailable => StatusCodes.Status503ServiceUnavailable,
        ErrorCategory.Timeout => StatusCodes.Status504GatewayTimeout,
        _ => StatusCodes.Status500InternalServerError,
    };

    public static IResult ToProblem(HttpContext context, AppError error)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(error);

        // The trigger endpoint answers errors with the event's id too (Req 11.9).
        if (error.CorrelationId is not null)
        {
            context.Response.Headers[CorrelationIdMiddleware.HeaderName] = error.CorrelationId;
        }

        IDictionary<string, object?>? extensions = null;
        if (error.FieldErrors.Count > 0)
        {
            extensions = new Dictionary<string, object?>
            {
                ["errors"] = error.FieldErrors.ToDictionary(e => e.Field, e => e.Messages, StringComparer.Ordinal),
            };
        }

        return Results.Problem(
            statusCode: StatusFor(error.Category),
            title: error.Category.ToString(),
            detail: error.Message,
            extensions: extensions);
    }

    public static IResult ToResult<T>(
        this Result<T> result, HttpContext context, Func<T, IResult> onSuccess)
    {
        ArgumentNullException.ThrowIfNull(result);
        ArgumentNullException.ThrowIfNull(onSuccess);
        return result.IsSuccess ? onSuccess(result.Value) : ToProblem(context, result.Error);
    }
}
