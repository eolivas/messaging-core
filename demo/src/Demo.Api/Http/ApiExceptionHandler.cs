using Demo.Application.Common;
using Demo.Application.Exceptions;
using FluentValidation;
using Microsoft.AspNetCore.Diagnostics;
using Microsoft.AspNetCore.Mvc;

namespace Demo.Api.Http;

/// <summary>
/// Maps unhandled exceptions to RFC 7807 responses. 400 lists every invalid field. 500 carries a
/// fixed message and the correlation id, with no stack trace or infrastructure detail (Req 12.3,
/// 12.4); the exception goes to the log with the same id. 413 covers the Kestrel body limit (Req 12.10).
/// </summary>
public sealed class ApiExceptionHandler : IExceptionHandler
{
    private readonly IProblemDetailsService _problemDetails;
    private readonly ILogger<ApiExceptionHandler> _logger;

    public ApiExceptionHandler(IProblemDetailsService problemDetails, ILogger<ApiExceptionHandler> logger)
    {
        _problemDetails = problemDetails ?? throw new ArgumentNullException(nameof(problemDetails));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async ValueTask<bool> TryHandleAsync(
        HttpContext httpContext, Exception exception, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(httpContext);
        ArgumentNullException.ThrowIfNull(exception);

        if (exception is OperationCanceledException && httpContext.RequestAborted.IsCancellationRequested)
        {
            // The client went away; there is nobody to answer.
            httpContext.Response.StatusCode = 499;
            return true;
        }

        var problem = exception switch
        {
            InvalidRequestException e => Validation(e.FieldErrors),
            ValidationException e => Validation(e.Errors
                .GroupBy(x => x.PropertyName, StringComparer.Ordinal)
                .Select(g => new FieldError(g.Key, g.Select(x => x.ErrorMessage).ToList()))
                .ToList()),
            BadHttpRequestException { StatusCode: StatusCodes.Status413PayloadTooLarge } =>
                Problem(StatusCodes.Status413PayloadTooLarge, "Request body too large", "The request body exceeds 1 MiB."),
            BadHttpRequestException => Problem(StatusCodes.Status400BadRequest, "Bad request", "The request is malformed."),
            SessionConflictException e => Problem(StatusCodes.Status409Conflict, "Session conflict", e.Message),
            _ => null,
        };

        if (problem is null)
        {
            _logger.LogError(
                exception,
                "Unhandled exception for correlation id {CorrelationId}",
                CorrelationIdMiddleware.Current(httpContext));
            problem = Problem(
                StatusCodes.Status500InternalServerError,
                "Unexpected error",
                "An unexpected error occurred. Quote the correlation id when reporting it.");
        }

        httpContext.Response.StatusCode = problem.Status ?? StatusCodes.Status500InternalServerError;
        return await _problemDetails.TryWriteAsync(new ProblemDetailsContext
        {
            HttpContext = httpContext,
            ProblemDetails = problem,
        }).ConfigureAwait(false);
    }

    private static ProblemDetails Problem(int status, string title, string detail) =>
        new() { Status = status, Title = title, Detail = detail };

    private static ProblemDetails Validation(IReadOnlyList<FieldError> errors)
    {
        var problem = Problem(StatusCodes.Status400BadRequest, "Validation failed", "One or more fields are invalid.");
        problem.Extensions["errors"] = errors.ToDictionary(e => e.Field, e => e.Messages, StringComparer.Ordinal);
        return problem;
    }
}
