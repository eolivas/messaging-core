namespace Demo.Application.Common;

/// <summary>An expected failure returned by a handler. Never carries stack traces or infrastructure detail.</summary>
public sealed record AppError
{
    private static readonly IReadOnlyList<FieldError> NoFieldErrors = Array.Empty<FieldError>();

    public AppError(ErrorCategory category, string message, IReadOnlyList<FieldError>? fieldErrors = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(message);
        Category = category;
        Message = message;
        FieldErrors = fieldErrors ?? NoFieldErrors;
    }

    public ErrorCategory Category { get; }

    public string Message { get; }

    public IReadOnlyList<FieldError> FieldErrors { get; }

    /// <summary>Correlation id of the event the failed operation concerned, when one exists (Req 11.9).</summary>
    public string? CorrelationId { get; init; }

    public static AppError Validation(string message, IReadOnlyList<FieldError>? fieldErrors = null) =>
        new(ErrorCategory.Validation, message, fieldErrors);

    public static AppError Conflict(string message) => new(ErrorCategory.Conflict, message);

    public static AppError NotFound(string message) => new(ErrorCategory.NotFound, message);

    public static AppError Unavailable(string message) => new(ErrorCategory.Unavailable, message);

    public static AppError Timeout(string message) => new(ErrorCategory.Timeout, message);
}
