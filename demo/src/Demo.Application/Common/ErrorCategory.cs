namespace Demo.Application.Common;

/// <summary>Category of an application error. The Api maps each to an HTTP status.</summary>
public enum ErrorCategory
{
    Validation,
    Conflict,
    NotFound,
    Unavailable,
    Timeout
}
