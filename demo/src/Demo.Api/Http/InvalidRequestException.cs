using Demo.Application.Common;

namespace Demo.Api.Http;

/// <summary>A request body failed binding. Mapped to a 400 Problem Details listing every field.</summary>
public sealed class InvalidRequestException : Exception
{
    public InvalidRequestException(IReadOnlyList<FieldError> fieldErrors)
        : base("The request is invalid.")
    {
        FieldErrors = fieldErrors ?? throw new ArgumentNullException(nameof(fieldErrors));
    }

    public IReadOnlyList<FieldError> FieldErrors { get; }
}
