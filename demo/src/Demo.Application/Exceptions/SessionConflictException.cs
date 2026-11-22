namespace Demo.Application.Exceptions;

/// <summary>Thrown when a Session operation conflicts with the current Session state.</summary>
public sealed class SessionConflictException : Exception
{
    public SessionConflictException(string message, string? activeProviderId = null)
        : base(message)
    {
        ActiveProviderId = activeProviderId;
    }

    /// <summary>The provider of the active Session, when one exists.</summary>
    public string? ActiveProviderId { get; }
}
