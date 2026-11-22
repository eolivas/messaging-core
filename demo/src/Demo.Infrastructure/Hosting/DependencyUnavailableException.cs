namespace Demo.Infrastructure.Hosting;

/// <summary>Raised when the broker of the selected provider does not accept a TCP connection.</summary>
public sealed class DependencyUnavailableException : Exception
{
    public DependencyUnavailableException(string message)
        : base(message)
    {
    }
}
