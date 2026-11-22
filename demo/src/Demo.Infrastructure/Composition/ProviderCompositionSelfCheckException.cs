namespace Demo.Infrastructure.Composition;

/// <summary>Thrown when a provider composition fails the startup self-check. Names the failed registration.</summary>
public sealed class ProviderCompositionSelfCheckException : Exception
{
    public ProviderCompositionSelfCheckException(string registration, string message, Exception? innerException = null)
        : base(message, innerException)
    {
        Registration = registration;
    }

    /// <summary>The failed registration, as "provider/microservice".</summary>
    public string Registration { get; }
}
