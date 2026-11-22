namespace Demo.Application.Providers;

/// <summary>Port: checks whether a broker endpoint accepts connections.</summary>
public interface IDependencyProbe
{
    Task<bool> CanReachAsync(string host, int port, TimeSpan timeout, CancellationToken cancellationToken);
}
