using Messaging.Abstractions;

namespace Messaging.Lifecycle;

/// <summary>
/// The result of a liveness probe (Req 10.1). Liveness answers only "is the service process
/// running?" — it is healthy whenever the process is alive and makes no transport call, so it
/// returns within 1 second trivially (Req 10.2). The embedding application maps this onto whatever
/// health-check infrastructure it exposes; the library owns only the decision, not the endpoint.
/// </summary>
/// <param name="IsHealthy">
/// True whenever the service process is running. The library always reports healthy because the
/// probe only executes when the process is alive; a dead process cannot answer at all.
/// </param>
public sealed record LivenessResult(bool IsHealthy)
{
    /// <summary>The shared healthy result; liveness has no per-call state so one instance suffices.</summary>
    public static readonly LivenessResult Healthy = new(true);
}
