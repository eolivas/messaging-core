namespace Messaging.Abstractions;

/// <summary>
/// The reachability of the active provider's underlying transport, used to back the readiness
/// probe (Req 10.3, 10.4). When unreachable, <see cref="UnavailableDependency"/> names the
/// dependency that is down.
/// </summary>
/// <param name="Reachable">Whether the provider can reach its underlying transport.</param>
/// <param name="UnavailableDependency">
/// The name of the unavailable dependency when <paramref name="Reachable"/> is false; otherwise null (Req 10.4).
/// </param>
public sealed record TransportHealth(bool Reachable, string? UnavailableDependency);
