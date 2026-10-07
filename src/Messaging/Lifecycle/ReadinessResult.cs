using Messaging.Abstractions;

namespace Messaging.Lifecycle;

/// <summary>
/// The result of a readiness probe (Req 10.3, 10.4, 1.7). Readiness is derived from the active
/// provider's <see cref="IMessagingProvider.CheckTransportAsync"/>: ready when the transport is
/// reachable, not-ready and naming the unavailable dependency when it is not. Every readiness
/// response also reports the active <see cref="MessagingMode"/> so the embedding application can
/// expose the configured mode through its readiness/informational endpoint (Req 1.7).
/// </summary>
/// <param name="IsReady">
/// True when the active provider can reach its underlying transport (Req 10.3); false otherwise (Req 10.4).
/// </param>
/// <param name="ActiveMode">The mode the active provider serves, exposed on every response (Req 1.7).</param>
/// <param name="UnavailableDependency">
/// When <paramref name="IsReady"/> is false, names the dependency reported unavailable by the
/// transport check (Req 10.4); null when ready.
/// </param>
public sealed record ReadinessResult(bool IsReady, MessagingMode ActiveMode, string? UnavailableDependency)
{
    /// <summary>
    /// Builds a ready result for <paramref name="activeMode"/>. A ready result never carries an
    /// unavailable dependency.
    /// </summary>
    /// <param name="activeMode">The active messaging mode to report (Req 1.7).</param>
    public static ReadinessResult Ready(MessagingMode activeMode)
        => new(IsReady: true, activeMode, UnavailableDependency: null);

    /// <summary>
    /// Builds a not-ready result for <paramref name="activeMode"/> naming the unavailable
    /// dependency (Req 10.4).
    /// </summary>
    /// <param name="activeMode">The active messaging mode to report (Req 1.7).</param>
    /// <param name="unavailableDependency">The dependency reported unavailable by the transport check.</param>
    public static ReadinessResult NotReady(MessagingMode activeMode, string? unavailableDependency)
        => new(IsReady: false, activeMode, unavailableDependency);
}
