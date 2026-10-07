using Messaging.Abstractions;

namespace Messaging.Lifecycle;

/// <summary>
/// The Application-layer health-check logic backing the embedding application's liveness and
/// readiness probes (Req 10.1–10.4, 1.7). It owns only the decision; the embedding application
/// wires it into whatever endpoint/health-check infrastructure it exposes. The library deliberately
/// does not stand up its own HTTP endpoints (see the design's lifecycle/health section).
/// </summary>
/// <remarks>
/// <para>
/// <b>Liveness</b> is a pure, non-blocking decision: whenever this code runs the process is alive,
/// so it reports healthy without any transport call, satisfying the 1-second bound trivially
/// (Req 10.1, 10.2).
/// </para>
/// <para>
/// <b>Readiness</b> delegates to <see cref="IMessagingProvider.CheckTransportAsync"/> and maps the
/// returned <see cref="TransportHealth"/> onto a <see cref="ReadinessResult"/>: ready when the
/// transport is reachable (Req 10.3), not-ready and naming the unavailable dependency otherwise
/// (Req 10.4). Every readiness response reports the provider's active <see cref="MessagingMode"/>
/// (Req 1.7). The mapping is intentionally total so a readiness decision exists for every possible
/// transport-health result, which Property 24 exercises directly.
/// </para>
/// <para>
/// SRP: this type only decides liveness/readiness. Reachability is the provider's concern
/// (<see cref="IMessagingProvider.CheckTransportAsync"/>), and exposing endpoints is the host's
/// concern. DIP: it depends on the <see cref="IMessagingProvider"/> port, injected via the
/// constructor.
/// </para>
/// </remarks>
public sealed class MessagingHealthChecks
{
    private readonly IMessagingProvider _provider;

    /// <summary>
    /// Creates the health-check logic over the active messaging provider.
    /// </summary>
    /// <param name="provider">The active provider whose transport reachability backs readiness (Req 10.3, 10.4).</param>
    /// <exception cref="ArgumentNullException">If <paramref name="provider"/> is null.</exception>
    public MessagingHealthChecks(IMessagingProvider provider)
    {
        _provider = provider ?? throw new ArgumentNullException(nameof(provider));
    }

    /// <summary>
    /// Reports liveness. Healthy whenever the process is running; makes no transport call so it
    /// returns within 1 second (Req 10.1, 10.2).
    /// </summary>
    /// <param name="cancellationToken">Token to cancel the probe.</param>
    /// <returns>A healthy <see cref="LivenessResult"/>.</returns>
    public ValueTask<LivenessResult> LivenessAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return ValueTask.FromResult(LivenessResult.Healthy);
    }

    /// <summary>
    /// Reports readiness by checking the active provider's transport reachability and projecting it
    /// onto a <see cref="ReadinessResult"/> that also carries the active mode (Req 10.3, 10.4, 1.7).
    /// </summary>
    /// <param name="cancellationToken">Token to cancel the transport check.</param>
    /// <returns>
    /// A ready result when the transport is reachable; otherwise a not-ready result naming the
    /// unavailable dependency. Either way the active mode is reported.
    /// </returns>
    public async ValueTask<ReadinessResult> ReadinessAsync(CancellationToken cancellationToken)
    {
        var health = await _provider.CheckTransportAsync(cancellationToken).ConfigureAwait(false);
        return MapToReadiness(health, _provider.Mode);
    }

    /// <summary>
    /// Projects a <see cref="TransportHealth"/> onto a <see cref="ReadinessResult"/> for the given
    /// active mode. Ready when and only when the transport is reachable (Req 10.3), otherwise
    /// not-ready naming the unavailable dependency (Req 10.4); the active mode is always carried
    /// (Req 1.7). Exposed as a pure, static mapping so Property 24 can exercise the projection over
    /// arbitrary transport-health results without a provider.
    /// </summary>
    /// <param name="health">The transport reachability result to map.</param>
    /// <param name="activeMode">The active messaging mode to report.</param>
    /// <returns>The readiness decision for <paramref name="health"/>.</returns>
    /// <exception cref="ArgumentNullException">If <paramref name="health"/> is null.</exception>
    public static ReadinessResult MapToReadiness(TransportHealth health, MessagingMode activeMode)
    {
        ArgumentNullException.ThrowIfNull(health);

        return health.Reachable
            ? ReadinessResult.Ready(activeMode)
            : ReadinessResult.NotReady(activeMode, health.UnavailableDependency);
    }
}
