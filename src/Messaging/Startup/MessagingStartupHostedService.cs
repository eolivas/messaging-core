using Messaging.Abstractions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Messaging.Startup;

/// <summary>
/// The thin hosting adapter that runs the pure <see cref="MessagingStartupValidator"/> as the
/// embedding application starts, so configuration is validated fail-fast and the active provider is
/// resolved <em>before</em> the Messaging Core enters a processing-ready state (Req 1.1, 2.1). It
/// lives in the Application package but depends only on the hosting <em>abstractions</em>
/// (<see cref="IHostedService"/>), never on a concrete host, keeping the inward-dependency rule
/// intact; all decision logic stays in the host-free <see cref="MessagingStartupValidator"/>.
/// </summary>
/// <remarks>
/// <para>
/// <b>Fail-fast at startup.</b> <see cref="StartAsync"/> invokes the validator synchronously as part
/// of host startup. A <see cref="MessagingConfigurationException"/> (invalid config — Req 2.2) or a
/// <see cref="ProviderResolutionException"/> (unresolved/ambiguous mode — Req 1.6) propagates out of
/// host startup, which aborts the application before it begins processing messages (Req 2.1). A
/// generic hosted service running before the consumer host means no message is accepted until this
/// gate passes.
/// </para>
/// <para>
/// <b>Expose the active mode (Req 1.7).</b> On success the resolved active mode is recorded on the
/// shared <see cref="MessagingReadinessState"/> so the readiness/informational endpoint the embedding
/// application exposes reports the mode the instance started in.
/// </para>
/// <para>
/// SRP: this type only sequences validation at startup and records the started mode. It performs no
/// transport work (<see cref="StopAsync"/> is a no-op — graceful shutdown of the consumer host is the
/// <c>GracefulShutdownCoordinator</c>'s concern).
/// </para>
/// </remarks>
public sealed class MessagingStartupHostedService : IHostedService
{
    private readonly MessagingStartupValidator _startupValidator;
    private readonly MessagingReadinessState _readinessState;
    private readonly ILogger<MessagingStartupHostedService> _logger;

    /// <summary>
    /// Creates the startup hosted service.
    /// </summary>
    /// <param name="startupValidator">The pure validate-then-resolve gate (Req 1.5, 2.1).</param>
    /// <param name="readinessState">The shared state the resolved active mode is recorded on (Req 1.7).</param>
    /// <param name="logger">Structured logger for the startup outcome.</param>
    /// <exception cref="ArgumentNullException">If any dependency is null.</exception>
    public MessagingStartupHostedService(
        MessagingStartupValidator startupValidator,
        MessagingReadinessState readinessState,
        ILogger<MessagingStartupHostedService> logger)
    {
        _startupValidator = startupValidator ?? throw new ArgumentNullException(nameof(startupValidator));
        _readinessState = readinessState ?? throw new ArgumentNullException(nameof(readinessState));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <summary>
    /// Validates configuration and resolves the active provider as the host starts, failing fast when
    /// either step fails so the application never reaches a processing-ready state with a bad
    /// configuration (Req 1.1, 1.6, 2.1). Records the active mode for readiness on success (Req 1.7).
    /// </summary>
    /// <param name="cancellationToken">The host startup cancellation token.</param>
    /// <exception cref="MessagingConfigurationException">If configuration validation fails (Req 2.2).</exception>
    /// <exception cref="ProviderResolutionException">If the mode is unresolved or ambiguous (Req 1.6).</exception>
    public Task StartAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        // Fail-fast: a thrown exception aborts host startup before any message is processed (Req 2.1).
        var provider = _startupValidator.Validate();

        // Expose the active mode on readiness/info once startup succeeds (Req 1.7).
        _readinessState.MarkStarted(provider.Mode);

        _logger.LogInformation(
            "Messaging Core startup validation passed; active messaging mode is {MessagingMode}.",
            provider.Mode);

        return Task.CompletedTask;
    }

    /// <summary>
    /// No-op on shutdown: the startup gate holds no resources and draining the consumer host is a
    /// separate lifecycle concern.
    /// </summary>
    /// <param name="cancellationToken">The host shutdown cancellation token.</param>
    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
