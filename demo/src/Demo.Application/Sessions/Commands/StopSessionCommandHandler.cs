using Demo.Application.Common;
using Demo.Application.Cqrs;
using Demo.Application.Flow;

namespace Demo.Application.Sessions.Commands;

/// <summary>Drains and stops the three hosts, then returns the Session to Idle (Req 4.8).</summary>
public sealed class StopSessionCommandHandler
    : ICommandHandler<StopSessionCommand, SessionStatusDto>
{
    public static readonly TimeSpan StopBudget = TimeSpan.FromSeconds(10);

    private readonly SessionCoordinator _coordinator;
    private readonly IMicroserviceCluster _cluster;
    private readonly IFlowEventBroadcaster _broadcaster;
    private readonly SessionStatusProvider _status;
    private readonly TimeProvider _timeProvider;

    public StopSessionCommandHandler(
        SessionCoordinator coordinator,
        IMicroserviceCluster cluster,
        IFlowEventBroadcaster broadcaster,
        SessionStatusProvider status,
        TimeProvider timeProvider)
    {
        _coordinator = coordinator ?? throw new ArgumentNullException(nameof(coordinator));
        _cluster = cluster ?? throw new ArgumentNullException(nameof(cluster));
        _broadcaster = broadcaster ?? throw new ArgumentNullException(nameof(broadcaster));
        _status = status ?? throw new ArgumentNullException(nameof(status));
        _timeProvider = timeProvider ?? throw new ArgumentNullException(nameof(timeProvider));
    }

    public async Task<Result<SessionStatusDto>> HandleAsync(
        StopSessionCommand command, CancellationToken cancellationToken)
    {
        if (!await _coordinator.BeginStopAsync(cancellationToken).ConfigureAwait(false))
        {
            return AppError.Conflict("No Provider is started.");
        }

        _broadcaster.Publish(new SessionChangedEvent(
            await _status.GetAsync(cancellationToken).ConfigureAwait(false)));

        try
        {
            // Not linked to the request token: once stopping began, the hosts must be released.
            using var budget = new CancellationTokenSource(StopBudget, _timeProvider);
            await _cluster.StopAsync(budget.Token).ConfigureAwait(false);
        }
        finally
        {
            await _coordinator.CompleteStopAsync(CancellationToken.None).ConfigureAwait(false);
        }

        var idle = await _status.GetAsync(cancellationToken).ConfigureAwait(false);
        _broadcaster.Publish(new SessionChangedEvent(idle));
        return Result<SessionStatusDto>.Success(idle);
    }
}
