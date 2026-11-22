using Demo.Application.Common;
using Demo.Application.Cqrs;
using Demo.Application.Exceptions;
using Demo.Application.Flow;

namespace Demo.Application.Sessions.Commands;

/// <summary>Clears the retained flow steps and resets the counters (Req 9.9).</summary>
public sealed class ClearFlowCommandHandler : ICommandHandler<ClearFlowCommand, Unit>
{
    private readonly SessionCoordinator _coordinator;
    private readonly SessionFlowLog _flowLog;
    private readonly IFlowEventBroadcaster _broadcaster;

    public ClearFlowCommandHandler(
        SessionCoordinator coordinator, SessionFlowLog flowLog, IFlowEventBroadcaster broadcaster)
    {
        _coordinator = coordinator ?? throw new ArgumentNullException(nameof(coordinator));
        _flowLog = flowLog ?? throw new ArgumentNullException(nameof(flowLog));
        _broadcaster = broadcaster ?? throw new ArgumentNullException(nameof(broadcaster));
    }

    public Task<Result<Unit>> HandleAsync(ClearFlowCommand command, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        try
        {
            _coordinator.EnsureActive();
        }
        catch (SessionConflictException ex)
        {
            return Task.FromResult<Result<Unit>>(AppError.Conflict(ex.Message));
        }

        _flowLog.Clear();
        _broadcaster.Publish(_flowLog.GetCounters());
        return Task.FromResult(Result<Unit>.Success(default));
    }
}
