using Demo.Application.Common;
using Demo.Application.Cqrs;
using Demo.Application.Exceptions;

namespace Demo.Application.Sessions.Commands;

/// <summary>Sets the failure mode of one consumer for the active Session (Req 10.5).</summary>
public sealed class SetFailureModeCommandHandler
    : ICommandHandler<SetFailureModeCommand, Unit>
{
    private readonly SessionCoordinator _coordinator;
    private readonly IMicroserviceCluster _cluster;

    public SetFailureModeCommandHandler(SessionCoordinator coordinator, IMicroserviceCluster cluster)
    {
        _coordinator = coordinator ?? throw new ArgumentNullException(nameof(coordinator));
        _cluster = cluster ?? throw new ArgumentNullException(nameof(cluster));
    }

    public Task<Result<Unit>> HandleAsync(
        SetFailureModeCommand command, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(command);
        cancellationToken.ThrowIfCancellationRequested();

        var errors = new List<FieldError>();
        if (!TryParse<ConsumerId>(command.ConsumerId, out var consumer))
        {
            errors.Add(new FieldError("consumerId", new[] { "Must be one of: Customer, Communications." }));
        }

        if (!TryParse<FailureMode>(command.Mode, out var mode))
        {
            errors.Add(new FieldError("mode", new[] { "Must be one of: Off, FailOnce, AlwaysFail." }));
        }

        if (errors.Count > 0)
        {
            return Task.FromResult<Result<Unit>>(AppError.Validation("The request is invalid.", errors));
        }

        try
        {
            _coordinator.EnsureActive();
        }
        catch (SessionConflictException ex)
        {
            return Task.FromResult<Result<Unit>>(AppError.Conflict(ex.Message));
        }

        _coordinator.SetFailureMode(consumer, mode);
        _cluster.SetFailureMode(consumer, mode);
        return Task.FromResult(Result<Unit>.Success(default));
    }

    private static bool TryParse<TEnum>(string? value, out TEnum result)
        where TEnum : struct, Enum
    {
        result = default;
        return !string.IsNullOrWhiteSpace(value)
            && !char.IsDigit(value[0])
            && Enum.TryParse(value, ignoreCase: true, out result)
            && Enum.IsDefined(result);
    }
}
