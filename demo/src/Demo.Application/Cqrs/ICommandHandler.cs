using Demo.Application.Common;

namespace Demo.Application.Cqrs;

public interface ICommandHandler<in TCommand, TResult>
{
    Task<Result<TResult>> HandleAsync(TCommand command, CancellationToken cancellationToken);
}
