using Demo.Application.Common;
using Demo.Application.Cqrs;
using Demo.Application.Exceptions;

namespace Demo.Application.Sessions.Queries;

/// <summary>
/// Returns the library configuration as applied by the active host. Without a Session it returns a
/// Conflict, and when the host cannot describe itself it returns an error, never default values.
/// </summary>
public sealed class GetIntegrationDetailsQueryHandler
    : IQueryHandler<GetIntegrationDetailsQuery, IntegrationDetailsDto>
{
    private readonly SessionCoordinator _coordinator;
    private readonly IMicroserviceCluster _cluster;

    public GetIntegrationDetailsQueryHandler(
        SessionCoordinator coordinator, IMicroserviceCluster cluster)
    {
        _coordinator = coordinator ?? throw new ArgumentNullException(nameof(coordinator));
        _cluster = cluster ?? throw new ArgumentNullException(nameof(cluster));
    }

    public Task<Result<IntegrationDetailsDto>> HandleAsync(
        GetIntegrationDetailsQuery query, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        try
        {
            _coordinator.EnsureActive();
        }
        catch (SessionConflictException ex)
        {
            return Task.FromResult<Result<IntegrationDetailsDto>>(AppError.Conflict(ex.Message));
        }

        try
        {
            var info = _cluster.Describe();
            return Task.FromResult(Result<IntegrationDetailsDto>.Success(new IntegrationDetailsDto(
                info.MessagingMode,
                info.ProviderName,
                info.IdempotencyEnabled,
                info.RetryMaxAttempts,
                info.BackoffStrategy)));
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return Task.FromResult<Result<IntegrationDetailsDto>>(
                AppError.Unavailable("The integration details could not be read."));
        }
    }
}
