using Demo.Application.Common;
using Demo.Application.Cqrs;
using Demo.Application.Exceptions;

namespace Demo.Application.Sessions.Commands;

/// <summary>
/// Requires an Active Session and asks Sales Leads to publish exactly one event, with a 10 s linked
/// timeout. A failed or timed-out call is never retried (Req 6.9, 6.10).
/// </summary>
public sealed class TriggerQuoteAcceptedCommandHandler
    : ICommandHandler<TriggerQuoteAcceptedCommand, TriggerResultDto>
{
    public static readonly TimeSpan TriggerBudget = TimeSpan.FromSeconds(10);

    private readonly SessionCoordinator _coordinator;
    private readonly IMicroserviceCluster _cluster;
    private readonly TimeProvider _timeProvider;

    public TriggerQuoteAcceptedCommandHandler(
        SessionCoordinator coordinator, IMicroserviceCluster cluster, TimeProvider timeProvider)
    {
        _coordinator = coordinator ?? throw new ArgumentNullException(nameof(coordinator));
        _cluster = cluster ?? throw new ArgumentNullException(nameof(cluster));
        _timeProvider = timeProvider ?? throw new ArgumentNullException(nameof(timeProvider));
    }

    public async Task<Result<TriggerResultDto>> HandleAsync(
        TriggerQuoteAcceptedCommand command, CancellationToken cancellationToken)
    {
        try
        {
            _coordinator.EnsureActive();
        }
        catch (SessionConflictException ex)
        {
            return AppError.Conflict(ex.Message);
        }

        using var budget = new CancellationTokenSource(TriggerBudget, _timeProvider);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, budget.Token);
        AcceptQuoteOutcome outcome;
        try
        {
            outcome = await _cluster.SalesLeads.AcceptQuoteAsync(linked.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (budget.IsCancellationRequested
            && !cancellationToken.IsCancellationRequested)
        {
            return AppError.Timeout("The publish did not complete within 10 seconds.");
        }

        if (!outcome.Succeeded || outcome.CorrelationId is null || outcome.LeadId is null)
        {
            var reason = FailureReasonSanitizer.Sanitize(outcome.Error);
            var error = AppError.Unavailable(
                string.IsNullOrWhiteSpace(reason) ? "The event could not be published." : reason);
            return error with { CorrelationId = outcome.CorrelationId };
        }

        return Result<TriggerResultDto>.Success(new TriggerResultDto(outcome.CorrelationId, outcome.LeadId));
    }
}
