using Demo.Application.Common;
using Demo.Application.Cqrs;
using Demo.Application.Exceptions;
using Demo.Application.Flow;
using Demo.Application.Providers;
using Demo.Application.Providers.Queries;
using FluentValidation;
using Microsoft.Extensions.Logging;

namespace Demo.Application.Sessions.Commands;

/// <summary>
/// Validates the request synchronously, moves the Session to Starting and returns 202 content.
/// The cluster start then runs on a coordinator-owned background task with a 30 s budget and,
/// on failure, a 10 s cleanup budget.
/// </summary>
public sealed class StartSessionCommandHandler
    : ICommandHandler<StartSessionCommand, SessionStatusDto>
{
    public static readonly TimeSpan StartBudget = TimeSpan.FromSeconds(30);
    public static readonly TimeSpan CleanupBudget = TimeSpan.FromSeconds(10);

    private readonly IValidator<StartSessionCommand> _validator;
    private readonly IProviderCatalog _catalog;
    private readonly SessionCoordinator _coordinator;
    private readonly IMicroserviceCluster _cluster;
    private readonly SessionFlowLog _flowLog;
    private readonly IFlowEventBroadcaster _broadcaster;
    private readonly SessionStatusProvider _status;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<StartSessionCommandHandler> _logger;

    public StartSessionCommandHandler(
        IValidator<StartSessionCommand> validator,
        IProviderCatalog catalog,
        SessionCoordinator coordinator,
        IMicroserviceCluster cluster,
        SessionFlowLog flowLog,
        IFlowEventBroadcaster broadcaster,
        SessionStatusProvider status,
        TimeProvider timeProvider,
        ILogger<StartSessionCommandHandler> logger)
    {
        _validator = validator ?? throw new ArgumentNullException(nameof(validator));
        _catalog = catalog ?? throw new ArgumentNullException(nameof(catalog));
        _coordinator = coordinator ?? throw new ArgumentNullException(nameof(coordinator));
        _cluster = cluster ?? throw new ArgumentNullException(nameof(cluster));
        _flowLog = flowLog ?? throw new ArgumentNullException(nameof(flowLog));
        _broadcaster = broadcaster ?? throw new ArgumentNullException(nameof(broadcaster));
        _status = status ?? throw new ArgumentNullException(nameof(status));
        _timeProvider = timeProvider ?? throw new ArgumentNullException(nameof(timeProvider));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task<Result<SessionStatusDto>> HandleAsync(
        StartSessionCommand command, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(command);

        var shape = await _validator.ValidateAsync(command, cancellationToken).ConfigureAwait(false);
        if (!shape.IsValid)
        {
            return AppError.Validation(
                "The request is invalid.",
                shape.Errors
                    .GroupBy(e => e.PropertyName, StringComparer.Ordinal)
                    .Select(g => new FieldError(g.Key, g.Select(e => e.ErrorMessage).ToArray()))
                    .ToArray());
        }

        var lookup = ProviderLookup.Find(_catalog, command.ProviderId);
        if (lookup.IsFailure)
        {
            return lookup.Error;
        }

        var id = lookup.Value.Id;
        var effective = ResolveEffectiveSettings(id, command.Overrides, out var failure);
        if (failure is not null)
        {
            return failure;
        }

        try
        {
            await _coordinator.BeginStartAsync(id, cancellationToken).ConfigureAwait(false);
        }
        catch (SessionConflictException ex)
        {
            return AppError.Conflict(ex.Message);
        }

        _flowLog.Clear();
        _broadcaster.Publish(_flowLog.GetCounters());
        var request = new ClusterStartRequest(id, effective!);
        _coordinator.RunInBackground(token => RunStartAsync(request, lookup.Value.DisplayName, token));

        var starting = await _status.GetAsync(cancellationToken).ConfigureAwait(false);
        _broadcaster.Publish(new SessionChangedEvent(starting));
        return Result<SessionStatusDto>.Success(starting);
    }

    private Dictionary<string, string>? ResolveEffectiveSettings(
        ProviderId id,
        IReadOnlyDictionary<string, string>? overrides,
        out AppError? failure)
    {
        failure = null;
        var snapshot = _catalog.GetDefaults(id);
        if (snapshot is null)
        {
            failure = AppError.Unavailable("The defaults for this provider could not be loaded.");
            return null;
        }

        var byName = snapshot.Settings.ToDictionary(s => s.Name, StringComparer.Ordinal);
        var effective = snapshot.Settings.ToDictionary(s => s.Name, s => s.Value, StringComparer.Ordinal);
        var errors = new List<FieldError>();

        foreach (var (name, value) in overrides ?? new Dictionary<string, string>())
        {
            if (!byName.TryGetValue(name, out var entry))
            {
                errors.Add(new FieldError(name, new[] { "The setting does not exist." }));
            }
            else if (entry.IsSecret)
            {
                errors.Add(new FieldError(name, new[] { "Secret settings cannot be overridden." }));
            }
            else if (!entry.Editable)
            {
                errors.Add(new FieldError(name, new[] { "The setting is read-only." }));
            }
            else
            {
                effective[name] = value;
            }
        }

        if (errors.Count == 0)
        {
            foreach (var error in _catalog.Validate(id, effective))
            {
                var message = error.PermittedValuesOrRange is null
                    ? error.Message
                    : $"{error.Message} Permitted: {error.PermittedValuesOrRange}";
                errors.Add(new FieldError(error.Field, new[] { message }));
            }
        }

        if (errors.Count > 0)
        {
            failure = AppError.Validation("The provider settings are invalid.", errors);
            return null;
        }

        return effective;
    }

    private async Task RunStartAsync(
        ClusterStartRequest request, string providerName, CancellationToken lifetime)
    {
        using var budget = new CancellationTokenSource(StartBudget, _timeProvider);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(lifetime, budget.Token);
        try
        {
            await _cluster.StartAsync(request, linked.Token).ConfigureAwait(false);
            await _coordinator.CompleteStartAsync(CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            await FailStartAsync(ex, budget.IsCancellationRequested, providerName).ConfigureAwait(false);
        }

        await PublishStatusAsync().ConfigureAwait(false);
    }

    private async Task FailStartAsync(Exception cause, bool timedOut, string providerName)
    {
        _logger.LogError(cause, "Session start failed for provider {Provider}.", providerName);

        using var cleanup = new CancellationTokenSource(CleanupBudget, _timeProvider);
        try
        {
            await _cluster.StopAsync(cleanup.Token).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Cleanup after a failed start of provider {Provider} failed.", providerName);
        }

        var detail = timedOut
            ? $"Provider '{providerName}' did not become ready within {StartBudget.TotalSeconds:0} seconds."
            : FailureReasonSanitizer.Sanitize(cause.Message);
        if (string.IsNullOrWhiteSpace(detail))
        {
            detail = $"Provider '{providerName}' could not be started.";
        }

        try
        {
            await _coordinator.FailStartAsync(
                new ProblemSummaryDto("Provider start failed", detail), CancellationToken.None)
                .ConfigureAwait(false);
        }
        catch (SessionConflictException ex)
        {
            _logger.LogError(ex, "The Session could not return to Idle after a failed start.");
        }
    }

    private async Task PublishStatusAsync()
    {
        try
        {
            _broadcaster.Publish(new SessionChangedEvent(
                await _status.GetAsync(CancellationToken.None).ConfigureAwait(false)));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Publishing the Session status after start failed.");
        }
    }
}
