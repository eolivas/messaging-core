using Demo.Application.Exceptions;
using Demo.Application.Providers;

namespace Demo.Application.Sessions;

/// <summary>
/// Owns the single Session: its state, active provider, per-consumer failure modes and last error.
/// Transitions run under a <see cref="SemaphoreSlim"/> so concurrent callers see one consistent state.
/// </summary>
public sealed class SessionCoordinator : IDisposable
{
    private readonly SemaphoreSlim _gate = new(1, 1);
    private readonly SessionStateMachine _machine = new();
    private readonly Dictionary<ConsumerId, FailureMode> _failureModes = NewFailureModes();
    private readonly CancellationTokenSource _lifetime = new();
    private Task _background = Task.CompletedTask;
    private ProviderId? _providerId;
    private ProblemSummaryDto? _lastError;

    public SessionState State => _machine.State;

    /// <summary>The provider of the Starting, Active or Stopping Session, or null when Idle.</summary>
    public ProviderId? ActiveProvider => Read(() => _providerId);

    public ProblemSummaryDto? LastError => Read(() => _lastError);

    /// <summary>Idle to Starting. Throws when a Session already exists (Req 4.7, 12.6).</summary>
    public async Task BeginStartAsync(ProviderId providerId, CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (!_machine.TryTransition(SessionState.Starting))
            {
                var active = _providerId?.Value;
                throw new SessionConflictException(
                    active is null
                        ? "A Session is already in progress."
                        : $"A Session is already in progress for provider '{active}'.",
                    active);
            }

            _providerId = providerId;
            _lastError = null;
            ResetFailureModes();
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>Starting to Active (Req 5.5).</summary>
    public Task CompleteStartAsync(CancellationToken cancellationToken) =>
        TransitionAsync(SessionState.Starting, SessionState.Active, clearProvider: false, error: null, cancellationToken);

    /// <summary>Starting to Idle, recording the sanitized error (Req 4.10, 5.6).</summary>
    public Task FailStartAsync(ProblemSummaryDto error, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(error);
        return TransitionAsync(SessionState.Starting, SessionState.Idle, clearProvider: true, error, cancellationToken);
    }

    /// <summary>
    /// Active to Stopping. Returns false when no Session is active, so Stop on Idle is a no-op.
    /// </summary>
    public async Task<bool> BeginStopAsync(CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            return _machine.TryTransition(SessionState.Stopping);
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>Stopping to Idle.</summary>
    public Task CompleteStopAsync(CancellationToken cancellationToken) =>
        TransitionAsync(SessionState.Stopping, SessionState.Idle, clearProvider: true, error: null, cancellationToken);

    /// <summary>Throws <see cref="SessionConflictException"/> unless the Session is Active (Req 12.5).</summary>
    public void EnsureActive()
    {
        if (_machine.State != SessionState.Active)
        {
            throw new SessionConflictException("No Provider is started.");
        }
    }

    public FailureMode GetFailureMode(ConsumerId consumer) => Read(() => _failureModes[consumer]);

    public void SetFailureMode(ConsumerId consumer, FailureMode mode)
    {
        _gate.Wait();
        try
        {
            _failureModes[consumer] = mode;
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>
    /// Runs Session start work on a coordinator-owned background task. The token is cancelled when the
    /// coordinator is disposed. The work must handle its own failures.
    /// </summary>
    public void RunInBackground(Func<CancellationToken, Task> work)
    {
        ArgumentNullException.ThrowIfNull(work);
        var token = _lifetime.Token;
        Volatile.Write(ref _background, Task.Run(() => work(token), CancellationToken.None));
    }

    /// <summary>Completes when the latest background work finishes.</summary>
    public Task WaitForBackgroundAsync() => Volatile.Read(ref _background);

    public void Dispose()
    {
        _lifetime.Cancel();
        _lifetime.Dispose();
        _gate.Dispose();
    }

    private async Task TransitionAsync(
        SessionState expected,
        SessionState target,
        bool clearProvider,
        ProblemSummaryDto? error,
        CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_machine.State != expected || !_machine.TryTransition(target))
            {
                throw new SessionConflictException(
                    $"Cannot move the Session from {_machine.State} to {target}.",
                    _providerId?.Value);
            }

            if (clearProvider)
            {
                _providerId = null;
                ResetFailureModes();
            }

            if (error is not null)
            {
                _lastError = error;
            }
        }
        finally
        {
            _gate.Release();
        }
    }

    private T Read<T>(Func<T> read)
    {
        _gate.Wait();
        try
        {
            return read();
        }
        finally
        {
            _gate.Release();
        }
    }

    private void ResetFailureModes()
    {
        foreach (var consumer in _failureModes.Keys.ToArray())
        {
            _failureModes[consumer] = FailureMode.Off;
        }
    }

    private static Dictionary<ConsumerId, FailureMode> NewFailureModes() =>
        Enum.GetValues<ConsumerId>().ToDictionary(c => c, _ => FailureMode.Off);
}
