namespace Demo.Application.Sessions;

/// <summary>
/// Guarded Session lifecycle: Idle to Starting to Active to Stopping to Idle, plus Starting to Idle
/// on a failed start. Any other transition is refused and leaves the state unchanged.
/// </summary>
public sealed class SessionStateMachine
{
    private readonly object _gate = new();
    private SessionState _state = SessionState.Idle;

    public SessionState State
    {
        get
        {
            lock (_gate)
            {
                return _state;
            }
        }
    }

    /// <summary>Whether the machine permits moving from <paramref name="from"/> to <paramref name="to"/>.</summary>
    public static bool IsAllowed(SessionState from, SessionState to) => (from, to) switch
    {
        (SessionState.Idle, SessionState.Starting) => true,
        (SessionState.Starting, SessionState.Active) => true,
        (SessionState.Starting, SessionState.Idle) => true,
        (SessionState.Active, SessionState.Stopping) => true,
        (SessionState.Stopping, SessionState.Idle) => true,
        _ => false
    };

    /// <summary>Moves to <paramref name="target"/> when allowed. Returns false and keeps the state otherwise.</summary>
    public bool TryTransition(SessionState target)
    {
        lock (_gate)
        {
            if (!IsAllowed(_state, target))
            {
                return false;
            }

            _state = target;
            return true;
        }
    }
}
