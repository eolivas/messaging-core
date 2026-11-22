using Demo.Application.Sessions;

namespace Demo.Application.Tests;

public class SessionStateMachineTests
{
    [Fact]
    public void OnlyDocumentedTransitionsAreAllowed()
    {
        var allowed = new HashSet<(SessionState, SessionState)>
        {
            (SessionState.Idle, SessionState.Starting),
            (SessionState.Starting, SessionState.Active),
            (SessionState.Starting, SessionState.Idle),
            (SessionState.Active, SessionState.Stopping),
            (SessionState.Stopping, SessionState.Idle)
        };
        foreach (var from in Enum.GetValues<SessionState>())
        {
            foreach (var to in Enum.GetValues<SessionState>())
            {
                Assert.Equal(allowed.Contains((from, to)), SessionStateMachine.IsAllowed(from, to));
            }
        }
    }

    [Fact]
    public void RefusedTransition_LeavesStateUnchanged()
    {
        var sut = new SessionStateMachine();
        Assert.False(sut.TryTransition(SessionState.Active));
        Assert.Equal(SessionState.Idle, sut.State);
    }
}
