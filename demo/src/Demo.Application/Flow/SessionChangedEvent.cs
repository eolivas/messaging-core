using Demo.Application.Sessions;

namespace Demo.Application.Flow;

/// <summary>SSE event <c>session</c>.</summary>
public sealed record SessionChangedEvent(SessionStatusDto Status) : SessionEvent;
