using Demo.Contracts;

namespace Demo.Application.Flow;

/// <summary>SSE event <c>flowStep</c>. <see cref="Completed"/> marks the step that closed an event.</summary>
public sealed record FlowStepEvent(long Sequence, FlowStep Step, bool Completed) : SessionEvent;
