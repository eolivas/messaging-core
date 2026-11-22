namespace Demo.Application.Flow;

/// <summary>SSE event <c>counters</c>.</summary>
public sealed record CountersEvent(long Published, long CustomersCreated, long EmailsSent) : SessionEvent;
