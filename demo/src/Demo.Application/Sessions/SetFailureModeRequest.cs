namespace Demo.Application.Sessions;

/// <summary>Mode is one of Off, FailOnce, AlwaysFail.</summary>
public sealed record SetFailureModeRequest(string Mode);
