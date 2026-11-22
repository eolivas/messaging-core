namespace Demo.Application.Sessions.Commands;

/// <summary>ConsumerId is Customer or Communications. Mode is Off, FailOnce or AlwaysFail.</summary>
public sealed record SetFailureModeCommand(string ConsumerId, string Mode);
