namespace Demo.Infrastructure.Messaging;

/// <summary>Last attempt number and classified failure kind seen for a correlation id.</summary>
public sealed record AttemptInfo(int Attempt, string FailureKind);
