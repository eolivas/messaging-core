namespace Demo.Application.Sessions;

public sealed record StartSessionRequest(string ProviderId, IReadOnlyDictionary<string, string>? Overrides);
