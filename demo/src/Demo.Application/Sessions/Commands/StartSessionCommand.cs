namespace Demo.Application.Sessions.Commands;

public sealed record StartSessionCommand(
    string ProviderId,
    IReadOnlyDictionary<string, string>? Overrides);
