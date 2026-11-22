namespace Demo.Application.Providers;

public sealed record ValidateSettingsRequest(IReadOnlyDictionary<string, string> Overrides);
