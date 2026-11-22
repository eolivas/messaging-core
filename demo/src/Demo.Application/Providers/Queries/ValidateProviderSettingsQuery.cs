namespace Demo.Application.Providers.Queries;

public sealed record ValidateProviderSettingsQuery(
    string ProviderId,
    IReadOnlyDictionary<string, string>? Overrides);
