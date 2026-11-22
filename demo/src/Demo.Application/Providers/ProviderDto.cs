namespace Demo.Application.Providers;

public sealed record ProviderDto(string Id, string DisplayName, bool Supported, string? UnsupportedReason);
