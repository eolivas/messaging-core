namespace Demo.Application.Providers;

public sealed record ProviderDescriptor(
    ProviderId Id,
    string DisplayName,
    bool Supported,
    string? UnsupportedReason);
