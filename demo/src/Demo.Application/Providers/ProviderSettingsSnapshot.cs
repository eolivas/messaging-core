namespace Demo.Application.Providers;

/// <summary>Raw defaults of a provider, with secret flags. Masking happens in the query handler.</summary>
public sealed record ProviderSettingsSnapshot(
    ProviderId ProviderId,
    string MessagingMode,
    IReadOnlyList<SettingEntry> Settings);
