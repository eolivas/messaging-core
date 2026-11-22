namespace Demo.Application.Providers;

public sealed record ProviderDefaultsDto(
    string ProviderId,
    string MessagingMode,
    IReadOnlyList<SettingEntryDto> Settings);
