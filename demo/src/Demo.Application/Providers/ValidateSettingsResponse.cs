namespace Demo.Application.Providers;

/// <summary>Response of the settings validate endpoint: <c>{valid, errors[]}</c>.</summary>
public sealed record ValidateSettingsResponse(bool Valid, IReadOnlyList<SettingErrorDto> Errors);
