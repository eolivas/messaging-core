namespace Demo.Application.Providers;

/// <summary>A setting as shown to the user. Secret values are already masked.</summary>
public sealed record SettingEntryDto(
    string Group,
    string Name,
    string Value,
    bool IsSecret,
    bool Editable,
    string? PermittedValuesOrRange);
