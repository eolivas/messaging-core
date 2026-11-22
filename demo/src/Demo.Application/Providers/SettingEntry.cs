namespace Demo.Application.Providers;

/// <summary>A raw provider setting. <see cref="Value"/> is unmasked and must never leave Application unmasked.</summary>
public sealed record SettingEntry(
    string Group,
    string Name,
    string Value,
    bool IsSecret,
    bool Editable,
    string? PermittedValuesOrRange);
