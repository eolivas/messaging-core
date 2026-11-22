namespace Demo.Application.Providers;

public sealed record SettingError(string Field, string Message, string? PermittedValuesOrRange);
