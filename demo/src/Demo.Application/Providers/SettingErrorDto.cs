namespace Demo.Application.Providers;

public sealed record SettingErrorDto(string Field, string Message, string? PermittedValuesOrRange);
