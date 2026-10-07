namespace Messaging.Abstractions;

/// <summary>
/// A single configuration validation failure, reported by its configuration name together with
/// the permitted set or permitted range where applicable (Req 2.2, 2.3).
/// </summary>
/// <param name="SettingName">The configuration name of the offending setting.</param>
/// <param name="Message">Human-readable description of the failure.</param>
/// <param name="PermittedValuesOrRange">
/// The permitted set or numeric range for the setting, when the failure is a range/set
/// violation; otherwise null (Req 2.3, 2.4).
/// </param>
public sealed record ConfigurationError(string SettingName, string Message, string? PermittedValuesOrRange);
