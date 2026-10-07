namespace Messaging.Abstractions;

/// <summary>
/// Declares a provider-specific setting so the configuration validator can check its presence
/// and permitted values during startup validation (Req 2.5).
/// </summary>
/// <param name="Name">The configuration name of the provider-specific setting.</param>
/// <param name="Required">Whether the setting must be present for the provider to operate.</param>
/// <param name="PermittedValuesOrRange">
/// The permitted set or numeric range for the setting, when constrained; otherwise null.
/// </param>
public sealed record ProviderSettingDescriptor(string Name, bool Required, string? PermittedValuesOrRange);
