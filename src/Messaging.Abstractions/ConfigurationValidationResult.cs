namespace Messaging.Abstractions;

/// <summary>
/// The aggregated result of startup configuration validation. Validation runs in a single pass
/// and collects every error rather than stopping at the first (Req 2.2, 2.3, 2.6).
/// </summary>
/// <param name="Errors">Every validation failure found; empty when the configuration is valid.</param>
public sealed record ConfigurationValidationResult(IReadOnlyList<ConfigurationError> Errors)
{
    /// <summary>True only when zero errors were found, permitting a processing-ready state (Req 2.6).</summary>
    public bool IsValid => Errors.Count == 0;
}
