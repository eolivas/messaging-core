using Demo.Application.Providers;

namespace Demo.Application.Sessions;

/// <summary>Validated input for starting the three microservice hosts.</summary>
public sealed record ClusterStartRequest(
    ProviderId ProviderId,
    IReadOnlyDictionary<string, string> EffectiveSettings);
