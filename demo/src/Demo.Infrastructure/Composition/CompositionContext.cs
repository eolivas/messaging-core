using Demo.Contracts;

namespace Demo.Infrastructure.Composition;

/// <summary>
/// Inputs shared by every provider composition for one microservice host. Service is the
/// microservice the host runs. EffectiveSettings holds provider settings keyed as Group:Name,
/// after user edits.
/// </summary>
public sealed record CompositionContext(
    MicroserviceId Service,
    IReadOnlyDictionary<string, string> EffectiveSettings);
