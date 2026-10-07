using Messaging.Abstractions;

namespace Messaging.Providers;

/// <summary>
/// Immutable <see cref="IProviderRegistry"/> built from the set of <see cref="IMessagingProvider"/>
/// instances registered in DI, keyed by each provider's <see cref="IMessagingProvider.Mode"/>
/// (Req 1.5, 3.4). The map is fixed at construction and never mutated, so resolution is stable for
/// the instance lifetime and the active mode cannot change after startup (Req 1.4).
/// </summary>
/// <remarks>
/// Ambiguity (more than one provider claiming the same mode) is retained rather than collapsed at
/// construction so that <see cref="Resolve"/> can fail fast with a precise ambiguous-mapping error
/// per the port contract (Req 3.5). <see cref="RegisteredModes"/> reports every mode that has at
/// least one provider, including ambiguously-mapped ones.
/// </remarks>
public sealed class ProviderRegistry : IProviderRegistry
{
    private readonly IReadOnlyDictionary<MessagingMode, IReadOnlyList<IMessagingProvider>> _providersByMode;

    /// <summary>
    /// Builds the registry from the DI-registered providers, grouping them by mode so that
    /// zero- and multiple-provider cases can be detected at resolution time (Req 1.5, 3.4, 3.5).
    /// </summary>
    /// <param name="providers">The providers registered in the container as <see cref="IMessagingProvider"/>.</param>
    /// <exception cref="ArgumentNullException">If <paramref name="providers"/> is null.</exception>
    public ProviderRegistry(IEnumerable<IMessagingProvider> providers)
    {
        ArgumentNullException.ThrowIfNull(providers);

        _providersByMode = providers
            .GroupBy(provider => provider.Mode)
            .ToDictionary(
                group => group.Key,
                group => (IReadOnlyList<IMessagingProvider>)group.ToArray());
    }

    /// <inheritdoc />
    public IReadOnlyCollection<MessagingMode> RegisteredModes => (IReadOnlyCollection<MessagingMode>)_providersByMode.Keys;

    /// <inheritdoc />
    public IMessagingProvider Resolve(MessagingMode mode)
    {
        if (!_providersByMode.TryGetValue(mode, out var matches) || matches.Count == 0)
        {
            throw ProviderResolutionException.Unresolved(mode, RegisteredModes);
        }

        if (matches.Count > 1)
        {
            throw ProviderResolutionException.Ambiguous(mode, matches.Count);
        }

        return matches[0];
    }
}
