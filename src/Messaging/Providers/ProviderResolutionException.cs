using Messaging.Abstractions;

namespace Messaging.Providers;

/// <summary>
/// Thrown when the <see cref="IProviderRegistry"/> cannot resolve exactly one
/// <see cref="IMessagingProvider"/> for a configured <see cref="MessagingMode"/>: either no
/// provider is registered for the mode (unresolved mapping, Req 1.6, 3.7) or more than one is
/// (ambiguous mapping, Req 3.5). Resolution fails fast at startup so a misconfigured instance
/// never reaches a processing-ready state.
/// </summary>
public sealed class ProviderResolutionException : Exception
{
    private ProviderResolutionException(MessagingMode mode, ProviderResolutionFailure failure, string message)
        : base(message)
    {
        Mode = mode;
        Failure = failure;
    }

    /// <summary>The mode whose resolution failed.</summary>
    public MessagingMode Mode { get; }

    /// <summary>Which resolution rule was violated — distinguishes unresolved from ambiguous.</summary>
    public ProviderResolutionFailure Failure { get; }

    /// <summary>
    /// Creates an exception for a mode with no registered provider (zero matches, Req 1.6, 3.7).
    /// </summary>
    /// <param name="mode">The mode that resolved to no provider.</param>
    /// <param name="registeredModes">The modes that do have a registered provider, for diagnostics.</param>
    public static ProviderResolutionException Unresolved(
        MessagingMode mode,
        IReadOnlyCollection<MessagingMode> registeredModes)
    {
        var registered = registeredModes.Count == 0
            ? "none"
            : string.Join(", ", registeredModes);

        return new ProviderResolutionException(
            mode,
            ProviderResolutionFailure.Unresolved,
            $"No messaging provider is registered for mode '{mode}'. Registered modes: {registered}.");
    }

    /// <summary>
    /// Creates an exception for a mode served by more than one provider (ambiguous mapping, Req 3.5).
    /// </summary>
    /// <param name="mode">The mode that resolved to multiple providers.</param>
    /// <param name="matchCount">How many providers claimed the mode.</param>
    public static ProviderResolutionException Ambiguous(MessagingMode mode, int matchCount)
    {
        return new ProviderResolutionException(
            mode,
            ProviderResolutionFailure.Ambiguous,
            $"Ambiguous provider mapping for mode '{mode}': {matchCount} providers are registered, "
            + "but exactly one is required.");
    }
}

/// <summary>Distinguishes the two provider-resolution failure modes.</summary>
public enum ProviderResolutionFailure
{
    /// <summary>No provider was registered for the requested mode (zero matches).</summary>
    Unresolved,

    /// <summary>More than one provider was registered for the requested mode.</summary>
    Ambiguous,
}
