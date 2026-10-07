namespace Messaging.Abstractions;

/// <summary>
/// Maps each supported <see cref="MessagingMode"/> to exactly one <see cref="IMessagingProvider"/>
/// and resolves the active provider at startup (Req 1.5, 3.4, 3.5).
/// </summary>
public interface IProviderRegistry
{
    /// <summary>
    /// Resolves the provider for the given mode. Fails fast on zero matches (unresolved mode) or
    /// more than one match (ambiguous mapping) (Req 1.6, 3.5, 3.7).
    /// </summary>
    /// <param name="mode">The configured mode to resolve.</param>
    IMessagingProvider Resolve(MessagingMode mode);

    /// <summary>The modes that currently have a registered provider.</summary>
    IReadOnlyCollection<MessagingMode> RegisteredModes { get; }
}
