namespace Messaging.MassTransit.AmazonSqs;

/// <summary>
/// Thrown in verify-only mode when required topics, queues, or subscriptions do not exist.
/// Names every missing resource.
/// </summary>
public sealed class TopologyResourceMissingException : Exception
{
    /// <summary>Creates the exception for the full set of missing resources.</summary>
    /// <param name="missingResources">The names of every missing resource.</param>
    public TopologyResourceMissingException(IReadOnlyList<string> missingResources)
        : base(BuildMessage(missingResources))
    {
        MissingResources = missingResources;
    }

    /// <summary>The names of every missing resource.</summary>
    public IReadOnlyList<string> MissingResources { get; }

    private static string BuildMessage(IReadOnlyList<string> missingResources)
    {
        ArgumentNullException.ThrowIfNull(missingResources);
        return $"Required topology resources are missing: {string.Join(", ", missingResources)}.";
    }
}
