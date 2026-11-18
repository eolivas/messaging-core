namespace Messaging.MassTransit.AmazonSqs;

/// <summary>
/// Thrown when an existing resource's type (FIFO or standard) disagrees with the configured delivery mode.
/// </summary>
public sealed class TopologyConflictException : Exception
{
    /// <summary>Creates the exception for a resource whose existing type conflicts with the mode.</summary>
    /// <param name="resource">The name of the conflicting resource.</param>
    /// <param name="existingType">The type the resource already has, for example <c>FIFO</c> or <c>Standard</c>.</param>
    public TopologyConflictException(string resource, string existingType)
        : base($"Resource '{resource}' already exists as type '{existingType}', which conflicts with the configured delivery mode.")
    {
        Resource = resource;
        ExistingType = existingType;
    }

    /// <summary>The name of the conflicting resource.</summary>
    public string Resource { get; }

    /// <summary>The type the resource already has.</summary>
    public string ExistingType { get; }
}
