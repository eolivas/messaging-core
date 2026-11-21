namespace Messaging.MassTransit.AmazonSqs;

/// <summary>
/// Thrown when an SNS or SQS control-plane call fails (for example an authorization or service error).
/// The message names the operation and resource only; the inner exception is kept for logging.
/// </summary>
public sealed class TopologyOperationFailedException : Exception
{
    /// <summary>Creates the exception for a failed operation on a resource.</summary>
    /// <param name="operation">The operation name, for example <c>sqs:CreateQueue</c>.</param>
    /// <param name="resource">The name of the resource the operation targeted.</param>
    /// <param name="innerException">The underlying SDK failure, if any.</param>
    public TopologyOperationFailedException(string operation, string resource, Exception? innerException = null)
        : base($"Topology operation '{operation}' failed for resource '{resource}'.", innerException)
    {
        Operation = operation;
        Resource = resource;
    }

    /// <summary>The operation name, for example <c>sqs:CreateQueue</c>.</summary>
    public string Operation { get; }

    /// <summary>The name of the resource the operation targeted.</summary>
    public string Resource { get; }
}
