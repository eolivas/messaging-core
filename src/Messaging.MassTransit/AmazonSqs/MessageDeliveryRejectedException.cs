namespace Messaging.MassTransit.AmazonSqs;

/// <summary>
/// Thrown when a delivery is rejected because the consumer is no longer accepting work (shutdown).
/// MassTransit then releases the message back to the queue.
/// </summary>
public sealed class MessageDeliveryRejectedException : Exception
{
    /// <summary>Creates the exception with a default message.</summary>
    public MessageDeliveryRejectedException()
        : base("The delivery was rejected because the consumer is no longer accepting messages.")
    {
    }

    /// <summary>Creates the exception with a description.</summary>
    /// <param name="message">A description that must not contain secrets or payload bytes.</param>
    public MessageDeliveryRejectedException(string message)
        : base(message)
    {
    }

    /// <summary>Creates the exception with a description and the underlying cause.</summary>
    /// <param name="message">A description that must not contain secrets or payload bytes.</param>
    /// <param name="innerException">The underlying cause.</param>
    public MessageDeliveryRejectedException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
