namespace Messaging.Kafka.Streaming;

/// <summary>
/// Thrown when a produce was not acknowledged by all in-sync replicas. The message names the topic
/// (and the Kafka error code) only; it never carries payload bytes, header values, or credentials
/// (Req 10.3, 12.10), and the native exception is deliberately not chained because its delivery
/// result holds the message payload.
/// </summary>
public sealed class KafkaProduceException : Exception
{
    /// <summary>Creates the exception for the given topic.</summary>
    /// <param name="topic">The topic the produce targeted.</param>
    /// <param name="detail">A payload-free description of the failure.</param>
    public KafkaProduceException(string topic, string detail)
        : base($"Produce to Kafka topic '{topic}' was not acknowledged by all in-sync replicas: {detail}")
    {
        Topic = topic;
    }

    /// <summary>The topic the failed produce targeted.</summary>
    public string Topic { get; }
}
