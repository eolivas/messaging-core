using Messaging.Contracts;

namespace Messaging.Kafka.Streaming;

/// <summary>
/// The seam the publisher produces through: a thin wrapper over the native Confluent.Kafka
/// <c>IProducer</c>. This is the shallow port the shell plugs into; the concrete producer
/// configuration (acks, idempotent producer, partitioner, serialization) is owned by a later spec.
/// Keeping the native client behind this port lets the shell express the "produce to a topic" shape
/// (Req 3.4) and stay testable without a live broker.
/// </summary>
public interface IKafkaProducer
{
    /// <summary>
    /// Produces the envelope to the given topic and completes when the broker has acknowledged the
    /// write. A thrown exception means the produce failed and the envelope is undelivered (Req 3.2).
    /// </summary>
    /// <param name="topic">The destination topic.</param>
    /// <param name="envelope">The envelope to produce.</param>
    /// <param name="cancellationToken">Token to cancel the produce.</param>
    Task ProduceAsync(string topic, MessageEnvelope envelope, CancellationToken cancellationToken);
}
