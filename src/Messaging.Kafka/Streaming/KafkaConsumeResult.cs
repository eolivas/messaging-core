using Messaging.Contracts;

namespace Messaging.Kafka.Streaming;

/// <summary>
/// A single record pulled from the Kafka consumer, pairing the decoded <see cref="MessageEnvelope"/>
/// with the partition and offset it came from. The consumer host commits that partition/offset only
/// <b>after</b> the core pipeline has processed the envelope, which is what makes delivery
/// at-least-once (Req 6.7).
/// </summary>
/// <param name="Envelope">The envelope decoded from the Kafka record.</param>
/// <param name="Partition">The partition the record was read from.</param>
/// <param name="Offset">The offset of the record within its partition.</param>
public sealed record KafkaConsumeResult(MessageEnvelope Envelope, int Partition, long Offset);
