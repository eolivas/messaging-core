namespace Messaging.Kafka.Streaming;

/// <summary>
/// Thrown when an offset commit is not acknowledged by the broker (or is requested for a record this
/// consumer never handed out). The message identifies the partition and offset only; it never carries
/// payload bytes, header values, or credentials (Req 4.7, 10.3). The committed offset is unchanged.
/// </summary>
public sealed class KafkaOffsetCommitException : Exception
{
    /// <summary>Creates the exception for a record at <paramref name="partition"/>/<paramref name="offset"/>.</summary>
    public KafkaOffsetCommitException(int partition, long offset, Exception? innerException = null)
        : base($"Kafka offset commit was not acknowledged for partition {partition}, offset {offset}.", innerException)
    {
        Partition = partition;
        Offset = offset;
    }

    /// <summary>The partition whose commit failed.</summary>
    public int Partition { get; }

    /// <summary>The offset of the record whose commit failed.</summary>
    public long Offset { get; }
}
