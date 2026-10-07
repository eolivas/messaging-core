namespace Messaging.Kafka.Streaming;

/// <summary>
/// The seam the consumer host drives: a thin wrapper over the native Confluent.Kafka
/// <c>IConsumer</c> that owns consumer-group membership, partition assignment, and — crucially —
/// <b>manual</b> offset commit. This is the shallow port the shell plugs into; the deep mechanics
/// (rebalance handling, partition-assignment strategy, offset-reset policy, retention/replay) are
/// owned by a later spec.
/// </summary>
/// <remarks>
/// <para>
/// Manual-commit contract: <see cref="CommitAsync"/> is a separate call from <see cref="ConsumeAsync"/>
/// precisely so the consumer host can commit an offset only after the core pipeline completes, never
/// before. Auto-commit is disabled by the concrete implementation; the shell owns commit timing to
/// preserve at-least-once (Req 6.7).
/// </para>
/// </remarks>
public interface IKafkaConsumer
{
    /// <summary>
    /// Subscribes the consumer to the topic within its consumer group, joining the group and
    /// receiving a partition assignment.
    /// </summary>
    /// <param name="topic">The topic to subscribe to.</param>
    void Subscribe(string topic);

    /// <summary>
    /// Blocks until the next record is available (or the token is cancelled) and returns it, or
    /// <c>null</c> when no record is available within the poll. Does not commit — commit is the
    /// host's responsibility via <see cref="CommitAsync"/> after the pipeline runs (Req 6.7).
    /// </summary>
    /// <param name="cancellationToken">Token to cancel the poll.</param>
    Task<KafkaConsumeResult?> ConsumeAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Commits the offset of the given record (the offset to resume from is the record's offset + 1).
    /// Called only after the core pipeline has processed the record's envelope, so a crash before the
    /// commit reprocesses the record rather than skipping it (at-least-once, Req 6.7).
    /// </summary>
    /// <param name="result">The processed record whose offset is being committed.</param>
    /// <param name="cancellationToken">Token to cancel the commit.</param>
    Task CommitAsync(KafkaConsumeResult result, CancellationToken cancellationToken);

    /// <summary>
    /// Leaves the consumer group and releases the partition assignment, closing the consumer so the
    /// group can rebalance the partitions onto the remaining members.
    /// </summary>
    void Close();
}
