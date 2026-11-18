namespace Messaging.MassTransit.AmazonSqs;

/// <summary>A desired SQS queue (consumer queue or dead-letter queue).</summary>
/// <param name="Name">The derived queue name.</param>
/// <param name="IsDeadLetter">Whether this is a dead-letter queue.</param>
/// <param name="DeadLetterTarget">Name of the dead-letter queue for redrive; null for a dead-letter queue.</param>
/// <param name="MaxReceiveCount">Redrive <c>maxReceiveCount</c>; null when no redrive applies.</param>
/// <param name="VisibilityTimeoutSeconds">Visibility timeout in seconds.</param>
/// <param name="RetentionSeconds">Message retention in seconds.</param>
/// <param name="Fifo">Whether the queue is FIFO.</param>
/// <param name="KmsKeyId">Optional KMS master key id.</param>
public sealed record QueueSpec(
    string Name,
    bool IsDeadLetter,
    string? DeadLetterTarget,
    int? MaxReceiveCount,
    int VisibilityTimeoutSeconds,
    int RetentionSeconds,
    bool Fifo,
    string? KmsKeyId);
