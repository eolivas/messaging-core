namespace Messaging.MassTransit.AmazonSqs;

/// <summary>A desired SNS topic.</summary>
/// <param name="Name">The derived topic name.</param>
/// <param name="Fifo">Whether the topic is FIFO.</param>
/// <param name="KmsKeyId">Optional KMS master key id.</param>
public sealed record TopicSpec(string Name, bool Fifo, string? KmsKeyId);
