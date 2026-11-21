namespace Messaging.MassTransit.AmazonSqs;

/// <summary>A desired subscription of a queue to a topic.</summary>
/// <param name="TopicName">The topic name.</param>
/// <param name="QueueName">The queue name.</param>
/// <param name="RawMessageDelivery">Whether raw message delivery is enabled.</param>
public sealed record SubscriptionSpec(string TopicName, string QueueName, bool RawMessageDelivery);
