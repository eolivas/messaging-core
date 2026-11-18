namespace Messaging.MassTransit.AmazonSqs;

/// <summary>
/// Narrow port over the SNS and SQS control plane. Lookups of absent resources return null (or an
/// empty list) instead of throwing. Authorization and service errors are not wrapped here.
/// </summary>
public interface ISnsSqsTopologyClient
{
    /// <summary>Returns the queue URL, or null when the queue does not exist.</summary>
    Task<string?> GetQueueUrlAsync(string queueName, CancellationToken cancellationToken);

    /// <summary>Returns all queue attributes, or null when the queue does not exist.</summary>
    Task<IReadOnlyDictionary<string, string>?> GetQueueAttributesAsync(
        string queueUrl, CancellationToken cancellationToken);

    /// <summary>Creates a queue and returns its URL.</summary>
    Task<string> CreateQueueAsync(
        string queueName, IReadOnlyDictionary<string, string> attributes, CancellationToken cancellationToken);

    /// <summary>Sets attributes on an existing queue.</summary>
    Task SetQueueAttributesAsync(
        string queueUrl, IReadOnlyDictionary<string, string> attributes, CancellationToken cancellationToken);

    /// <summary>Returns the topic ARN, or null when the topic does not exist.</summary>
    Task<string?> GetTopicArnAsync(string topicName, CancellationToken cancellationToken);

    /// <summary>Creates a topic and returns its ARN.</summary>
    Task<string> CreateTopicAsync(
        string topicName, IReadOnlyDictionary<string, string> attributes, CancellationToken cancellationToken);

    /// <summary>Lists all subscriptions of a topic; empty when the topic does not exist.</summary>
    Task<IReadOnlyList<SnsSubscriptionInfo>> ListSubscriptionsByTopicAsync(
        string topicArn, CancellationToken cancellationToken);

    /// <summary>Subscribes a queue to a topic and returns the subscription ARN.</summary>
    Task<string> SubscribeAsync(
        string topicArn,
        string queueArn,
        IReadOnlyDictionary<string, string> attributes,
        CancellationToken cancellationToken);

    /// <summary>Probes SQS and SNS reachability. Throws on any failure.</summary>
    Task PingAsync(CancellationToken cancellationToken);
}
