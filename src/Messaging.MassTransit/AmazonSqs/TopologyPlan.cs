namespace Messaging.MassTransit.AmazonSqs;

/// <summary>The desired SNS/SQS topology for one provider instance.</summary>
/// <param name="Topics">Topics to ensure.</param>
/// <param name="Queues">Queues to ensure, dead-letter queues first.</param>
/// <param name="Subscriptions">Queue-to-topic subscriptions to ensure.</param>
public sealed record TopologyPlan(
    IReadOnlyList<TopicSpec> Topics,
    IReadOnlyList<QueueSpec> Queues,
    IReadOnlyList<SubscriptionSpec> Subscriptions)
{
    /// <summary>A plan that provisions nothing (publisher role).</summary>
    public static TopologyPlan Empty { get; } = new(
        Array.Empty<TopicSpec>(), Array.Empty<QueueSpec>(), Array.Empty<SubscriptionSpec>());
}
