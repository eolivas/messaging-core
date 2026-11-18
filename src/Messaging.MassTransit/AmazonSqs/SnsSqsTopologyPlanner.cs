using Messaging.Abstractions;
using Messaging.Consuming;
using Messaging.MassTransit.Configuration;

namespace Messaging.MassTransit.AmazonSqs;

/// <summary>
/// Pure derivation of the desired <see cref="TopologyPlan"/>. MassTransit creates no auxiliary queues
/// eagerly (spike R2), so the plan holds only the consumer queue, its dead-letter queue, topics,
/// and subscriptions.
/// </summary>
public static class SnsSqsTopologyPlanner
{
    /// <summary>The maximum retention SQS allows, used for dead-letter queues.</summary>
    public const int DeadLetterRetentionSeconds = 1_209_600;

    /// <summary>Builds the plan.</summary>
    /// <param name="settings">Provider settings.</param>
    /// <param name="mode">The messaging mode.</param>
    /// <param name="role">The bus role.</param>
    /// <param name="contracts">The registered contracts.</param>
    /// <returns>The desired topology.</returns>
    /// <exception cref="ResourceNameTooLongException">A derived name exceeds its limit.</exception>
    public static TopologyPlan Plan(
        AmazonSqsProviderSettings settings,
        MessagingMode mode,
        MassTransitRole role,
        IEnumerable<ContractRegistration> contracts)
    {
        ArgumentNullException.ThrowIfNull(settings);
        ArgumentNullException.ThrowIfNull(contracts);

        if (role == MassTransitRole.Publisher)
        {
            return TopologyPlan.Empty;
        }

        var queueName = SnsSqsNamingConvention.QueueName(settings.EndpointName, settings);
        var deadLetterName = SnsSqsNamingConvention.DeadLetterQueueName(queueName, settings.DeadLetterQueueSuffix);

        var queues = new[]
        {
            new QueueSpec(
                deadLetterName,
                IsDeadLetter: true,
                DeadLetterTarget: null,
                MaxReceiveCount: null,
                settings.VisibilityTimeoutSeconds,
                DeadLetterRetentionSeconds,
                settings.Fifo,
                settings.KmsMasterKeyId),
            new QueueSpec(
                queueName,
                IsDeadLetter: false,
                deadLetterName,
                settings.MaxReceiveCount,
                settings.VisibilityTimeoutSeconds,
                settings.MessageRetentionSeconds,
                settings.Fifo,
                settings.KmsMasterKeyId),
        };

        if (mode != MessagingMode.PublishSubscribe)
        {
            return new TopologyPlan(Array.Empty<TopicSpec>(), queues, Array.Empty<SubscriptionSpec>());
        }

        var topicNames = contracts
            .Select(c => SnsSqsNamingConvention.TopicName(c.MessageType, settings))
            .Distinct(StringComparer.Ordinal)
            .ToArray();

        return new TopologyPlan(
            topicNames.Select(t => new TopicSpec(t, settings.Fifo, settings.KmsMasterKeyId)).ToArray(),
            queues,
            topicNames.Select(t => new SubscriptionSpec(t, queueName, settings.RawMessageDelivery)).ToArray());
    }
}
