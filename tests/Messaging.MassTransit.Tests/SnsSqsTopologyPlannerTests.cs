using System;
using System.Linq;
using Messaging.Abstractions;
using Messaging.Consuming;
using Messaging.MassTransit.AmazonSqs;
using Messaging.MassTransit.Configuration;
using Xunit;

namespace Messaging.MassTransit.Tests;

/// <summary>
/// Unit tests for <see cref="SnsSqsTopologyPlanner"/>. Requirements 11.1, 11.3, 11.5, 11.6, 14.8.
/// </summary>
[Trait("Feature", "sns-sqs-provider")]
public class SnsSqsTopologyPlannerTests
{
    private sealed record OrderPlaced;

    private sealed record OrderShipped;

    private static AmazonSqsProviderSettings Settings(
        bool fifo = false, string? kmsKeyId = null) => new()
        {
            Region = "us-east-1",
            EndpointName = "orders",
            Fifo = fifo,
            KmsMasterKeyId = kmsKeyId,
        };

    private static ContractRegistration[] Contracts(params string[] messageTypes) =>
        messageTypes.Select(t => new ContractRegistration(t, typeof(OrderPlaced))).ToArray();

    [Fact]
    public void Plan_PublisherRole_ReturnsEmptyPlan()
    {
        var plan = SnsSqsTopologyPlanner.Plan(
            Settings(), MessagingMode.PublishSubscribe, MassTransitRole.Publisher, Contracts("a.b.One"));

        Assert.Empty(plan.Queues);
        Assert.Empty(plan.Topics);
        Assert.Empty(plan.Subscriptions);
    }

    [Theory]
    [InlineData(MassTransitRole.Consumer)]
    [InlineData(MassTransitRole.Both)]
    public void Plan_ConsumerOrBothRole_CreatesQueueAndDeadLetterQueue(MassTransitRole role)
    {
        var plan = SnsSqsTopologyPlanner.Plan(
            Settings(), MessagingMode.PublishSubscribe, role, Contracts("a.b.One"));

        Assert.Equal(2, plan.Queues.Count);
        var dlq = Assert.Single(plan.Queues, q => q.IsDeadLetter);
        var queue = Assert.Single(plan.Queues, q => !q.IsDeadLetter);
        Assert.Equal("orders", queue.Name);
        Assert.Equal(dlq.Name, queue.DeadLetterTarget);
        Assert.Equal(AmazonSqsProviderSettings.DefaultMaxReceiveCount, queue.MaxReceiveCount);
        Assert.Null(dlq.MaxReceiveCount);
        Assert.Equal(SnsSqsTopologyPlanner.DeadLetterRetentionSeconds, dlq.RetentionSeconds);
    }

    [Fact]
    public void Plan_OneQueueSubscribedToManyTopics_CreatesOneTopicAndSubscriptionPerContract()
    {
        var plan = SnsSqsTopologyPlanner.Plan(
            Settings(),
            MessagingMode.PublishSubscribe,
            MassTransitRole.Consumer,
            Contracts("a.b.One", "a.b.Two", "a.b.Three"));

        var queue = Assert.Single(plan.Queues, q => !q.IsDeadLetter);
        Assert.Equal(3, plan.Topics.Count);
        Assert.Equal(3, plan.Subscriptions.Count);
        Assert.All(plan.Subscriptions, s => Assert.Equal(queue.Name, s.QueueName));
        Assert.Equal(
            plan.Topics.Select(t => t.Name).OrderBy(n => n, StringComparer.Ordinal),
            plan.Subscriptions.Select(s => s.TopicName).OrderBy(n => n, StringComparer.Ordinal));
    }

    [Fact]
    public void Plan_DuplicateMessageTypes_CollapseToOneTopicAndSubscription()
    {
        var plan = SnsSqsTopologyPlanner.Plan(
            Settings(),
            MessagingMode.PublishSubscribe,
            MassTransitRole.Both,
            new[]
            {
                new ContractRegistration("a.b.One", typeof(OrderPlaced)),
                new ContractRegistration("a.b.One", typeof(OrderShipped)),
            });

        Assert.Single(plan.Topics);
        Assert.Single(plan.Subscriptions);
    }

    [Fact]
    public void Plan_NoContracts_CreatesQueuesOnly()
    {
        var plan = SnsSqsTopologyPlanner.Plan(
            Settings(), MessagingMode.PublishSubscribe, MassTransitRole.Consumer, Contracts());

        Assert.Equal(2, plan.Queues.Count);
        Assert.Empty(plan.Topics);
        Assert.Empty(plan.Subscriptions);
    }

    [Fact]
    public void Plan_MessageQueueMode_HasQueuesButNoTopicsOrSubscriptions()
    {
        var plan = SnsSqsTopologyPlanner.Plan(
            Settings(), MessagingMode.MessageQueue, MassTransitRole.Both, Contracts("a.b.One", "a.b.Two"));

        Assert.Equal(2, plan.Queues.Count);
        Assert.Empty(plan.Topics);
        Assert.Empty(plan.Subscriptions);
    }

    [Fact]
    public void Plan_FifoEnabled_MarksEveryTopicAndQueueFifo()
    {
        var plan = SnsSqsTopologyPlanner.Plan(
            Settings(fifo: true),
            MessagingMode.PublishSubscribe,
            MassTransitRole.Consumer,
            Contracts("a.b.One", "a.b.Two"));

        Assert.All(plan.Queues, q => Assert.True(q.Fifo));
        Assert.All(plan.Queues, q => Assert.EndsWith(".fifo", q.Name, StringComparison.Ordinal));
        Assert.All(plan.Topics, t => Assert.True(t.Fifo));
        Assert.All(plan.Topics, t => Assert.EndsWith(".fifo", t.Name, StringComparison.Ordinal));
    }

    [Fact]
    public void Plan_FifoDisabled_MarksNothingFifo()
    {
        var plan = SnsSqsTopologyPlanner.Plan(
            Settings(), MessagingMode.PublishSubscribe, MassTransitRole.Consumer, Contracts("a.b.One"));

        Assert.All(plan.Queues, q => Assert.False(q.Fifo));
        Assert.All(plan.Topics, t => Assert.False(t.Fifo));
    }

    [Fact]
    public void Plan_KmsKeyConfigured_CarriesKeyIntoEveryQueueAndTopic()
    {
        const string key = "alias/messaging-test";

        var plan = SnsSqsTopologyPlanner.Plan(
            Settings(kmsKeyId: key),
            MessagingMode.PublishSubscribe,
            MassTransitRole.Consumer,
            Contracts("a.b.One", "a.b.Two"));

        Assert.All(plan.Queues, q => Assert.Equal(key, q.KmsKeyId));
        Assert.All(plan.Topics, t => Assert.Equal(key, t.KmsKeyId));
    }

    [Fact]
    public void Plan_NoKmsKey_LeavesKeyNull()
    {
        var plan = SnsSqsTopologyPlanner.Plan(
            Settings(), MessagingMode.PublishSubscribe, MassTransitRole.Consumer, Contracts("a.b.One"));

        Assert.All(plan.Queues, q => Assert.Null(q.KmsKeyId));
        Assert.All(plan.Topics, t => Assert.Null(t.KmsKeyId));
    }

    [Fact]
    public void Plan_NullArguments_Throw()
    {
        Assert.Throws<ArgumentNullException>(() => SnsSqsTopologyPlanner.Plan(
            null!, MessagingMode.PublishSubscribe, MassTransitRole.Consumer, Contracts()));
        Assert.Throws<ArgumentNullException>(() => SnsSqsTopologyPlanner.Plan(
            Settings(), MessagingMode.PublishSubscribe, MassTransitRole.Consumer, null!));
    }
}
