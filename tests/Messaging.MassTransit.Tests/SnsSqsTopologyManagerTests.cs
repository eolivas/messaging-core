using System.Text.Json;
using Messaging.MassTransit.AmazonSqs;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Messaging.MassTransit.Tests;

/// <summary>
/// Unit tests for <see cref="SnsSqsTopologyManager"/> over <see cref="FakeSnsSqsTopologyClient"/>.
/// Requirements 5.4, 5.5, 5.8, 5.10, 8.7, 14.8, 17.6.
/// </summary>
[Trait("Feature", "sns-sqs-provider")]
public class SnsSqsTopologyManagerTests
{
    private const string Kms = "alias/messaging-key";

    private static SnsSqsTopologyManager NewManager(FakeSnsSqsTopologyClient client) =>
        new(
            client,
            new AmazonSqsProviderSettings { Region = "us-east-1", EndpointName = "orders" },
            NullLogger<SnsSqsTopologyManager>.Instance);

    private static QueueSpec Dlq(string name = "orders-dlq", bool fifo = false, string? kms = null) =>
        new(name, true, null, null, 30, 60, fifo, kms);

    private static QueueSpec MainQueue(
        string name = "orders", bool fifo = false, string? kms = null, int visibility = 45, int retention = 3600) =>
        new(name, false, "orders-dlq", 7, visibility, retention, fifo, kms);

    private static TopologyPlan QueuePlan(params QueueSpec[] queues) =>
        new(Array.Empty<TopicSpec>(), queues, Array.Empty<SubscriptionSpec>());

    private static IReadOnlyDictionary<string, string> CreatedAttributes(
        FakeSnsSqsTopologyClient client, string queueName) =>
        client.SnapshotQueueAttributes(FakeSnsSqsTopologyClient.QueueUrlFor(queueName))
        ?? throw new InvalidOperationException($"Queue {queueName} was not created.");

    [Fact]
    public async Task Apply_CreatesQueue_MapsVisibilityAndRetention()
    {
        var client = new FakeSnsSqsTopologyClient();

        await NewManager(client).ApplyAsync(QueuePlan(Dlq(), MainQueue()), true, CancellationToken.None);

        var attrs = CreatedAttributes(client, "orders");
        Assert.Equal("45", attrs["VisibilityTimeout"]);
        Assert.Equal("3600", attrs["MessageRetentionPeriod"]);
        Assert.False(attrs.ContainsKey("KmsMasterKeyId"));
        Assert.False(attrs.ContainsKey("FifoQueue"));
    }

    [Fact]
    public async Task Apply_DeadLetterQueue_UsesMaximumRetention()
    {
        var client = new FakeSnsSqsTopologyClient();

        await NewManager(client).ApplyAsync(QueuePlan(Dlq(), MainQueue()), true, CancellationToken.None);

        var attrs = CreatedAttributes(client, "orders-dlq");
        Assert.Equal("1209600", attrs["MessageRetentionPeriod"]);
        Assert.Equal("30", attrs["VisibilityTimeout"]);
        Assert.False(attrs.ContainsKey("RedrivePolicy"));
    }

    [Fact]
    public async Task Apply_MainQueue_RedrivePolicyTargetsDeadLetterQueueArn()
    {
        var client = new FakeSnsSqsTopologyClient();

        await NewManager(client).ApplyAsync(QueuePlan(Dlq(), MainQueue()), true, CancellationToken.None);

        var attrs = CreatedAttributes(client, "orders");
        using var doc = JsonDocument.Parse(attrs["RedrivePolicy"]);
        Assert.Equal(
            FakeSnsSqsTopologyClient.QueueArnFor("orders-dlq"),
            doc.RootElement.GetProperty("deadLetterTargetArn").GetString());
        Assert.Equal(7, doc.RootElement.GetProperty("maxReceiveCount").GetInt32());
    }

    [Fact]
    public async Task Apply_KmsKeyConfigured_SetsKmsMasterKeyIdOnQueuesAndTopics()
    {
        var client = new FakeSnsSqsTopologyClient();
        var plan = new TopologyPlan(
            new[] { new TopicSpec("orders-topic", false, Kms) },
            new[] { Dlq(kms: Kms), MainQueue(kms: Kms) },
            Array.Empty<SubscriptionSpec>());

        await NewManager(client).ApplyAsync(plan, true, CancellationToken.None);

        Assert.Equal(Kms, CreatedAttributes(client, "orders")["KmsMasterKeyId"]);
        Assert.Equal(Kms, CreatedAttributes(client, "orders-dlq")["KmsMasterKeyId"]);
        Assert.Contains(client.Calls, c => c == "CreateTopic");
    }

    [Fact]
    public async Task Apply_FifoQueue_SetsFifoQueueAttribute()
    {
        var client = new FakeSnsSqsTopologyClient();

        await NewManager(client).ApplyAsync(
            QueuePlan(Dlq("orders-dlq.fifo", true), MainQueue("orders.fifo", true) with { DeadLetterTarget = "orders-dlq.fifo" }),
            true,
            CancellationToken.None);

        Assert.Equal("true", CreatedAttributes(client, "orders.fifo")["FifoQueue"]);
    }

    [Fact]
    public async Task Apply_AllResourcesPresent_PerformsNoMutation()
    {
        var client = new FakeSnsSqsTopologyClient();
        var topicArn = client.PresetTopic("orders-topic");
        client.PresetQueue("orders-dlq");
        client.PresetQueue("orders");
        var plan = new TopologyPlan(
            new[] { new TopicSpec("orders-topic", false, null) },
            new[] { Dlq(), MainQueue() },
            new[] { new SubscriptionSpec("orders-topic", "orders", true) });
        client.PresetSubscription(topicArn, FakeSnsSqsTopologyClient.QueueArnFor("orders"));
        var policy = QueuePolicyDocument.Merge(null, FakeSnsSqsTopologyClient.QueueArnFor("orders"), new[] { topicArn });
        client.PresetQueueAttributes(
            FakeSnsSqsTopologyClient.QueueUrlFor("orders"),
            new Dictionary<string, string>
            {
                ["QueueArn"] = FakeSnsSqsTopologyClient.QueueArnFor("orders"),
                ["Policy"] = policy,
            });

        await NewManager(client).ApplyAsync(plan, true, CancellationToken.None);

        Assert.Equal(0, client.MutationCount);
    }

    [Fact]
    public async Task Apply_ExistingQueueWithDifferentSettings_IsNotModified()
    {
        var client = new FakeSnsSqsTopologyClient();
        client.PresetQueue("orders-dlq");
        var url = client.PresetQueue("orders", new Dictionary<string, string> { ["VisibilityTimeout"] = "5" });

        await NewManager(client).ApplyAsync(QueuePlan(Dlq(), MainQueue()), true, CancellationToken.None);

        Assert.Equal("5", client.SnapshotQueueAttributes(url)!["VisibilityTimeout"]);
        Assert.Equal(0, client.MutationCount);
    }

    [Fact]
    public async Task Apply_AuthorizationFailure_NamesOperationAndResourceWithoutCredentials()
    {
        const string secret = "AKIAIOSFODNN7EXAMPLE";
        var client = new FakeSnsSqsTopologyClient();
        var denied = new UnauthorizedAccessException($"AccessDenied for {secret} secretkey=wJalrXUtnFEMI");
        client.FailOn(SnsSqsTopologyOperation.CreateQueue, denied, "orders-dlq");

        var ex = await Assert.ThrowsAsync<TopologyOperationFailedException>(() =>
            NewManager(client).ApplyAsync(QueuePlan(Dlq(), MainQueue()), true, CancellationToken.None));

        Assert.Equal("sqs:CreateQueue", ex.Operation);
        Assert.Equal("orders-dlq", ex.Resource);
        Assert.Contains("sqs:CreateQueue", ex.Message);
        Assert.Contains("orders-dlq", ex.Message);
        Assert.DoesNotContain(secret, ex.Message);
        Assert.DoesNotContain("secretkey", ex.Message);
        Assert.Same(denied, ex.InnerException);
    }

    [Fact]
    public async Task Apply_TopicLookupDenied_NamesSnsOperationAndTopic()
    {
        var client = new FakeSnsSqsTopologyClient();
        client.FailOn(SnsSqsTopologyOperation.GetTopicArn, new UnauthorizedAccessException("denied"));
        var plan = new TopologyPlan(
            new[] { new TopicSpec("orders-topic", false, null) },
            Array.Empty<QueueSpec>(),
            Array.Empty<SubscriptionSpec>());

        var ex = await Assert.ThrowsAsync<TopologyOperationFailedException>(() =>
            NewManager(client).ApplyAsync(plan, true, CancellationToken.None));

        Assert.Equal("sns:GetTopicAttributes", ex.Operation);
        Assert.Equal("orders-topic", ex.Resource);
    }

    [Fact]
    public async Task Apply_ExistingStandardQueueWhenFifoPlanned_ThrowsTopologyConflict()
    {
        var client = new FakeSnsSqsTopologyClient();
        client.PresetQueue("orders.fifo");

        var ex = await Assert.ThrowsAsync<TopologyConflictException>(() =>
            NewManager(client).ApplyAsync(
                QueuePlan(MainQueue("orders.fifo", true) with { DeadLetterTarget = null, MaxReceiveCount = null }),
                true,
                CancellationToken.None));

        Assert.Equal("orders.fifo", ex.Resource);
        Assert.Equal("Standard", ex.ExistingType);
        Assert.Equal(0, client.MutationCount);
    }

    [Fact]
    public async Task Apply_ExistingFifoQueueWhenStandardPlanned_ThrowsTopologyConflict()
    {
        var client = new FakeSnsSqsTopologyClient();
        client.PresetQueue("orders", new Dictionary<string, string> { ["FifoQueue"] = "true" });

        var ex = await Assert.ThrowsAsync<TopologyConflictException>(() =>
            NewManager(client).ApplyAsync(
                QueuePlan(MainQueue() with { DeadLetterTarget = null, MaxReceiveCount = null }),
                true,
                CancellationToken.None));

        Assert.Equal("FIFO", ex.ExistingType);
    }

    [Fact]
    public async Task Apply_Subscription_QueuePolicyScopedToSnsServicePrincipalAndSourceArn()
    {
        var client = new FakeSnsSqsTopologyClient();
        var plan = new TopologyPlan(
            new[] { new TopicSpec("orders-topic", false, null) },
            new[] { Dlq(), MainQueue() },
            new[] { new SubscriptionSpec("orders-topic", "orders", true) });

        await NewManager(client).ApplyAsync(plan, true, CancellationToken.None);

        var policy = CreatedAttributes(client, "orders")["Policy"];
        Assert.DoesNotContain("\"*\"", policy);
        using var doc = JsonDocument.Parse(policy);
        var statement = Assert.Single(doc.RootElement.GetProperty("Statement").EnumerateArray());
        Assert.Equal("Allow", statement.GetProperty("Effect").GetString());
        Assert.Equal("sns.amazonaws.com", statement.GetProperty("Principal").GetProperty("Service").GetString());
        Assert.Equal("sqs:SendMessage", statement.GetProperty("Action").GetString());
        Assert.Equal(FakeSnsSqsTopologyClient.QueueArnFor("orders"), statement.GetProperty("Resource").GetString());
        Assert.Equal(
            FakeSnsSqsTopologyClient.TopicArnFor("orders-topic"),
            statement.GetProperty("Condition").GetProperty("ArnEquals").GetProperty("aws:SourceArn").GetString());
    }
}
