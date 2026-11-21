using System.Text.Json;
using Amazon.SQS.Model;
using Messaging.Abstractions;
using Messaging.MassTransit.AmazonSqs;
using Messaging.MassTransit.Configuration;
namespace Messaging.MassTransit.Tests;

/// <summary>Task 14.7: topology creation, idempotent restart, verify-only failure (Req 5.1, 5.3, 5.4, 5.6, 5.7, 5.9, 18.10).</summary>
[Collection(LocalStackCollection.Name)]
[Trait("Category", "Integration")]
public sealed class SnsSqsTopologyCreationIntegrationTests : IAsyncLifetime
{
    private const int MaxReceiveCount = 3;
    private const int VisibilitySeconds = 45;
    private const int RetentionSeconds = 600;
    private readonly LocalStackContainerFixture _fixture;
    private SnsSqsIntegrationHarness _harness = null!;

    public SnsSqsTopologyCreationIntegrationTests(LocalStackContainerFixture fixture) => _fixture = fixture;

    public async Task InitializeAsync() => _harness = await SnsSqsIntegrationHarness.CreateAsync(_fixture);

    public async Task DisposeAsync() => await _harness.DisposeAsync();

    [Fact]
    public async Task Start_AutoCreateEnabled_CreatesTopicQueuesSubscriptionPolicyAndRedrive()
    {
        using var cts = new CancellationTokenSource(SnsSqsIntegrationHarness.WaitTimeout);
        var bus = CreateBus("topo-create");

        await bus.StartBusAsync(cts.Token);

        var topicArn = await FindTopicArnAsync(_harness.TopicName());
        Assert.NotNull(topicArn);
        var queueName = _harness.QueueName("topo-create");
        var deadLetterName = SnsSqsNamingConvention.DeadLetterQueueName(queueName, "_error");
        var queue = await GetQueueAttributesAsync(queueName);
        var deadLetter = await GetQueueAttributesAsync(deadLetterName);

        Assert.Equal(VisibilitySeconds.ToString(), queue["VisibilityTimeout"]);
        Assert.Equal(RetentionSeconds.ToString(), queue["MessageRetentionPeriod"]);
        using (var redrive = JsonDocument.Parse(queue["RedrivePolicy"]))
        {
            Assert.Equal(deadLetter["QueueArn"], redrive.RootElement.GetProperty("deadLetterTargetArn").GetString());
            Assert.Equal(MaxReceiveCount, ReadInt(redrive.RootElement.GetProperty("maxReceiveCount")));
        }

        var subscription = Assert.Single(
            (await _harness.Sns.ListSubscriptionsByTopicAsync(topicArn)).Subscriptions);
        Assert.Equal("sqs", subscription.Protocol);
        Assert.Equal(queue["QueueArn"], subscription.Endpoint);

        AssertPolicyScopedToTopic(queue["Policy"], topicArn!);
    }

    [Fact]
    public async Task Start_SecondTimeWithSameConfiguration_LeavesTopologyUnchanged()
    {
        using var cts = new CancellationTokenSource(SnsSqsIntegrationHarness.WaitTimeout);
        var first = CreateBus("topo-twice");
        await first.StartBusAsync(cts.Token);
        var before = await SnapshotAsync("topo-twice");
        await first.StopBusAsync(cts.Token);

        var second = CreateBus("topo-twice");
        await second.StartBusAsync(cts.Token);
        var after = await SnapshotAsync("topo-twice");

        Assert.Equal(before, after);
    }

    [Fact]
    public async Task Start_VerifyOnlyWithMissingResources_FailsNamingMissingResourceAndCreatesNothing()
    {
        using var cts = new CancellationTokenSource(SnsSqsIntegrationHarness.WaitTimeout);
        var bus = CreateBus("topo-verify", s => s with { AutoCreateTopology = false });
        var queueName = _harness.QueueName("topo-verify");

        var failure = await Assert.ThrowsAsync<TopologyResourceMissingException>(() => bus.StartBusAsync(cts.Token));

        Assert.Contains(queueName, failure.MissingResources);
        Assert.Contains(_harness.TopicName(), failure.MissingResources);
        Assert.Contains(queueName, failure.Message, StringComparison.Ordinal);
        await Assert.ThrowsAsync<QueueDoesNotExistException>(() => _harness.Sqs.GetQueueUrlAsync(queueName, cts.Token));
        Assert.Null(await FindTopicArnAsync(_harness.TopicName()));
    }

    private AmazonSqsBusAdapter CreateBus(
        string endpoint, Func<AmazonSqsProviderSettings, AmazonSqsProviderSettings>? extra = null) =>
        _harness.CreateBus(
            endpoint,
            MassTransitRole.Consumer,
            MessagingMode.PublishSubscribe,
            new CapturingConsumeProcessor(),
            s =>
            {
                var configured = s with
                {
                    MaxReceiveCount = MaxReceiveCount,
                    VisibilityTimeoutSeconds = VisibilitySeconds,
                    MessageRetentionSeconds = RetentionSeconds,
                };
                return extra is null ? configured : extra(configured);
            });

    private async Task<string?> FindTopicArnAsync(string topicName)
    {
        string? token = null;
        do
        {
            var page = await _harness.Sns.ListTopicsAsync(token);
            var match = page.Topics.FirstOrDefault(t => t.TopicArn.EndsWith(":" + topicName, StringComparison.Ordinal));
            if (match is not null)
            {
                return match.TopicArn;
            }

            token = page.NextToken;
        }
        while (!string.IsNullOrEmpty(token));

        return null;
    }

    private async Task<Dictionary<string, string>> GetQueueAttributesAsync(string queueName)
    {
        var url = (await _harness.Sqs.GetQueueUrlAsync(queueName)).QueueUrl;
        var response = await _harness.Sqs.GetQueueAttributesAsync(url, new List<string> { "All" });
        return new Dictionary<string, string>(response.Attributes, StringComparer.Ordinal);
    }

    private async Task<string> SnapshotAsync(string endpoint)
    {
        var queueName = _harness.QueueName(endpoint);
        var deadLetterName = SnsSqsNamingConvention.DeadLetterQueueName(queueName, "_error");
        var topicArn = await FindTopicArnAsync(_harness.TopicName());
        var subscriptions = (await _harness.Sns.ListSubscriptionsByTopicAsync(topicArn))
            .Subscriptions.Select(s => $"{s.SubscriptionArn}|{s.Protocol}|{s.Endpoint}").OrderBy(s => s, StringComparer.Ordinal);
        var parts = new List<string> { topicArn!, string.Join(";", subscriptions) };
        foreach (var name in new[] { queueName, deadLetterName })
        {
            var attributes = await GetQueueAttributesAsync(name);
            foreach (var key in new[] { "QueueArn", "VisibilityTimeout", "MessageRetentionPeriod", "RedrivePolicy", "Policy" })
            {
                parts.Add($"{name}.{key}={(attributes.TryGetValue(key, out var value) ? value : "<none>")}");
            }
        }

        return string.Join("\n", parts);
    }

    private static int ReadInt(JsonElement element) =>
        element.ValueKind == JsonValueKind.String ? int.Parse(element.GetString()!) : element.GetInt32();

    private static void AssertPolicyScopedToTopic(string policy, string topicArn)
    {
        using var document = JsonDocument.Parse(policy);
        var statements = document.RootElement.GetProperty("Statement").EnumerateArray().ToArray();
        var statement = Assert.Single(statements, s => ConditionSourceArn(s) == topicArn);
        Assert.Equal("Allow", statement.GetProperty("Effect").GetString());
        Assert.Contains("sqs:SendMessage", statement.GetProperty("Action").ToString());
        foreach (var each in statements)
        {
            var principal = each.GetProperty("Principal");
            if (principal.ValueKind == JsonValueKind.String)
            {
                Assert.NotEqual("*", principal.GetString());
                continue;
            }

            foreach (var property in principal.EnumerateObject())
            {
                Assert.DoesNotContain("*", property.Value.ToString());
            }
        }
    }

    private static string? ConditionSourceArn(JsonElement statement) =>
        statement.TryGetProperty("Condition", out var condition)
        && condition.TryGetProperty("ArnEquals", out var arnEquals)
        && arnEquals.TryGetProperty("aws:SourceArn", out var arn)
            ? arn.ToString()
            : null;
}
