using Messaging.Abstractions;
using Messaging.MassTransit.AmazonSqs;
using Messaging.MassTransit.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
namespace Messaging.MassTransit.Tests;

/// <summary>Task 14.6: the SQS redrive policy moves an over-received message to the DLQ (Req 10.10, 18.9).</summary>
[Collection(LocalStackCollection.Name)]
[Trait("Category", "Integration")]
public sealed class SnsSqsRedriveIntegrationTests : IAsyncLifetime
{
    private const int MaxReceiveCount = 2;
    private readonly LocalStackContainerFixture _fixture;
    private SnsSqsIntegrationHarness _harness = null!;

    public SnsSqsRedriveIntegrationTests(LocalStackContainerFixture fixture) => _fixture = fixture;

    public async Task InitializeAsync() => _harness = await SnsSqsIntegrationHarness.CreateAsync(_fixture);

    public async Task DisposeAsync() => await _harness.DisposeAsync();

    [Fact]
    public async Task Receive_MoreThanMaxReceiveCountWithoutDelete_MessageAppearsInDeadLetterQueue()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        var settings = _harness.CreateSettings(
            "redrive", s => s with { MaxReceiveCount = MaxReceiveCount, VisibilityTimeoutSeconds = 1 });
        var plan = SnsSqsTopologyPlanner.Plan(
            settings,
            MessagingMode.MessageQueue,
            MassTransitRole.Consumer,
            new[] { new Messaging.Consuming.ContractRegistration(
                SnsSqsIntegrationHarness.DefaultMessageType, typeof(SnsSqsIntegrationHarness.HarnessContract)) });
        var topology = new SnsSqsTopologyManager(
            new AwsSnsSqsTopologyClient(_harness.Sqs, _harness.Sns), settings, NullLogger<SnsSqsTopologyManager>.Instance);
        await topology.ApplyAsync(plan, autoCreate: true, cts.Token);

        var queueName = _harness.QueueName("redrive");
        var deadLetterName = SnsSqsNamingConvention.DeadLetterQueueName(queueName, settings.DeadLetterQueueSuffix);
        var queueUrl = (await _harness.Sqs.GetQueueUrlAsync(queueName, cts.Token)).QueueUrl;
        var body = "redrive-" + Guid.NewGuid().ToString("N");
        await _harness.Sqs.SendMessageAsync(queueUrl, body, cts.Token);

        // Receive MaxReceiveCount times and let the visibility timeout lapse each time (no delete).
        for (var receive = 1; receive <= MaxReceiveCount; receive++)
        {
            var received = await _harness.ReceiveRawAsync(queueName, TimeSpan.FromSeconds(15));
            Assert.NotNull(received);
            Assert.Equal(body, received!.Body);
            Assert.Equal(receive.ToString(), received.Attributes["ApproximateReceiveCount"]);
            await Task.Delay(TimeSpan.FromSeconds(1.5), cts.Token);
        }

        // The next receive attempt exceeds the maximum, so SQS redrives instead of delivering.
        var overLimit = await _harness.ReceiveRawAsync(queueName, TimeSpan.FromSeconds(5));
        Assert.Null(overLimit);

        var deadLettered = await _harness.ReceiveRawAsync(deadLetterName, TimeSpan.FromSeconds(20));
        Assert.NotNull(deadLettered);
        Assert.Equal(body, deadLettered!.Body);
    }
}
