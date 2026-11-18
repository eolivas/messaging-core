using Amazon;
using Amazon.Runtime;
using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using Amazon.SQS;
using Amazon.SQS.Model;
using global::MassTransit;

namespace Messaging.MassTransit.Tests;

/// <summary>
/// Spike R5 (task 1.3): characterizes AWS SDK v4 (SQS 4.0.100.14, SNS 4.0.100.15) and the MassTransit
/// Amazon SQS transport (8.5.11) against LocalStack.
///
/// OUTCOME (LocalStack 4.14.0, SQS 4.0.100.14, SNS 4.0.100.15, MassTransit.AmazonSQS 8.5.11): PASS.
/// R5 is not a blocker; no SDK v3 fallback needed.
/// Worked with raw clients (BasicAWSCredentials test/test, ServiceURL + AuthenticationRegion):
///  - SNS CreateTopic, SQS CreateQueue, GetQueueAttributes(["QueueArn"]) -> QueueARN,
///    SetQueueAttributes {"Policy": json} (sns.amazonaws.com + ArnEquals aws:SourceArn),
///    Subscribe(Protocol "sqs", Attributes RawMessageDelivery=true), Publish with a message attribute,
///    ReceiveMessage(MessageAttributeNames ["All"]) with the attribute preserved, DeleteMessage,
///    DeleteTopic, DeleteQueue.
///  - MassTransit: Bus.Factory.CreateUsingAmazonSqs, cfg.Host(region, h => AccessKey/SecretKey/
///    Config(AmazonSQSConfig)/Config(AmazonSimpleNotificationServiceConfig)), ReceiveEndpoint + Handler,
///    GetSendEndpoint("queue:name").Send, consumed once.
/// Note: SQS and SNS both define MessageAttributeValue; qualify the SNS one.
/// Environment note: on this macOS Docker Desktop host Testcontainers needed
/// DOCKER_HOST=unix:///var/run/docker.sock (the ~/.docker/run socket fails the Ryuk bind mount).
/// </summary>
[Collection(LocalStackCollection.Name)]
[Trait("Category", "Integration")]
public sealed class AmazonSqsSpikeR5Tests
{
    private readonly LocalStackContainerFixture _fixture;

    public AmazonSqsSpikeR5Tests(LocalStackContainerFixture fixture) => _fixture = fixture;

    public sealed record SpikeR5Message
    {
        public string Text { get; init; } = string.Empty;
    }

    private static string Prefix() => "spk5-" + Guid.NewGuid().ToString("N")[..8];

    [Fact]
    public async Task Raw_sdk_v4_topology_publish_receive_delete_round_trip()
    {
        var url = await _fixture.GetServiceUrlAsync();
        var creds = new BasicAWSCredentials(
            LocalStackContainerFixture.AccessKeyId, LocalStackContainerFixture.SecretAccessKey);
        using var sqs = new AmazonSQSClient(creds, new AmazonSQSConfig
        {
            ServiceURL = url,
            AuthenticationRegion = LocalStackContainerFixture.Region,
        });
        using var sns = new AmazonSimpleNotificationServiceClient(creds,
            new AmazonSimpleNotificationServiceConfig
            {
                ServiceURL = url,
                AuthenticationRegion = LocalStackContainerFixture.Region,
            });

        var prefix = Prefix();
        var topicArn = (await sns.CreateTopicAsync(prefix + "-topic")).TopicArn;
        var queueUrl = (await sqs.CreateQueueAsync(prefix + "-queue")).QueueUrl;
        var attrs = await sqs.GetQueueAttributesAsync(queueUrl, new List<string> { "QueueArn" });
        var queueArn = attrs.QueueARN;
        Assert.False(string.IsNullOrEmpty(queueArn));

        var policy = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\","
            + "\"Principal\":{\"Service\":\"sns.amazonaws.com\"},\"Action\":\"sqs:SendMessage\","
            + $"\"Resource\":\"{queueArn}\",\"Condition\":{{\"ArnEquals\":{{\"aws:SourceArn\":\"{topicArn}\"}}}}}}]}}";
        await sqs.SetQueueAttributesAsync(new SetQueueAttributesRequest
        {
            QueueUrl = queueUrl,
            Attributes = new Dictionary<string, string> { ["Policy"] = policy },
        });

        var sub = await sns.SubscribeAsync(new SubscribeRequest
        {
            TopicArn = topicArn,
            Protocol = "sqs",
            Endpoint = queueArn,
            Attributes = new Dictionary<string, string> { ["RawMessageDelivery"] = "true" },
        });
        Assert.False(string.IsNullOrEmpty(sub.SubscriptionArn));

        await sns.PublishAsync(new PublishRequest
        {
            TopicArn = topicArn,
            Message = "hello",
            MessageAttributes = new Dictionary<string, Amazon.SimpleNotificationService.Model.MessageAttributeValue>
            {
                ["x-correlation-id"] = new() { DataType = "String", StringValue = "abc" },
            },
        });

        var received = new List<Message>();
        for (var i = 0; i < 10 && received.Count == 0; i++)
        {
            var r = await sqs.ReceiveMessageAsync(new ReceiveMessageRequest
            {
                QueueUrl = queueUrl,
                WaitTimeSeconds = 2,
                MaxNumberOfMessages = 1,
                MessageAttributeNames = new List<string> { "All" },
            });
            received.AddRange(r.Messages ?? new List<Message>());
        }

        var msg = Assert.Single(received);
        Assert.Equal("hello", msg.Body);
        Assert.Equal("abc", msg.MessageAttributes["x-correlation-id"].StringValue);
        await sqs.DeleteMessageAsync(queueUrl, msg.ReceiptHandle);

        await sns.DeleteTopicAsync(topicArn);
        await sqs.DeleteQueueAsync(queueUrl);
    }

    [Fact]
    public async Task MassTransit_amazon_bus_sends_and_consumes_one_message()
    {
        var url = await _fixture.GetServiceUrlAsync();
        var prefix = Prefix();
        var queueName = prefix + "-mt-queue";
        var received = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);

        var bus = Bus.Factory.CreateUsingAmazonSqs(cfg =>
        {
            cfg.Host(LocalStackContainerFixture.Region, h =>
            {
                h.AccessKey(LocalStackContainerFixture.AccessKeyId);
                h.SecretKey(LocalStackContainerFixture.SecretAccessKey);
                h.Config(new AmazonSQSConfig
                {
                    ServiceURL = url,
                    AuthenticationRegion = LocalStackContainerFixture.Region,
                });
                h.Config(new AmazonSimpleNotificationServiceConfig
                {
                    ServiceURL = url,
                    AuthenticationRegion = LocalStackContainerFixture.Region,
                });
            });
            cfg.ReceiveEndpoint(queueName, e =>
            {
                e.Handler<SpikeR5Message>(ctx =>
                {
                    received.TrySetResult(ctx.Message.Text);
                    return Task.CompletedTask;
                });
            });
        });

        using var startCts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        await bus.StartAsync(startCts.Token);
        try
        {
            var endpoint = await bus.GetSendEndpoint(new Uri($"queue:{queueName}"));
            await endpoint.Send(new SpikeR5Message { Text = "mt-hello" }, startCts.Token);

            var winner = await Task.WhenAny(received.Task, Task.Delay(TimeSpan.FromSeconds(30)));
            Assert.Same(received.Task, winner);
            Assert.Equal("mt-hello", await received.Task);
        }
        finally
        {
            await bus.StopAsync();
        }
    }
}
