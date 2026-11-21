using Amazon;
using Amazon.Runtime;
using Amazon.SQS;
using Amazon.SQS.Model;
using MassTransit;
using MassTransit.Middleware;

namespace Messaging.MassTransit.Tests;

/// <summary>
/// Spike R3 (task 1.5): a receive endpoint with <c>UseMessageRetry(r =&gt; r.None())</c> and the error
/// pipe replaced by <see cref="RethrowErrorTransportFilter"/> must leave a faulted message undeleted
/// (redelivered after the visibility timeout, dead-lettered by SQS redrive after maxReceiveCount),
/// while a consumer that returns normally deletes it. Requirements 1.9, 10.8, 10.9, 10.10.
/// </summary>
/// <remarks>
/// FINDINGS (MassTransit 8.5.11, MassTransit.AmazonSQS 8.5.11, LocalStack):
/// Endpoint shape that compiles and works (same as design key decision 4):
///   cfg.ReceiveEndpoint(queueName, e =>
///   {
///       e.ConfigureConsumeTopology = false;
///       e.UseMessageRetry(r => r.None());
///       e.ConfigureError(x => x.UseFilter(new RethrowErrorTransportFilter()));
///       e.Consumer(() => new Consumer());
///   });
/// Host: cfg.Host("us-east-1", h => { h.AccessKey(..); h.SecretKey(..);
///   h.Config(new AmazonSQSConfig { ServiceURL, AuthenticationRegion });
///   h.Config(new AmazonSimpleNotificationServiceConfig { ServiceURL, AuthenticationRegion }); });
/// Only correction to the design: RethrowErrorTransportFilter lives in namespace
/// MassTransit.Middleware (assembly MassTransit.dll), so `using MassTransit.Middleware;` is needed.
/// No AmazonSQS-specific API is involved; ConfigureError/UseFilter are core receive-endpoint APIs.
/// Observed on LocalStack (queue VisibilityTimeout=2s, RedrivePolicy maxReceiveCount=3):
///  - throwing consumer: the message is not deleted; it is redelivered after about the visibility
///    timeout (gap of 1s or more between receives); after exactly 3 receives SQS redrive moves it to
///    the DLQ; main queue ends empty; MassTransit created no `_error` queue and did not delete
///    or move the message itself.
///  - returning consumer: one receive, deleted (main queue and DLQ empty after the timeout).
/// The queue and redrive policy are pre-created through the SDK. MassTransit reused the existing
/// queue and kept its attributes (ConfigureConsumeTopology = false; no QueueAttributes set).
/// </remarks>
[Collection(LocalStackCollection.Name)]
[Trait("Category", "Integration")]
public sealed class AmazonSqsSpikeR3Tests
{
    private const int MaxReceiveCount = 3;
    private const int VisibilityTimeoutSeconds = 2;

    private readonly LocalStackContainerFixture _fixture;

    public AmazonSqsSpikeR3Tests(LocalStackContainerFixture fixture) => _fixture = fixture;

    public sealed class SpikeMessage
    {
        public string Id { get; set; } = string.Empty;
    }

    private sealed class SpikeConsumer(bool fail, List<DateTime> receives) : IConsumer<SpikeMessage>
    {
        public Task Consume(ConsumeContext<SpikeMessage> context)
        {
            lock (receives)
            {
                receives.Add(DateTime.UtcNow);
            }

            return fail ? throw new InvalidOperationException("spike failure") : Task.CompletedTask;
        }
    }

    [Fact]
    public async Task Throwing_consumer_leaves_message_undeleted_redelivers_and_dead_letters()
    {
        var (url, sqs, queueUrl, dlqUrl, prefix) = await ArrangeAsync();
        var receives = new List<DateTime>();
        var bus = CreateBus(url, prefix + "-main", fail: true, receives);
        await bus.StartAsync(TimeSpan.FromSeconds(60));
        try
        {
            var endpoint = await bus.GetSendEndpoint(new Uri($"queue:{prefix}-main"));
            await endpoint.Send(new SpikeMessage { Id = "1" });

            await WaitUntilAsync(() => Count(receives) >= MaxReceiveCount, 40);
            await WaitUntilAsync(async () => await MessageCountAsync(sqs, dlqUrl) >= 1, 40);

            Assert.Equal(MaxReceiveCount, Count(receives));
            Assert.True(
                receives[1] - receives[0] >= TimeSpan.FromSeconds(VisibilityTimeoutSeconds - 1),
                "redelivery should wait for the visibility timeout");
            Assert.Equal(1, await MessageCountAsync(sqs, dlqUrl));
            Assert.Equal(0, await MessageCountAsync(sqs, queueUrl));
            Assert.Equal(MaxReceiveCount, Count(receives));
            var queues = (await sqs.ListQueuesAsync(prefix)).QueueUrls ?? [];
            Assert.DoesNotContain(queues, q => q.EndsWith("_error", StringComparison.Ordinal));
        }
        finally
        {
            await bus.StopAsync();
            sqs.Dispose();
        }
    }

    [Fact]
    public async Task Normally_returning_consumer_deletes_message()
    {
        var (url, sqs, queueUrl, dlqUrl, prefix) = await ArrangeAsync();
        var receives = new List<DateTime>();
        var bus = CreateBus(url, prefix + "-main", fail: false, receives);
        await bus.StartAsync(TimeSpan.FromSeconds(60));
        try
        {
            var endpoint = await bus.GetSendEndpoint(new Uri($"queue:{prefix}-main"));
            await endpoint.Send(new SpikeMessage { Id = "1" });

            await WaitUntilAsync(() => Count(receives) >= 1, 30);
            // wait longer than the visibility timeout: a deleted message is never redelivered
            await Task.Delay(TimeSpan.FromSeconds(VisibilityTimeoutSeconds + 3));

            Assert.Equal(1, Count(receives));
            Assert.Equal(0, await MessageCountAsync(sqs, queueUrl));
            Assert.Equal(0, await MessageCountAsync(sqs, dlqUrl));
        }
        finally
        {
            await bus.StopAsync();
            sqs.Dispose();
        }
    }

    private async Task<(string Url, AmazonSQSClient Sqs, string QueueUrl, string DlqUrl, string Prefix)>
        ArrangeAsync()
    {
        var url = await _fixture.GetServiceUrlAsync();
        var prefix = "r3spike" + Guid.NewGuid().ToString("N")[..10];
        var sqs = new AmazonSQSClient(
            new BasicAWSCredentials(LocalStackContainerFixture.AccessKeyId, LocalStackContainerFixture.SecretAccessKey),
            new AmazonSQSConfig { ServiceURL = url, AuthenticationRegion = LocalStackContainerFixture.Region });

        var dlqUrl = (await sqs.CreateQueueAsync(prefix + "-dlq")).QueueUrl;
        var dlqArn = (await sqs.GetQueueAttributesAsync(dlqUrl, ["QueueArn"])).QueueARN;
        var queueUrl = (await sqs.CreateQueueAsync(new CreateQueueRequest
        {
            QueueName = prefix + "-main",
            Attributes = new Dictionary<string, string>
            {
                ["VisibilityTimeout"] = VisibilityTimeoutSeconds.ToString(),
                ["RedrivePolicy"] =
                    $"{{\"deadLetterTargetArn\":\"{dlqArn}\",\"maxReceiveCount\":\"{MaxReceiveCount}\"}}",
            },
        })).QueueUrl;
        return (url, sqs, queueUrl, dlqUrl, prefix);
    }

    private static IBusControl CreateBus(string serviceUrl, string queueName, bool fail, List<DateTime> receives) =>
        Bus.Factory.CreateUsingAmazonSqs(cfg =>
        {
            cfg.Host(LocalStackContainerFixture.Region, h =>
            {
                h.AccessKey(LocalStackContainerFixture.AccessKeyId);
                h.SecretKey(LocalStackContainerFixture.SecretAccessKey);
                h.Config(new AmazonSQSConfig
                {
                    ServiceURL = serviceUrl,
                    AuthenticationRegion = LocalStackContainerFixture.Region,
                });
                h.Config(new Amazon.SimpleNotificationService.AmazonSimpleNotificationServiceConfig
                {
                    ServiceURL = serviceUrl,
                    AuthenticationRegion = LocalStackContainerFixture.Region,
                });
            });

            cfg.ReceiveEndpoint(queueName, e =>
            {
                e.ConfigureConsumeTopology = false;
                e.UseMessageRetry(r => r.None());
                e.ConfigureError(x => x.UseFilter(new RethrowErrorTransportFilter()));
                e.Consumer(() => new SpikeConsumer(fail, receives));
            });
        });

    private static int Count(List<DateTime> receives)
    {
        lock (receives)
        {
            return receives.Count;
        }
    }

    private static async Task<int> MessageCountAsync(AmazonSQSClient sqs, string queueUrl)
    {
        var attrs = await sqs.GetQueueAttributesAsync(queueUrl,
            ["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"]);
        return attrs.ApproximateNumberOfMessages + attrs.ApproximateNumberOfMessagesNotVisible;
    }

    private static Task WaitUntilAsync(Func<bool> condition, int seconds) =>
        WaitUntilAsync(() => Task.FromResult(condition()), seconds);

    private static async Task WaitUntilAsync(Func<Task<bool>> condition, int seconds)
    {
        var deadline = DateTime.UtcNow.AddSeconds(seconds);
        while (DateTime.UtcNow < deadline)
        {
            if (await condition())
            {
                return;
            }

            await Task.Delay(250);
        }
    }
}
