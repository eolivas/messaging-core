using System.Collections.Concurrent;
using Amazon.Runtime;
using Amazon.Runtime.Internal;
using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using Amazon.SQS;
using Amazon.SQS.Model;
using MassTransit;
using Microsoft.Extensions.DependencyInjection;
using Xunit.Abstractions;

namespace Messaging.MassTransit.Tests;

// SPIKE R1 / R2 (task 1.4). FINDINGS are recorded in the comment block at the bottom of this file.
[Collection(LocalStackCollection.Name)]
[Trait("Category", "Integration")]
public sealed class AmazonSqsSpikeR1R2Tests
{
    private const string RedriveKey = "RedrivePolicy";
    private static readonly string[] CompareKeys =
    {
        "VisibilityTimeout", "MessageRetentionPeriod", RedriveKey, "ReceiveMessageWaitTimeSeconds",
    };

    private readonly LocalStackContainerFixture _fixture;
    private readonly ITestOutputHelper _output;

    public AmazonSqsSpikeR1R2Tests(LocalStackContainerFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
        AwsCallRecorder.EnsureRegistered();
    }

    public sealed record SpikeMessage(string Text);

    public sealed class SpikeConsumer : IConsumer<SpikeMessage>
    {
        public Task Consume(ConsumeContext<SpikeMessage> context) => Task.CompletedTask;
    }

    public sealed class FaultingConsumer : IConsumer<SpikeMessage>
    {
        public Task Consume(ConsumeContext<SpikeMessage> context) =>
            throw new InvalidOperationException("spike fault");
    }

    private sealed class Suffix : IErrorQueueNameFormatter, IDeadLetterQueueNameFormatter
    {
        public string FormatErrorQueueName(string queueName) => queueName + "-failed";
        public string FormatDeadLetterQueueName(string queueName) => queueName + "-unconsumed";
    }

    private sealed record Provisioned(
        string Prefix, string Queue, string QueueUrl, string QueueArn, string DlqUrl,
        string TopicArn, string SubscriptionArn,
        IReadOnlyDictionary<string, string> QueueAttrs);

    private async Task<(IAmazonSQS Sqs, IAmazonSimpleNotificationService Sns, string Url)> ClientsAsync()
    {
        var url = await _fixture.GetServiceUrlAsync();
        var creds = new BasicAWSCredentials(
            LocalStackContainerFixture.AccessKeyId, LocalStackContainerFixture.SecretAccessKey);
        var sqs = new AmazonSQSClient(creds, new AmazonSQSConfig { ServiceURL = url, AuthenticationRegion = LocalStackContainerFixture.Region });
        var sns = new AmazonSimpleNotificationServiceClient(
            creds, new AmazonSimpleNotificationServiceConfig { ServiceURL = url, AuthenticationRegion = LocalStackContainerFixture.Region });
        return (sqs, sns, url);
    }

    private static async Task<Provisioned> ProvisionAsync(
        IAmazonSQS sqs, IAmazonSimpleNotificationService sns, string prefix,
        string visibility, string retention)
    {
        var dlq = prefix + "-dlq";
        var dlqUrl = (await sqs.CreateQueueAsync(dlq)).QueueUrl;
        var dlqArn = (await sqs.GetQueueAttributesAsync(dlqUrl, new List<string> { "QueueArn" })).QueueARN;
        var queue = prefix + "-q";
        var queueUrl = (await sqs.CreateQueueAsync(new CreateQueueRequest
        {
            QueueName = queue,
            Attributes = new Dictionary<string, string>
            {
                ["VisibilityTimeout"] = visibility,
                ["MessageRetentionPeriod"] = retention,
                [RedriveKey] = $"{{\"deadLetterTargetArn\":\"{dlqArn}\",\"maxReceiveCount\":\"5\"}}",
            },
        })).QueueUrl;
        var queueArn = (await sqs.GetQueueAttributesAsync(queueUrl, new List<string> { "QueueArn" })).QueueARN;
        var topicArn = (await sns.CreateTopicAsync(prefix + "-topic")).TopicArn;
        var subArn = (await sns.SubscribeAsync(new SubscribeRequest
        {
            TopicArn = topicArn,
            Protocol = "sqs",
            Endpoint = queueArn,
            Attributes = new Dictionary<string, string> { ["RawMessageDelivery"] = "true" },
            ReturnSubscriptionArn = true,
        })).SubscriptionArn;
        var attrs = await ReadAttrsAsync(sqs, queueUrl);
        return new Provisioned(prefix, queue, queueUrl, queueArn, dlqUrl, topicArn, subArn, attrs);
    }

    private static async Task<IReadOnlyDictionary<string, string>> ReadAttrsAsync(IAmazonSQS sqs, string url)
    {
        var all = (await sqs.GetQueueAttributesAsync(url, new List<string> { "All" })).Attributes;
        return CompareKeys.Where(all.ContainsKey).ToDictionary(k => k, k => all[k]);
    }

    private async Task<IReadOnlyList<string>> ListQueuesAsync(IAmazonSQS sqs, string prefix)
    {
        var r = await sqs.ListQueuesAsync(new ListQueuesRequest { QueueNamePrefix = prefix });
        return (r.QueueUrls ?? new List<string>()).Select(u => u[(u.LastIndexOf('/') + 1)..]).OrderBy(x => x).ToList();
    }

    /// <summary>Builds and starts a bus; returns the start exception (null on success) and recorded AWS calls.</summary>
    private async Task<(Exception? StartError, IReadOnlyList<string> Calls, ServiceProvider Provider)> StartBusAsync(
        string url, string queue, Action<IAmazonSqsReceiveEndpointConfigurator>? configureEndpoint = null,
        Action<IAmazonSqsBusFactoryConfigurator>? configureBus = null, bool faulting = false)
    {
        var services = new ServiceCollection();
        services.AddMassTransit(x =>
        {
            if (faulting) x.AddConsumer<FaultingConsumer>(); else x.AddConsumer<SpikeConsumer>();
            x.UsingAmazonSqs((ctx, cfg) =>
            {
                cfg.Host(LocalStackContainerFixture.Region, h =>
                {
                    h.AccessKey(LocalStackContainerFixture.AccessKeyId);
                    h.SecretKey(LocalStackContainerFixture.SecretAccessKey);
                    h.Config(new AmazonSQSConfig { ServiceURL = url, AuthenticationRegion = LocalStackContainerFixture.Region });
                    h.Config(new AmazonSimpleNotificationServiceConfig { ServiceURL = url, AuthenticationRegion = LocalStackContainerFixture.Region });
                });
                configureBus?.Invoke(cfg);
                cfg.ReceiveEndpoint(queue, e =>
                {
                    e.ConfigureConsumeTopology = false;
                    configureEndpoint?.Invoke(e);
                    if (faulting) e.ConfigureConsumer<FaultingConsumer>(ctx); else e.ConfigureConsumer<SpikeConsumer>(ctx);
                });
            });
        });
        var provider = services.BuildServiceProvider();
        var bus = provider.GetRequiredService<IBusControl>();
        AwsCallRecorder.Begin();
        Exception? error = null;
        try
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
            await bus.StartAsync(cts.Token);
        }
        catch (Exception ex)
        {
            error = ex;
        }
        return (error, AwsCallRecorder.Snapshot(), provider);
    }

    private static Action<IAmazonSqsReceiveEndpointConfigurator> MatchingAttrs(Provisioned p) => e =>
    {
        e.QueueAttributes["VisibilityTimeout"] = p.QueueAttrs["VisibilityTimeout"];
        e.QueueAttributes["MessageRetentionPeriod"] = p.QueueAttrs["MessageRetentionPeriod"];
        e.QueueAttributes[RedriveKey] = p.QueueAttrs[RedriveKey];
    };

    private async Task StopAsync(ServiceProvider provider)
    {
        try
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            await provider.GetRequiredService<IBusControl>().StopAsync(cts.Token);
        }
        catch (Exception ex)
        {
            _output.WriteLine("stop error: " + ex.GetType().Name);
        }
        await provider.DisposeAsync();
    }

    private void Dump(string title, Exception? err, IReadOnlyList<string> calls, IEnumerable<string> queues)
    {
        _output.WriteLine($"== {title}");
        _output.WriteLine("start error: " + (err is null ? "none" : err.GetType().Name + ": " + err.Message));
        _output.WriteLine("aws calls: " + string.Join(", ", calls.GroupBy(c => c).Select(g => $"{g.Key}x{g.Count()}")));
        _output.WriteLine("queues: " + string.Join(", ", queues));
    }

    [Fact]
    public async Task Start_AgainstPreProvisionedResources_MatchingAttributes_LeavesEverythingUnchanged()
    {
        var (sqs, sns, url) = await ClientsAsync();
        var prefix = "spk14a-" + Guid.NewGuid().ToString("N")[..8];
        var p = await ProvisionAsync(sqs, sns, prefix, "45", "86400");

        var (err, calls, provider) = await StartBusAsync(url, p.Queue, MatchingAttrs(p));
        try
        {
            var queues = await ListQueuesAsync(sqs, prefix);
            Dump("matching attributes", err, calls, queues);
            Assert.Null(err);
            Assert.Equal(p.QueueAttrs, await ReadAttrsAsync(sqs, p.QueueUrl));
            var topics = (await sns.ListTopicsAsync()).Topics.Where(t => t.TopicArn.Contains(prefix)).ToList();
            Assert.Single(topics);
            var subs = (await sns.ListSubscriptionsByTopicAsync(p.TopicArn)).Subscriptions;
            Assert.Single(subs);
            Assert.Equal(p.SubscriptionArn, subs[0].SubscriptionArn);
            // Eager auxiliary queues (R2): recorded, asserted below.
            Assert.DoesNotContain(queues, q => q.EndsWith("_error") || q.EndsWith("_skipped"));
        }
        finally
        {
            await StopAsync(provider);
        }
    }

    [Fact]
    public async Task Start_AgainstPreExistingQueueWithDifferentAttributes_RecordsOutcome()
    {
        var (sqs, sns, url) = await ClientsAsync();
        var prefix = "spk14b-" + Guid.NewGuid().ToString("N")[..8];
        var p = await ProvisionAsync(sqs, sns, prefix, "45", "86400");

        // Different visibility/retention and no redrive policy on the MassTransit side.
        var (err, calls, provider) = await StartBusAsync(url, p.Queue, e =>
        {
            e.QueueAttributes["VisibilityTimeout"] = "120";
            e.QueueAttributes["MessageRetentionPeriod"] = "345600";
        });
        try
        {
            var after = await ReadAttrsAsync(sqs, p.QueueUrl);
            Dump("different attributes", err, calls, await ListQueuesAsync(sqs, prefix));
            _output.WriteLine("before: " + string.Join(", ", p.QueueAttrs.Select(kv => kv.Key + "=" + kv.Value)));
            _output.WriteLine("after:  " + string.Join(", ", after.Select(kv => kv.Key + "=" + kv.Value)));
            // Finding is asserted: see FINDINGS block.
            Assert.Equal(p.QueueAttrs, after);
        }
        finally
        {
            await StopAsync(provider);
        }
    }

    [Fact]
    public async Task Start_DefaultEndpointSettings_AgainstPreExistingQueue_RecordsOutcome()
    {
        var (sqs, sns, url) = await ClientsAsync();
        var prefix = "spk14c-" + Guid.NewGuid().ToString("N")[..8];
        var p = await ProvisionAsync(sqs, sns, prefix, "45", "86400");

        var (err, calls, provider) = await StartBusAsync(url, p.Queue);
        try
        {
            var after = await ReadAttrsAsync(sqs, p.QueueUrl);
            Dump("default endpoint settings", err, calls, await ListQueuesAsync(sqs, prefix));
            _output.WriteLine("after:  " + string.Join(", ", after.Select(kv => kv.Key + "=" + kv.Value)));
            Assert.Equal(p.QueueAttrs, after);
        }
        finally
        {
            await StopAsync(provider);
        }
    }

    [Fact]
    public async Task Start_WithNonDefaultDeadLetterSuffix_DoesNotEagerlyCreateAuxiliaryQueues()
    {
        var (sqs, sns, url) = await ClientsAsync();
        var prefix = "spk14d-" + Guid.NewGuid().ToString("N")[..8];
        var p = await ProvisionAsync(sqs, sns, prefix, "45", "86400");

        var (err, calls, provider) = await StartBusAsync(url, p.Queue, MatchingAttrs(p), cfg =>
        {
            cfg.SendTopology.ErrorQueueNameFormatter = new Suffix();
            cfg.SendTopology.DeadLetterQueueNameFormatter = new Suffix();
        });
        try
        {
            var queues = await ListQueuesAsync(sqs, prefix);
            Dump("non-default suffix", err, calls, queues);
            Assert.Null(err);
            Assert.Equal(new[] { p.Queue, prefix + "-dlq" }.OrderBy(x => x), queues);
        }
        finally
        {
            await StopAsync(provider);
        }
    }

    [Fact]
    public async Task Fault_CreatesErrorQueueLazily_UsingTheConfiguredSuffix()
    {
        var (sqs, sns, url) = await ClientsAsync();
        var prefix = "spk14e-" + Guid.NewGuid().ToString("N")[..8];
        var p = await ProvisionAsync(sqs, sns, prefix, "45", "86400");

        var (err, calls, provider) = await StartBusAsync(url, p.Queue, e =>
        {
            MatchingAttrs(p)(e);
            e.UseMessageRetry(r => r.None());
        }, cfg => cfg.SendTopology.ErrorQueueNameFormatter = new Suffix(), faulting: true);
        try
        {
            Assert.Null(err);
            var bus = provider.GetRequiredService<IBusControl>();
            var send = await bus.GetSendEndpoint(new Uri($"queue:{p.Queue}"));
            await send.Send(new SpikeMessage("boom"));
            IReadOnlyList<string> queues = Array.Empty<string>();
            for (var i = 0; i < 30; i++)
            {
                queues = await ListQueuesAsync(sqs, prefix);
                if (queues.Any(q => q.EndsWith("-failed"))) break;
                await Task.Delay(500);
            }
            Dump("fault -> error queue", null, AwsCallRecorder.Snapshot(), queues);
            Assert.Contains(prefix + "-q-failed", queues);
        }
        finally
        {
            await StopAsync(provider);
        }
    }

    private sealed class AwsCallRecorder : IRuntimePipelineCustomizer
    {
        private static readonly ConcurrentQueue<string> Calls = new();
        private static int _registered;
        private static volatile bool _recording;

        public string UniqueName => "spike-r1r2-recorder";

        public static void EnsureRegistered()
        {
            if (Interlocked.Exchange(ref _registered, 1) == 0)
                RuntimePipelineCustomizerRegistry.Instance.Register(new AwsCallRecorder());
        }

        public static void Begin()
        {
            Calls.Clear();
            _recording = true;
        }

        public static IReadOnlyList<string> Snapshot() => Calls.ToArray();

        public void Customize(Type serviceClientType, RuntimePipeline pipeline)
        {
            if (serviceClientType.Namespace is { } ns && (ns.StartsWith("Amazon.SQS") || ns.StartsWith("Amazon.SimpleNotificationService")))
                pipeline.AddHandlerBefore<Marshaller>(new Handler(serviceClientType.Name.Contains("SQS") ? "sqs" : "sns"));
        }

        private sealed class Handler : PipelineHandler
        {
            private readonly string _svc;
            public Handler(string svc) => _svc = svc;

            public override async Task<T> InvokeAsync<T>(IExecutionContext executionContext)
            {
                if (_recording)
                    Calls.Enqueue(_svc + ":" + executionContext.RequestContext.OriginalRequest.GetType().Name.Replace("Request", ""));
                return await base.InvokeAsync<T>(executionContext).ConfigureAwait(false);
            }
        }
    }

    /*
     * FINDINGS (MassTransit.AmazonSQS 8.5.11, AWSSDK v4, LocalStack; observed via an SDK pipeline recorder)
     *
     * R1. Bus start against a pre-provisioned queue (+ topic + raw subscription), with
     *     ConfigureConsumeTopology = false, succeeds. Queue attributes (VisibilityTimeout,
     *     MessageRetentionPeriod, RedrivePolicy), the topic and the subscription ARN are unchanged.
     *     This holds with matching endpoint attributes, with different ones, and with defaults.
     *     MassTransit does NOT issue CreateQueue when the queue exists: its calls at start are
     *     sqs:GetQueueUrl + sqs:GetQueueAttributes (lookup first, create only on miss), then
     *     sqs:ReceiveMessage once the consumer loop runs. So there is no "idempotent create with
     *     different attributes" to be rejected, and endpoint attribute settings are ignored for
     *     an existing queue (never applied via SetQueueAttributes). No SNS call occurs at start
     *     when the consume topology is off (no CreateTopic/Subscribe/ListTopics).
     *     Caveat: if the queue were missing, MassTransit would create it with ITS attributes.
     *
     * R2. {queue}_error and {queue}_skipped are NOT created eagerly: after start the only queues
     *     are the pre-provisioned ones, with default and with non-default suffix formatters.
     *     (Source: error/dead-letter transports are built lazily; the create filter runs on
     *     first send.) The error queue is created on first fault: sqs:CreateQueue for
     *     "{queue}<suffix>" using cfg.SendTopology.ErrorQueueNameFormatter (observed "-failed").
     *     Therefore a consumer fault needs sqs:CreateQueue (+ GetQueueUrl/SendMessage) for the
     *     error queue, unless it is pre-provisioned; _skipped is created only on an unconsumed
     *     message. Pre-provisioning the error queue under the formatted name avoids the create.
     *     Also observed on the fault path: sns:ListTopics/CreateTopic/GetTopicAttributes/
     *     PublishBatch (Fault<T> publish, not part of start; avoidable via the core pipeline).
     *
     * IAM actions MassTransit needs at start, verify-only, consume topology off:
     *     sqs:GetQueueUrl, sqs:GetQueueAttributes, sqs:ReceiveMessage,
     *     (steady state) sqs:DeleteMessage[Batch], sqs:ChangeMessageVisibility.
     *     No sqs:CreateQueue, sqs:SetQueueAttributes, sns:* at start.
     *     Per-message error path additionally: sqs:CreateQueue/SendMessage[Batch] for the error
     *     queue (SendMessage only if pre-provisioned).
     *
     * Env note: Testcontainers needed DOCKER_HOST=unix:///var/run/docker.sock in this shell, and
     * the first LocalStack start can exceed the fixture's 60 s timeout when other agents start
     * containers concurrently (retry succeeded).
     */
}
