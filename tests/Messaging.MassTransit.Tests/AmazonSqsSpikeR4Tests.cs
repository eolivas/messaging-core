using System.Text.Json;
using Amazon.Runtime;
using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using Amazon.SQS;
using Amazon.SQS.Model;
using MassTransit;
using Messaging.MassTransit.Transport;
using Xunit.Abstractions;

namespace Messaging.MassTransit.Tests;

/// <summary>
/// Spike R4 (sns-sqs-provider task 1.6): with <c>RawMessageDelivery</c> true and false, an
/// <see cref="EnvelopeMessage"/> published through an SNS topic reaches a MassTransit consumer, and
/// the <c>x-correlation-id</c>, <c>traceparent</c> and <c>h.*</c> headers are recorded per mode.
/// A second "probe" queue on the same topic is read with the raw SDK to capture the wire shape.
/// </summary>
/// <remarks>
/// <para><b>FINDINGS (R4), LocalStack 4.14.0, MassTransit.AmazonSQS 8.5.11, AWSSDK v4:</b></para>
/// <list type="number">
/// <item>The body deserializes to <see cref="EnvelopeMessage"/> in both modes. MassTransit unwraps the
/// SNS JSON notification (<c>Type</c>/<c>Message</c>/<c>MessageAttributes</c>) itself.</item>
/// <item>Raw delivery (true): the SNS message attributes arrive as SQS message attributes, and
/// <c>x-correlation-id</c>, <c>traceparent</c>, <c>h.tenant</c> all appear in
/// <c>ReceiveContext.TransportHeaders</c>.</item>
/// <item>Wrapped delivery (false): the SQS message carries no message attributes. The SNS attributes
/// are only inside the wrapper JSON (<c>MessageAttributes.{name}.Value</c>), and
/// <c>ReceiveContext.TransportHeaders</c> is EMPTY. The MassTransit header provider does not
/// project the wrapper's <c>MessageAttributes</c> into transport headers. Design risk R4 is
/// confirmed as a real gap for wrapped mode.</item>
/// <item>In both modes, <c>ConsumeContext.Headers</c> holds all three headers, because MassTransit's
/// JSON envelope serializes <c>SendContext.Headers</c> into the body (<c>"headers"</c>). Those
/// values are body-resident, so the 10-attribute cap does not limit them.</item>
/// </list>
/// <para><b>Implication for design (task 1.7):</b> <c>SqsEnvelopeConsumer</c> / <c>FromWire</c>
/// should read <c>x-correlation-id</c>, <c>traceparent</c> and <c>h.*</c> from
/// <c>ConsumeContext.Headers</c> (works in both modes), not from <c>ReceiveContext.TransportHeaders</c>.
/// Alternatives: require <c>RawMessageDelivery = true</c>, or parse the wrapper. The attributes are
/// still useful for SNS filter policies and for non-MassTransit peers in raw mode.</para>
/// </remarks>
[Collection(LocalStackCollection.Name)]
[Trait("Category", "Integration")]
public sealed class AmazonSqsSpikeR4Tests
{
    private const string CorrelationKey = "x-correlation-id";
    private const string TraceparentKey = "traceparent";
    private const string UserHeaderKey = "h.tenant";
    private const string CorrelationValue = "corr-r4";
    private const string UserHeaderValue = "acme";
    private const string TraceparentValue = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

    private static readonly TimeSpan ReceiveTimeout = TimeSpan.FromSeconds(30);

    private readonly LocalStackContainerFixture _fixture;
    private readonly ITestOutputHelper _output;

    public AmazonSqsSpikeR4Tests(LocalStackContainerFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task PublishViaSnsTopic_RawDeliveryMode_BodyAndHeadersReachConsumer(bool rawDelivery)
    {
        // Arrange
        var url = await _fixture.GetServiceUrlAsync();
        var prefix = $"r4-spike-{Guid.NewGuid().ToString("N")[..10]}";
        var topicName = $"{prefix}-topic";
        var queueName = $"{prefix}-queue";
        var probeQueueName = $"{prefix}-probe";

        var credentials = new BasicAWSCredentials(
            LocalStackContainerFixture.AccessKeyId, LocalStackContainerFixture.SecretAccessKey);
        using var sqs = new AmazonSQSClient(credentials, SqsConfig(url));
        using var sns = new AmazonSimpleNotificationServiceClient(credentials, SnsConfig(url));

        var topicArn = (await sns.CreateTopicAsync(topicName)).TopicArn;
        var queueUrl = await CreateSubscribedQueueAsync(sqs, sns, topicArn, queueName, rawDelivery);
        var probeUrl = await CreateSubscribedQueueAsync(sqs, sns, topicArn, probeQueueName, rawDelivery);

        var received = new TaskCompletionSource<Observed>(TaskCreationOptions.RunContinuationsAsynchronously);
        var bus = CreateBus(url, topicName, queueName, received);
        var messageId = Guid.NewGuid();

        try
        {
            await bus.StartAsync(ReceiveTimeout);

            // Act
            await bus.Publish(
                new EnvelopeMessage
                {
                    MessageId = messageId,
                    CorrelationId = CorrelationValue,
                    MessageType = "r4.spike",
                    CreatedAtUtc = DateTimeOffset.UtcNow,
                    Payload = new byte[] { 1, 2, 3, 4 },
                },
                ctx =>
                {
                    ctx.Headers.Set(CorrelationKey, CorrelationValue);
                    ctx.Headers.Set(TraceparentKey, TraceparentValue);
                    ctx.Headers.Set(UserHeaderKey, UserHeaderValue);
                });

            var done = await Task.WhenAny(received.Task, Task.Delay(ReceiveTimeout));
            Assert.True(done == received.Task, "No message reached the consumer within 30 seconds.");
            var observed = await received.Task;
            var probe = await ReceiveProbeAsync(sqs, probeUrl);

            // AWSSDK v4 returns null (not an empty map) when the SQS message has no attributes.
            var probeAttributes = probe.MessageAttributes ?? new Dictionary<string, Amazon.SQS.Model.MessageAttributeValue>();

            _output.WriteLine($"rawDelivery={rawDelivery}");
            _output.WriteLine("transport headers: " + JsonSerializer.Serialize(observed.Transport));
            _output.WriteLine("consume headers:   " + JsonSerializer.Serialize(observed.Consume));
            _output.WriteLine("probe SQS attributes: " + string.Join(",", probeAttributes.Keys));
            _output.WriteLine("probe body: " + probe.Body);

            // Assert: body deserializes in both modes.
            Assert.Equal(messageId, observed.Body.MessageId);
            Assert.Equal(CorrelationValue, observed.Body.CorrelationId);
            Assert.Equal(new byte[] { 1, 2, 3, 4 }, observed.Body.Payload);

            // Assert: ConsumeContext.Headers carries all three in both modes (MT envelope body).
            AssertHeaders(observed.Consume);

            if (rawDelivery)
            {
                // Raw: SNS attributes become SQS attributes and surface as transport headers.
                AssertHeaders(observed.Transport);
                Assert.Equal(CorrelationValue, probeAttributes[CorrelationKey].StringValue);
            }
            else
            {
                // Wrapped: attributes live only inside the SNS JSON; MT exposes no transport headers.
                Assert.DoesNotContain(CorrelationKey, observed.Transport.Keys);
                Assert.DoesNotContain(TraceparentKey, observed.Transport.Keys);
                Assert.DoesNotContain(UserHeaderKey, observed.Transport.Keys);
                Assert.Empty(probeAttributes);

                using var wrapper = JsonDocument.Parse(probe.Body);
                var attributes = wrapper.RootElement.GetProperty("MessageAttributes");
                Assert.Equal(CorrelationValue, attributes.GetProperty(CorrelationKey).GetProperty("Value").GetString());
                Assert.Equal(TraceparentValue, attributes.GetProperty(TraceparentKey).GetProperty("Value").GetString());
                Assert.Equal(UserHeaderValue, attributes.GetProperty(UserHeaderKey).GetProperty("Value").GetString());
            }
        }
        finally
        {
            await bus.StopAsync(ReceiveTimeout);
            await sns.DeleteTopicAsync(topicArn);
            await sqs.DeleteQueueAsync(queueUrl);
            await sqs.DeleteQueueAsync(probeUrl);
        }
    }

    private static void AssertHeaders(IReadOnlyDictionary<string, string> headers)
    {
        Assert.Equal(CorrelationValue, headers[CorrelationKey]);
        Assert.Equal(TraceparentValue, headers[TraceparentKey]);
        Assert.Equal(UserHeaderValue, headers[UserHeaderKey]);
    }

    private static AmazonSQSConfig SqsConfig(string url) =>
        new() { ServiceURL = url, AuthenticationRegion = LocalStackContainerFixture.Region };

    private static AmazonSimpleNotificationServiceConfig SnsConfig(string url) =>
        new() { ServiceURL = url, AuthenticationRegion = LocalStackContainerFixture.Region };

    private static async Task<string> CreateSubscribedQueueAsync(
        IAmazonSQS sqs,
        IAmazonSimpleNotificationService sns,
        string topicArn,
        string queueName,
        bool rawDelivery)
    {
        var queueUrl = (await sqs.CreateQueueAsync(queueName)).QueueUrl;
        var queueArn = (await sqs.GetQueueAttributesAsync(queueUrl, new List<string> { "QueueArn" })).QueueARN;
        var policy = JsonSerializer.Serialize(new
        {
            Version = "2012-10-17",
            Statement = new[]
            {
                new
                {
                    Effect = "Allow",
                    Principal = new { Service = "sns.amazonaws.com" },
                    Action = "sqs:SendMessage",
                    Resource = queueArn,
                    Condition = new { ArnEquals = new Dictionary<string, string> { ["aws:SourceArn"] = topicArn } },
                },
            },
        });
        await sqs.SetQueueAttributesAsync(queueUrl, new Dictionary<string, string> { ["Policy"] = policy });
        await sns.SubscribeAsync(new SubscribeRequest
        {
            TopicArn = topicArn,
            Protocol = "sqs",
            Endpoint = queueArn,
            Attributes = new Dictionary<string, string>
            {
                ["RawMessageDelivery"] = rawDelivery ? "true" : "false",
            },
        });
        return queueUrl;
    }

    private static async Task<Amazon.SQS.Model.Message> ReceiveProbeAsync(IAmazonSQS sqs, string probeUrl)
    {
        var deadline = DateTime.UtcNow + ReceiveTimeout;
        while (DateTime.UtcNow < deadline)
        {
            var response = await sqs.ReceiveMessageAsync(new ReceiveMessageRequest
            {
                QueueUrl = probeUrl,
                WaitTimeSeconds = 5,
                MaxNumberOfMessages = 1,
                MessageAttributeNames = new List<string> { "All" },
            });
            if (response.Messages is { Count: > 0 })
            {
                return response.Messages[0];
            }
        }

        throw new TimeoutException("Probe queue received no message within 30 seconds.");
    }

    private static IBusControl CreateBus(
        string url, string topicName, string queueName, TaskCompletionSource<Observed> received) =>
        Bus.Factory.CreateUsingAmazonSqs(cfg =>
        {
            cfg.Host(LocalStackContainerFixture.Region, h =>
            {
                h.AccessKey(LocalStackContainerFixture.AccessKeyId);
                h.SecretKey(LocalStackContainerFixture.SecretAccessKey);
                h.Config(SqsConfig(url));
                h.Config(SnsConfig(url));
                h.AllowTransportHeader(header =>
                    header.Key == CorrelationKey || header.Key == TraceparentKey
                    || header.Key.StartsWith("h.", StringComparison.Ordinal));
            });

            cfg.Message<EnvelopeMessage>(m => m.SetEntityName(topicName));

            cfg.ReceiveEndpoint(queueName, e =>
            {
                e.ConfigureConsumeTopology = false;
                e.UseMessageRetry(r => r.None());
                e.Handler<EnvelopeMessage>(ctx =>
                {
                    received.TrySetResult(new Observed(
                        ctx.Message,
                        ToDictionary(ctx.ReceiveContext.TransportHeaders.GetAll()),
                        ToDictionary(ctx.Headers.GetAll())));
                    return Task.CompletedTask;
                });
            });
        });

    private static Dictionary<string, string> ToDictionary(IEnumerable<KeyValuePair<string, object>> headers)
    {
        var result = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var header in headers)
        {
            result[header.Key] = header.Value?.ToString() ?? string.Empty;
        }

        return result;
    }

    private sealed record Observed(
        EnvelopeMessage Body,
        Dictionary<string, string> Transport,
        Dictionary<string, string> Consume);
}
