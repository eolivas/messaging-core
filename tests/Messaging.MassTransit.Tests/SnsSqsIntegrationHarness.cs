using System.Collections.Concurrent;
using System.Security.Cryptography;
using Amazon.Runtime;
using Amazon.SimpleNotificationService;
using Amazon.SQS;
using Messaging.Abstractions;
using Messaging.Consuming;
using Messaging.Contracts;
using Messaging.Lifecycle;
using Messaging.MassTransit.AmazonSqs;
using Messaging.MassTransit.Configuration;
using Messaging.MassTransit.Consuming;
using Messaging.Observability;
using Microsoft.Extensions.Logging.Abstractions;
namespace Messaging.MassTransit.Tests;

/// <summary>
/// Task 14.1 harness for LocalStack integration tests. One instance per test class: it owns a random
/// <c>t{8 hex}-</c> prefix that isolates topic and queue names, builds real
/// <see cref="AmazonSqsBusAdapter"/> instances with dummy credentials against the fixture endpoint,
/// and stops and disposes everything it built.
/// </summary>
internal sealed class SnsSqsIntegrationHarness : IAsyncDisposable
{
    /// <summary>The wait budget every integration assertion uses.</summary>
    public static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(30);

    /// <summary>The message type the harness registers by default.</summary>
    public const string DefaultMessageType = "test.contract";

    private readonly ConcurrentQueue<AmazonSqsBusAdapter> _adapters = new();
    private readonly ConcurrentQueue<IDisposable> _disposables = new();

    private SnsSqsIntegrationHarness(string serviceUrl)
    {
        ServiceUrl = serviceUrl;
        Prefix = "t" + Convert.ToHexString(RandomNumberGenerator.GetBytes(4)).ToLowerInvariant() + "-";
        var credentials = new BasicAWSCredentials(
            LocalStackContainerFixture.AccessKeyId, LocalStackContainerFixture.SecretAccessKey);
        Sqs = new AmazonSQSClient(credentials, new AmazonSQSConfig
        {
            ServiceURL = serviceUrl,
            AuthenticationRegion = LocalStackContainerFixture.Region,
        });
        Sns = new AmazonSimpleNotificationServiceClient(credentials, new AmazonSimpleNotificationServiceConfig
        {
            ServiceURL = serviceUrl,
            AuthenticationRegion = LocalStackContainerFixture.Region,
        });
        _disposables.Enqueue(Sqs);
        _disposables.Enqueue(Sns);
    }

    /// <summary>The random <c>t{8 hex}-</c> prefix applied to every topic and queue name.</summary>
    public string Prefix { get; }

    /// <summary>The LocalStack edge endpoint.</summary>
    public string ServiceUrl { get; }

    /// <summary>A direct SQS client for assertions about queue state.</summary>
    public IAmazonSQS Sqs { get; }

    /// <summary>A direct SNS client for assertions about topic state.</summary>
    public IAmazonSimpleNotificationService Sns { get; }

    /// <summary>Creates a harness against the fixture's LocalStack endpoint.</summary>
    public static async Task<SnsSqsIntegrationHarness> CreateAsync(LocalStackContainerFixture fixture)
    {
        ArgumentNullException.ThrowIfNull(fixture);
        return new SnsSqsIntegrationHarness(await fixture.GetServiceUrlAsync().ConfigureAwait(false));
    }

    /// <summary>Creates a harness against an explicit endpoint (a dedicated, stoppable container).</summary>
    public static SnsSqsIntegrationHarness CreateForUrl(string serviceUrl)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(serviceUrl);
        return new SnsSqsIntegrationHarness(serviceUrl);
    }
    /// <summary>Settings for an endpoint: prefixed names, dummy credentials, fast polling.</summary>
    public AmazonSqsProviderSettings CreateSettings(
        string endpointName, Func<AmazonSqsProviderSettings, AmazonSqsProviderSettings>? customize = null)
    {
        var settings = new AmazonSqsProviderSettings
        {
            Region = LocalStackContainerFixture.Region,
            EndpointName = endpointName,
            ServiceUrl = ServiceUrl,
            AccessKeyId = LocalStackContainerFixture.AccessKeyId,
            SecretAccessKey = LocalStackContainerFixture.SecretAccessKey,
            TopicNamePrefix = Prefix,
            QueueNamePrefix = Prefix,
            WaitTimeSeconds = 1,
        };
        return customize is null ? settings : customize(settings);
    }

    /// <summary>The prefixed topic name the adapter derives for <paramref name="messageType"/>.</summary>
    public string TopicName(string messageType = DefaultMessageType) =>
        SnsSqsNamingConvention.TopicName(messageType, CreateSettings("unused"));

    /// <summary>The prefixed queue name the adapter derives for <paramref name="endpointName"/>.</summary>
    public string QueueName(string endpointName) =>
        SnsSqsNamingConvention.QueueName(endpointName, CreateSettings(endpointName));

    /// <summary>
    /// Builds a real adapter (not started). Pass a processor for consuming roles. Disposed with the harness.
    /// </summary>
    public AmazonSqsBusAdapter CreateBus(
        string endpointName,
        MassTransitRole role,
        MessagingMode mode,
        IMassTransitConsumeProcessor? processor,
        Func<AmazonSqsProviderSettings, AmazonSqsProviderSettings>? customize = null,
        IEnumerable<ContractRegistration>? contracts = null,
        InFlightTracker? tracker = null)
    {
        var settings = CreateSettings(endpointName, customize);
        var topology = new SnsSqsTopologyManager(
            new AwsSnsSqsTopologyClient(Sqs, Sns), settings, NullLogger<SnsSqsTopologyManager>.Instance);
        var metrics = new AmazonSqsMetrics();
        _disposables.Enqueue(metrics);
        var observability = new ObservabilityContext(
            new NoOpMessagingLog(),
            new NoOpMessagingMetrics(),
            new MessagingTraceSource(tracingEnabled: false),
            new PayloadRedactor());
        var adapter = AmazonSqsBusAdapter.Create(
            settings,
            role,
            mode,
            contracts ?? new[] { new ContractRegistration(DefaultMessageType, typeof(HarnessContract)) },
            processor,
            topology,
            new AwsSnsSqsTopologyClient(Sqs, Sns),
            observability,
            metrics,
            tracker ?? new InFlightTracker(),
            shutdownTimeoutSeconds: 10,
            NullLogger<AmazonSqsBusAdapter>.Instance);
        _adapters.Enqueue(adapter);
        return adapter;
    }

    /// <summary>
    /// Polls <paramref name="queueName"/> with the raw SQS client until a message arrives or
    /// <paramref name="timeout"/> elapses. The message is not deleted. Returns null on timeout.
    /// </summary>
    public async Task<Amazon.SQS.Model.Message?> ReceiveRawAsync(string queueName, TimeSpan timeout)
    {
        var url = (await Sqs.GetQueueUrlAsync(queueName).ConfigureAwait(false)).QueueUrl;
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            var response = await Sqs.ReceiveMessageAsync(new Amazon.SQS.Model.ReceiveMessageRequest
            {
                QueueUrl = url,
                MaxNumberOfMessages = 1,
                WaitTimeSeconds = 1,
                MessageAttributeNames = new List<string> { "All" },
                MessageSystemAttributeNames = new List<string> { "All" },
            }).ConfigureAwait(false);
            if (response.Messages is { Count: > 0 })
            {
                return response.Messages[0];
            }
        }

        return null;
    }

    /// <summary>Reads the approximate visible and not-visible message counts of a queue.</summary>
    public async Task<(int Visible, int NotVisible)> GetMessageCountsAsync(string queueName)
    {
        var url = (await Sqs.GetQueueUrlAsync(queueName).ConfigureAwait(false)).QueueUrl;
        var attributes = await Sqs.GetQueueAttributesAsync(url, new List<string>
        {
            "ApproximateNumberOfMessages",
            "ApproximateNumberOfMessagesNotVisible",
        }).ConfigureAwait(false);
        return (attributes.ApproximateNumberOfMessages, attributes.ApproximateNumberOfMessagesNotVisible);
    }

    /// <summary>Builds a stable, fully populated envelope for <see cref="DefaultMessageType"/>.</summary>
    public static Messaging.Contracts.MessageEnvelope CreateEnvelope(
        string? text = null, string messageType = DefaultMessageType) =>
        EnvelopeFactory.Create(Guid.NewGuid().ToString("D"), EnvelopeFactory.Utf8(text ?? "hello")) with
        {
            MessageType = messageType,
        };

    /// <summary>Asserts every envelope field (Req 6.3 to 6.5) equals the published value.</summary>
    public static void AssertDeliveredEqualsPublished(
        Messaging.Contracts.MessageEnvelope expected, Messaging.Contracts.MessageEnvelope actual)
    {
        Assert.Equal(expected.MessageId, actual.MessageId);
        Assert.Equal(expected.CorrelationId, actual.CorrelationId);
        Assert.Equal(expected.MessageType, actual.MessageType);
        Assert.Equal(expected.CreatedAtUtc, actual.CreatedAtUtc);
        Assert.Equal(expected.ContractVersion, actual.ContractVersion);
        Assert.Equal(
            expected.Headers.OrderBy(h => h.Key, StringComparer.Ordinal),
            actual.Headers.OrderBy(h => h.Key, StringComparer.Ordinal));
        Assert.Equal(expected.Payload.ToArray(), actual.Payload.ToArray());
    }

    public async ValueTask DisposeAsync()
    {
        while (_adapters.TryDequeue(out var adapter))
        {
            try
            {
                await adapter.StopBusAsync(CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception)
            {
                // Best-effort cleanup; the bus may never have started.
            }

            await adapter.DisposeAsync().ConfigureAwait(false);
        }

        while (_disposables.TryDequeue(out var disposable))
        {
            disposable.Dispose();
        }
    }

    /// <summary>Contract type registered for <see cref="DefaultMessageType"/>.</summary>
    public sealed record HarnessContract(string Text);
}
