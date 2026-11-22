using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using Messaging.Abstractions;
using Messaging.Consuming;
using Messaging.Contracts;
using Messaging.Kafka.Streaming;
using Messaging.Lifecycle;
using Messaging.MassTransit.AmazonSqs;
using Messaging.MassTransit.Configuration;
using Messaging.MassTransit.Consuming;
using Messaging.MassTransit.Transport;
using Messaging.Observability;
using Microsoft.Extensions.Logging.Abstractions;

namespace Demo.Infrastructure.Tests.Spike;

/// <summary>
/// Spike (task 1.2, Req 11.1, 11.2): building the transport clients must not open a connection.
/// Each test points the client at a local listener and asserts that nothing connects.
/// </summary>
public sealed class NoNetworkIoSpikeTests
{
    private static readonly TimeSpan ObservationWindow = TimeSpan.FromSeconds(2);
    private static readonly TimeSpan BuildBudget = TimeSpan.FromSeconds(5);

    [Fact]
    public async Task RabbitMqBusAdapterCreate_PointedAtListener_AcceptsNoConnection()
    {
        using var listener = new ProbeListener();
        var settings = new MassTransitProviderSettings(
            MassTransitTransport.RabbitMq,
            $"amqp://guest:guest@127.0.0.1:{listener.Port}/",
            host: "127.0.0.1");

        var elapsed = await listener.BuildAndObserveAsync(() => RabbitMqBusAdapter.Create(
            settings,
            MassTransitRole.Both,
            MessagingMode.PublishSubscribe,
            new NoOpProcessor(),
            NewObservability(),
            NullLogger<RabbitMqBusAdapter>.Instance,
            "spike-queue"));

        Assert.Equal(0, listener.AcceptedConnections);
        Assert.True(elapsed < BuildBudget, $"Build took {elapsed}.");
    }

    [Fact]
    public async Task AmazonSqsBusAdapterCreate_PointedAtListener_AcceptsNoConnection()
    {
        using var listener = new ProbeListener();
        var sqs = new AmazonSqsProviderSettings
        {
            Region = "us-east-1",
            EndpointName = "spike",
            ServiceUrl = $"http://127.0.0.1:{listener.Port}",
            AccessKeyId = "spike-key",
            SecretAccessKey = "spike-secret",
        };

        var elapsed = await listener.BuildAndObserveAsync(() => AmazonSqsBusAdapter.Create(
            sqs,
            MassTransitRole.Both,
            MessagingMode.PublishSubscribe,
            new[] { new ContractRegistration("spike-message", typeof(SpikeContract)) },
            new NoOpProcessor(),
            new SnsSqsTopologyManager(
                new UnreachableTopologyClient(), sqs, NullLogger<SnsSqsTopologyManager>.Instance),
            new UnreachableTopologyClient(),
            NewObservability(),
            new AmazonSqsMetrics(),
            new InFlightTracker(),
            shutdownTimeoutSeconds: 10,
            NullLogger<AmazonSqsBusAdapter>.Instance));

        Assert.Equal(0, listener.AcceptedConnections);
        Assert.True(elapsed < BuildBudget, $"Build took {elapsed}.");
    }

    [Fact]
    public async Task KafkaClientConfigFactory_PointedAtListener_AcceptsNoConnection()
    {
        using var listener = new ProbeListener();
        var factory = NewKafkaFactory(listener.Port, out _);

        var elapsed = await listener.BuildAndObserveAsync(() =>
        {
            factory.CreateProducerConfig();
            factory.CreateConsumerConfig();
            factory.CreateAdminClientConfig();
            return factory;
        });

        Assert.Equal(0, listener.AcceptedConnections);
        Assert.True(elapsed < BuildBudget, $"Build took {elapsed}.");
    }

    [Fact]
    public async Task ConfluentKafkaConsumerConstruction_NotSubscribed_OpensBackgroundConnections()
    {
        using var listener = new ProbeListener();
        var factory = NewKafkaFactory(listener.Port, out _);
        ConfluentKafkaConsumer? consumer = null;
        try
        {
            var elapsed = await listener.BuildAndObserveAsync(() =>
                consumer = new ConfluentKafkaConsumer(
                    factory, NullLogger<ConfluentKafkaConsumer>.Instance));

            // Characterization: librdkafka connects in background threads once built, even unsubscribed.
            Assert.True(listener.AcceptedConnections > 0);
            Assert.True(elapsed < BuildBudget, $"Build took {elapsed}.");
        }
        finally
        {
            (consumer as IDisposable)?.Dispose();
        }
    }

    [Fact]
    public async Task ConfluentKafkaProducerConstruction_NoProduce_OpensBackgroundConnections()
    {
        using var listener = new ProbeListener();
        var factory = NewKafkaFactory(listener.Port, out var settings);
        ConfluentKafkaProducer? producer = null;
        try
        {
            var elapsed = await listener.BuildAndObserveAsync(() =>
                producer = new ConfluentKafkaProducer(
                    factory, settings, NullLogger<ConfluentKafkaProducer>.Instance));

            // Characterization: see demo/docs/spike-no-network-io.md.
            Assert.True(listener.AcceptedConnections > 0);
            Assert.True(elapsed < BuildBudget, $"Build took {elapsed}.");
        }
        finally
        {
            (producer as IDisposable)?.Dispose();
        }
    }

    private static KafkaClientConfigFactory NewKafkaFactory(int port, out KafkaProviderSettings settings)
    {
        settings = new KafkaProviderSettings
        {
            BootstrapServers = $"127.0.0.1:{port}",
            ConsumerGroupId = "spike-group",
            Topic = "spike-topic",
        };
        return new KafkaClientConfigFactory(settings, new KafkaSecuritySettings());
    }

    private static ObservabilityContext NewObservability() => new(
        new NoOpLog(),
        new NoOpMetrics(),
        new MessagingTraceSource(tracingEnabled: false),
        new PayloadRedactor());

    private sealed record SpikeContract(string Value);

    private sealed class NoOpProcessor : IMassTransitConsumeProcessor
    {
        public Task ProcessAsync(MessageEnvelope envelope, CancellationToken cancellationToken) =>
            Task.CompletedTask;
    }

    private sealed class NoOpLog : IMessagingLog
    {
        public void MessageEvent(MessagingEventType eventType, string correlationId, DateTimeOffset timestampUtc)
        {
        }
    }

    private sealed class NoOpMetrics : IMessagingMetrics
    {
        public void RecordPublished() { }

        public void RecordConsumed() { }

        public void RecordRetry() { }

        public void RecordDeadLetter() { }
    }

    /// <summary>Control-plane stub: any call is a failure, because bus construction must not make one.</summary>
    private sealed class UnreachableTopologyClient : ISnsSqsTopologyClient
    {
        public Task<string?> GetQueueUrlAsync(string queueName, CancellationToken cancellationToken) =>
            throw Unexpected();

        public Task<IReadOnlyDictionary<string, string>?> GetQueueAttributesAsync(
            string queueUrl, CancellationToken cancellationToken) => throw Unexpected();

        public Task<string> CreateQueueAsync(
            string queueName, IReadOnlyDictionary<string, string> attributes, CancellationToken cancellationToken) =>
            throw Unexpected();

        public Task SetQueueAttributesAsync(
            string queueUrl, IReadOnlyDictionary<string, string> attributes, CancellationToken cancellationToken) =>
            throw Unexpected();

        public Task<string?> GetTopicArnAsync(string topicName, CancellationToken cancellationToken) =>
            throw Unexpected();

        public Task<string> CreateTopicAsync(
            string topicName, IReadOnlyDictionary<string, string> attributes, CancellationToken cancellationToken) =>
            throw Unexpected();

        public Task<IReadOnlyList<SnsSubscriptionInfo>> ListSubscriptionsByTopicAsync(
            string topicArn, CancellationToken cancellationToken) => throw Unexpected();

        public Task<string> SubscribeAsync(
            string topicArn,
            string queueArn,
            IReadOnlyDictionary<string, string> attributes,
            CancellationToken cancellationToken) => throw Unexpected();

        public Task PingAsync(CancellationToken cancellationToken) => throw Unexpected();

        private static InvalidOperationException Unexpected() =>
            new("The topology client must not be called while building a bus.");
    }

    /// <summary>Loopback listener that counts accepted connections.</summary>
    private sealed class ProbeListener : IDisposable
    {
        private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
        private readonly CancellationTokenSource _stop = new();
        private int _accepted;
        private Task? _loop;

        public ProbeListener()
        {
            _listener.Start();
            Port = ((IPEndPoint)_listener.LocalEndpoint).Port;
            _loop = Task.Run(AcceptLoopAsync);
        }

        public int Port { get; }

        public int AcceptedConnections => Volatile.Read(ref _accepted);

        /// <summary>Runs the build, then waits the observation window for background connections.</summary>
        public async Task<TimeSpan> BuildAndObserveAsync<T>(Func<T> build)
        {
            var sw = Stopwatch.StartNew();
            build();
            sw.Stop();
            await Task.Delay(ObservationWindow);
            return sw.Elapsed;
        }

        public void Dispose()
        {
            _stop.Cancel();
            _listener.Stop();
            _stop.Dispose();
        }

        private async Task AcceptLoopAsync()
        {
            while (!_stop.IsCancellationRequested)
            {
                try
                {
                    using var client = await _listener.AcceptTcpClientAsync(_stop.Token);
                    Interlocked.Increment(ref _accepted);
                }
                catch (OperationCanceledException)
                {
                    return; // Expected on dispose.
                }
                catch (ObjectDisposedException)
                {
                    return; // Listener stopped on dispose.
                }
                catch (SocketException)
                {
                    return; // Listener stopped on dispose.
                }
            }
        }
    }
}
