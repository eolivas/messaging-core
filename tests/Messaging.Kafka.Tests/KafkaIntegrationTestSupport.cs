using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Messaging.Abstractions;
using Messaging.Configuration;
using Messaging.Consuming;
using Messaging.Contracts;
using Messaging.Kafka.Streaming;
using Messaging.Observability;
using Messaging.Resilience;
using Microsoft.Extensions.Logging.Abstractions;

namespace Messaging.Kafka.Tests;

/// <summary>
/// Builds the production Kafka adapters (<see cref="ConfluentKafkaProducer"/>,
/// <see cref="ConfluentKafkaConsumer"/>, <see cref="KafkaStreamMessageProcessor"/>) for the
/// containerized integration tests. Clients are configured through <see cref="KafkaClientConfigFactory"/>
/// with default Plaintext security, matching the local docker-compose / Testcontainers broker.
/// </summary>
internal static class KafkaIntegrationFactory
{
    /// <summary>Provider settings pointing at the container broker for a unique topic and group.</summary>
    public static KafkaProviderSettings Settings(string bootstrapServers, string topic, string consumerGroupId) => new()
    {
        BootstrapServers = bootstrapServers,
        ConsumerGroupId = consumerGroupId,
        Topic = topic,
    };

    public static KafkaClientConfigFactory ConfigFactory(KafkaProviderSettings settings) =>
        new(settings, new KafkaSecuritySettings());

    public static ConfluentKafkaProducer CreateProducer(KafkaProviderSettings settings) =>
        new(ConfigFactory(settings), settings, NullLogger<ConfluentKafkaProducer>.Instance);

    public static ConfluentKafkaConsumer CreateConsumer(KafkaProviderSettings settings, TimeSpan? pollTimeout = null) =>
        new(ConfigFactory(settings), NullLogger<ConfluentKafkaConsumer>.Instance, pollTimeout);

    /// <summary>
    /// Builds the production <see cref="KafkaStreamMessageProcessor"/> over real core collaborators with
    /// no-op observability, delivering to <paramref name="handler"/> and routing dead letters to
    /// <paramref name="deadLetters"/>. Retries use a no-op delay so tests do not wait on backoff.
    /// </summary>
    public static KafkaStreamMessageProcessor CreateProcessor(
        Func<MessageEnvelope, Task> handler,
        IDeadLetterSink? deadLetters = null,
        int maxAttempts = 1)
    {
        var observability = new ObservabilityContext(
            new NoOpLog(),
            new NoOpMetrics(),
            new MessagingTraceSource(tracingEnabled: false),
            new PayloadRedactor());

        var retryPolicy = new RetryPolicyConfig { MaxAttempts = maxAttempts };
        var router = new DeadLetterRouter(
            deadLetters ?? new CapturingDeadLetterSink(),
            new NoOpDeadLetterMetrics(),
            new NoOpDeadLetterLog());
        var retryExecutor = new RetryExecutor(
            new FailureClassifier(),
            new BackoffCalculator(static (min, _) => min),
            router,
            delay: static (_, _) => Task.CompletedTask);

        var pipeline = new ConsumePipeline(
            new ConsumerHostStateMachine(),
            new AcceptAllBinder(),
            new DelegateInvoker(handler));
        pipeline.StartAsync(CancellationToken.None).GetAwaiter().GetResult();

        return new KafkaStreamMessageProcessor(observability, retryExecutor, pipeline, router, retryPolicy);
    }

    /// <summary>Dead-letter sink that records every message it receives.</summary>
    public sealed class CapturingDeadLetterSink : IDeadLetterSink
    {
        private readonly List<DeadLetterMessage> _messages = new();

        public IReadOnlyList<DeadLetterMessage> Messages
        {
            get
            {
                lock (_messages)
                {
                    return _messages.ToArray();
                }
            }
        }

        public Task SendAsync(DeadLetterMessage message, CancellationToken cancellationToken)
        {
            lock (_messages)
            {
                _messages.Add(message);
            }

            return Task.CompletedTask;
        }
    }

    private sealed class AcceptAllBinder : IContractBinder
    {
        public bool IsKnown(string messageType) => true;

        public object Bind(MessageEnvelope envelope) => new object();
    }

    private sealed class DelegateInvoker : IHandlerInvoker
    {
        private readonly Func<MessageEnvelope, Task> _handler;

        public DelegateInvoker(Func<MessageEnvelope, Task> handler) => _handler = handler;

        public Task InvokeAsync(
            MessageEnvelope envelope,
            object boundMessage,
            ConsumeContext context,
            CancellationToken cancellationToken) => _handler(envelope);
    }

    private sealed class NoOpLog : IMessagingLog
    {
        public void MessageEvent(MessagingEventType eventType, string correlationId, DateTimeOffset timestampUtc)
        {
        }
    }

    private sealed class NoOpMetrics : IMessagingMetrics
    {
        public void RecordPublished()
        {
        }

        public void RecordConsumed()
        {
        }

        public void RecordRetry()
        {
        }

        public void RecordDeadLetter()
        {
        }
    }

    private sealed class NoOpDeadLetterMetrics : IDeadLetterMetrics
    {
        public void RecordDeadLetter()
        {
        }
    }

    private sealed class NoOpDeadLetterLog : IDeadLetterLog
    {
        public void DeadLettered(string correlationId, string failureReason, int attemptCount)
        {
        }

        public void DeadLetterRoutingFailed(string correlationId, string failureReason, int attemptCount, Exception error)
        {
        }
    }
}

/// <summary>
/// Minimal admin helper that creates the test topic before producing, so the first produce/consume
/// does not race topic auto-creation.
/// </summary>
internal static class KafkaTopicAdmin
{
    public static async Task CreateTopicAsync(
        string bootstrapServers,
        string topic,
        int partitions = 1,
        CancellationToken cancellationToken = default)
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig
        {
            BootstrapServers = bootstrapServers,
        }).Build();

        try
        {
            await admin.CreateTopicsAsync(new[]
            {
                new TopicSpecification
                {
                    Name = topic,
                    NumPartitions = partitions,
                    ReplicationFactor = 1,
                },
            }).ConfigureAwait(false);
        }
        catch (CreateTopicsException ex) when (ex.Results.All(r => r.Error.Code == ErrorCode.TopicAlreadyExists))
        {
            // Idempotent: a prior test already created the topic.
        }
    }
}
