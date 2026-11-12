using Messaging.Abstractions;
using Messaging.Consuming;
using Messaging.Contracts;
using Messaging.Kafka.Streaming;
using Microsoft.Extensions.Logging.Abstractions;

namespace Messaging.Kafka.Tests;

/// <summary>
/// Streaming (Kafka) at-least-once integration test (task 22.3), run against a <b>real</b> Kafka
/// broker in a Testcontainer. It exercises the actual adapter shells
/// (<see cref="KafkaMessagePublisher"/>, <see cref="KafkaConsumerHost"/>) over native
/// Confluent.Kafka producer/consumer, proving two guarantees end-to-end:
/// <list type="number">
/// <item><b>At-least-once (Req 6.7):</b> a record whose processing fails before the offset is
/// committed is not lost — a consumer in the same group redelivers it after restart, because the
/// host commits manually and only after the pipeline completes.</item>
/// <item><b>Correlation id propagation (Req 9.4):</b> the correlation id the producer sent is the
/// exact correlation id the consumer receives, unchanged across the real transport.</item>
/// </list>
/// The one-engine / real-dependency rule forbids a fake broker here: offset-commit redelivery is a
/// broker behaviour, so only a real Kafka demonstrates it.
/// </summary>
[Collection(KafkaCollection.Name)]
public sealed class KafkaAtLeastOnceIntegrationTests
{
    private readonly KafkaContainerFixture _kafka;

    public KafkaAtLeastOnceIntegrationTests(KafkaContainerFixture kafka) => _kafka = kafka;

    [Fact]
    public async Task ProcessingFailureBeforeCommit_RedeliversRecord_AndPropagatesCorrelationId()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var cancellationToken = cts.Token;

        var bootstrapServers = await _kafka.GetBootstrapServersAsync(cancellationToken);

        // A unique topic and consumer group per test run keep this test independent of any other
        // test's offsets or records (no shared state across tests).
        var topic = $"at-least-once-{Guid.NewGuid():N}";
        var consumerGroupId = $"group-{Guid.NewGuid():N}";
        await KafkaTopicAdmin.CreateTopicAsync(bootstrapServers, topic, partitions: 1, cancellationToken);

        var settings = new KafkaProviderSettings
        {
            BootstrapServers = bootstrapServers,
            ConsumerGroupId = consumerGroupId,
            Topic = topic,
        };

        var sentCorrelationId = $"corr-{Guid.NewGuid():N}";
        var payload = new byte[] { 10, 20, 30, 40 };
        var envelope = new MessageEnvelope
        {
            MessageId = Guid.NewGuid(),
            CorrelationId = sentCorrelationId,
            MessageType = "test.contract",
            CreatedAtUtc = DateTimeOffset.UtcNow,
            Headers = new Dictionary<string, string> { ["tenant"] = "acme" },
            Payload = payload,
        };

        // Arrange: produce the message through the real publisher shell over a native producer.
        using (var producer = KafkaIntegrationFactory.CreateProducer(settings))
        {
            var publisher = new KafkaMessagePublisher(producer, settings, NullLogger<KafkaMessagePublisher>.Instance);
            var publishResult = await publisher.PublishAsync(envelope, cancellationToken);
            Assert.True(publishResult.Succeeded, publishResult.Error);
        }

        // Act 1 — first consumer through the PRODUCTION KafkaStreamMessageProcessor: the handler fails
        // and the dead-letter sink also fails, so the processor throws RetainOffsetException and the
        // host never reaches CommitAsync. The offset stays uncommitted (Req 4.5, 6.4).
        MessageEnvelope? firstAttemptEnvelope = null;
        using (var consumer = KafkaIntegrationFactory.CreateConsumer(settings))
        {
            var failingProcessor = KafkaIntegrationFactory.CreateProcessor(
                env =>
                {
                    firstAttemptEnvelope = env;
                    throw new InvalidOperationException("Simulated pipeline failure before commit.");
                },
                new FailingDeadLetterSink(),
                maxAttempts: 1);
            var host = new KafkaConsumerHost(consumer, failingProcessor, settings, NullLogger<KafkaConsumerHost>.Instance);

            await host.StartAsync(cancellationToken);
            try
            {
                await Assert.ThrowsAsync<RetainOffsetException>(() => ConsumeUntilRecordAsync(host, cancellationToken));
            }
            finally
            {
                // Leave the group WITHOUT committing.
                await host.StopAsync(cancellationToken);
            }
        }

        Assert.NotNull(firstAttemptEnvelope);
        Assert.Equal(sentCorrelationId, firstAttemptEnvelope!.CorrelationId);

        // Act 2 — second consumer in the SAME group must receive the same record within 60s (Req 13.6).
        MessageEnvelope? redeliveredEnvelope = null;
        using var redeliveryCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        redeliveryCts.CancelAfter(TimeSpan.FromSeconds(60));
        using (var consumer = KafkaIntegrationFactory.CreateConsumer(settings))
        {
            var succeedingProcessor = KafkaIntegrationFactory.CreateProcessor(
                env =>
                {
                    redeliveredEnvelope = env;
                    return Task.CompletedTask;
                });
            var host = new KafkaConsumerHost(consumer, succeedingProcessor, settings, NullLogger<KafkaConsumerHost>.Instance);

            await host.StartAsync(cancellationToken);
            try
            {
                var consumed = await ConsumeUntilRecordAsync(host, redeliveryCts.Token);
                Assert.True(consumed, "The uncommitted record should be redelivered within 60s to a new consumer in the same group.");
            }
            finally
            {
                await host.StopAsync(cancellationToken);
            }
        }

        // Assert: the record was redelivered (not lost) and carried the same correlation id end-to-end.
        Assert.NotNull(redeliveredEnvelope);
        Assert.Equal(sentCorrelationId, redeliveredEnvelope!.CorrelationId);
        Assert.Equal(envelope.MessageId, redeliveredEnvelope.MessageId);
        Assert.Equal(envelope.MessageType, redeliveredEnvelope.MessageType);
        Assert.Equal(payload, redeliveredEnvelope.Payload.ToArray());
    }

    /// <summary>
    /// Drives <see cref="KafkaConsumerHost.ConsumeOnceAsync"/> until it processes one record or the
    /// token is cancelled. A real broker may return empty polls before the record is assigned, so an
    /// empty poll is retried rather than treated as the end.
    /// </summary>
    private static async Task<bool> ConsumeUntilRecordAsync(KafkaConsumerHost host, CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                if (await host.ConsumeOnceAsync(cancellationToken))
                {
                    return true;
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return false;
            }
        }

        return false;
    }

    /// <summary>A dead-letter sink that always fails, forcing the processor to retain the offset.</summary>
    private sealed class FailingDeadLetterSink : IDeadLetterSink
    {
        public Task SendAsync(DeadLetterMessage message, CancellationToken cancellationToken) =>
            throw new InvalidOperationException("Dead-letter sink unavailable.");
    }
}
