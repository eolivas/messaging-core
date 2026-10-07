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
        using (var producer = new ConfluentKafkaProducer(bootstrapServers))
        {
            var publisher = new KafkaMessagePublisher(producer, settings, NullLogger<KafkaMessagePublisher>.Instance);
            var publishResult = await publisher.PublishAsync(envelope, cancellationToken);
            Assert.True(publishResult.Succeeded, publishResult.Error);
        }

        // Act 1 — first consumer: processing throws BEFORE the host commits the offset, so the record
        // stays uncommitted. The host must surface the failure and leave nothing committed (Req 6.7).
        MessageEnvelope? firstAttemptEnvelope = null;
        using (var consumer = new ConfluentKafkaConsumer(bootstrapServers, consumerGroupId))
        {
            var failingProcessor = new DelegatingStreamProcessor(env =>
            {
                firstAttemptEnvelope = env;
                throw new InvalidOperationException("Simulated pipeline failure before commit.");
            });
            var host = new KafkaConsumerHost(consumer, failingProcessor, settings, NullLogger<KafkaConsumerHost>.Instance);

            await host.StartAsync(cancellationToken);
            await Assert.ThrowsAsync<InvalidOperationException>(() => ConsumeUntilRecordAsync(host, cancellationToken));
            // Leave the group WITHOUT committing. StopAsync closes the consumer; the offset was never
            // committed, so the broker still regards the record as undelivered for this group.
            await host.StopAsync(cancellationToken);
        }

        // The first consumer did receive the record (it just failed to process it): correlation id
        // propagated across the real transport on this first delivery too (Req 9.4).
        Assert.NotNull(firstAttemptEnvelope);
        Assert.Equal(sentCorrelationId, firstAttemptEnvelope!.CorrelationId);

        // Act 2 — second consumer in the SAME group: because the offset was never committed, the
        // broker redelivers the uncommitted record. This is the at-least-once guarantee (Req 6.7).
        MessageEnvelope? redeliveredEnvelope = null;
        using (var consumer = new ConfluentKafkaConsumer(bootstrapServers, consumerGroupId))
        {
            var succeedingProcessor = new DelegatingStreamProcessor(env => redeliveredEnvelope = env);
            var host = new KafkaConsumerHost(consumer, succeedingProcessor, settings, NullLogger<KafkaConsumerHost>.Instance);

            await host.StartAsync(cancellationToken);
            try
            {
                var consumed = await ConsumeUntilRecordAsync(host, cancellationToken);
                Assert.True(consumed, "The uncommitted record should be redelivered to a new consumer in the same group.");
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
            if (await host.ConsumeOnceAsync(cancellationToken))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// A test processor that runs an arbitrary action per envelope — lets a test fail the first
    /// delivery and succeed the redelivery without a bespoke fake per case.
    /// </summary>
    private sealed class DelegatingStreamProcessor : IStreamMessageProcessor
    {
        private readonly Action<MessageEnvelope> _onProcess;

        public DelegatingStreamProcessor(Action<MessageEnvelope> onProcess) => _onProcess = onProcess;

        public Task ProcessAsync(MessageEnvelope envelope, CancellationToken cancellationToken)
        {
            _onProcess(envelope);
            return Task.CompletedTask;
        }
    }
}
