using Messaging.Contracts;
using Messaging.Kafka.Streaming;
using Microsoft.Extensions.Logging.Abstractions;

namespace Messaging.Kafka.Tests;

/// <summary>
/// Task 12.4 (Req 4.2, 4.6, 6.1, 13.8): against a real broker, a record processed successfully by the
/// production <see cref="KafkaStreamMessageProcessor"/> has its offset committed, so a newly started
/// consumer in the same group is not redelivered it.
/// </summary>
[Collection(KafkaCollection.Name)]
public sealed class KafkaSuccessPathIntegrationTests
{
    // Quiet window observed after the second consumer is proven to hold its partition assignment.
    private static readonly TimeSpan ObservationWindow = TimeSpan.FromSeconds(30);

    private readonly KafkaContainerFixture _kafka;

    public KafkaSuccessPathIntegrationTests(KafkaContainerFixture kafka) => _kafka = kafka;

    [Fact]
    public async Task SuccessfullyProcessedRecord_IsCommitted_AndNotRedeliveredToNewConsumerInSameGroup()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(4));
        var ct = cts.Token;

        var bootstrapServers = await _kafka.GetBootstrapServersAsync(ct);
        var topic = $"success-commit-{Guid.NewGuid():N}";
        var settings = KafkaIntegrationFactory.Settings(bootstrapServers, topic, $"group-{Guid.NewGuid():N}");
        await KafkaTopicAdmin.CreateTopicAsync(bootstrapServers, topic, partitions: 1, ct);

        var first = NewEnvelope("first");
        await PublishAsync(settings, first, ct);

        // Act 1: first consumer processes successfully (production processor) and commits.
        var firstSeen = new List<Guid>();
        using (var consumer = KafkaIntegrationFactory.CreateConsumer(settings))
        {
            var processor = KafkaIntegrationFactory.CreateProcessor(env =>
            {
                lock (firstSeen)
                {
                    firstSeen.Add(env.MessageId);
                }

                return Task.CompletedTask;
            });
            var host = new KafkaConsumerHost(consumer, processor, settings, NullLogger<KafkaConsumerHost>.Instance);
            await host.StartAsync(ct);
            try
            {
                Assert.True(await ConsumeUntilAsync(host, () => firstSeen.Count > 0, ct),
                    "The first consumer should receive and process the record.");
            }
            finally
            {
                await host.StopAsync(ct);
            }
        }

        Assert.Equal(new[] { first.MessageId }, firstSeen);

        // Act 2: second consumer, same group. Publish a marker AFTER it starts. Single partition means
        // the marker arrives after any redelivered record, so receiving the marker first proves both
        // that the consumer holds its assignment and that the first record was not redelivered.
        var marker = NewEnvelope("marker");
        var secondSeen = new List<Guid>();
        using (var consumer = KafkaIntegrationFactory.CreateConsumer(settings))
        {
            var processor = KafkaIntegrationFactory.CreateProcessor(env =>
            {
                lock (secondSeen)
                {
                    secondSeen.Add(env.MessageId);
                }

                return Task.CompletedTask;
            });
            var host = new KafkaConsumerHost(consumer, processor, settings, NullLogger<KafkaConsumerHost>.Instance);
            await host.StartAsync(ct);
            try
            {
                await PublishAsync(settings, marker, ct);

                Assert.True(await ConsumeUntilAsync(host, () => secondSeen.Contains(marker.MessageId), ct),
                    "The second consumer should receive the marker record.");

                // Continue polling for the observation window after assignment is proven.
                var deadline = DateTime.UtcNow + ObservationWindow;
                while (DateTime.UtcNow < deadline)
                {
                    await host.ConsumeOnceAsync(ct);
                }
            }
            finally
            {
                await host.StopAsync(ct);
            }
        }

        Assert.Equal(new[] { marker.MessageId }, secondSeen);
    }

    private static MessageEnvelope NewEnvelope(string label) => new()
    {
        MessageId = Guid.NewGuid(),
        CorrelationId = $"corr-{label}-{Guid.NewGuid():N}",
        MessageType = "test.contract",
        CreatedAtUtc = DateTimeOffset.UtcNow,
        Headers = new Dictionary<string, string>(),
        Payload = new byte[] { 1, 2, 3 },
    };

    private static async Task PublishAsync(KafkaProviderSettings settings, MessageEnvelope envelope, CancellationToken ct)
    {
        using var producer = KafkaIntegrationFactory.CreateProducer(settings);
        var publisher = new KafkaMessagePublisher(producer, settings, NullLogger<KafkaMessagePublisher>.Instance);
        var result = await publisher.PublishAsync(envelope, ct);
        Assert.True(result.Succeeded, result.Error);
    }

    private static async Task<bool> ConsumeUntilAsync(KafkaConsumerHost host, Func<bool> done, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            await host.ConsumeOnceAsync(ct);
            if (done())
            {
                return true;
            }
        }

        return false;
    }
}
