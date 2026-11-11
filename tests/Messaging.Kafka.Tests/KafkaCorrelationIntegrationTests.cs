using Messaging.Contracts;
using Messaging.Kafka.Streaming;
using Microsoft.Extensions.Logging.Abstractions;

namespace Messaging.Kafka.Tests;

/// <summary>
/// Task 12.3: proves over a real Kafka broker that the correlation id and the other envelope fields
/// survive publish (<see cref="KafkaMessagePublisher"/> over <see cref="ConfluentKafkaProducer"/>) and
/// consume (<see cref="KafkaConsumerHost"/> + production <see cref="KafkaStreamMessageProcessor"/>)
/// unchanged (Req 5.2, 9.4, 9.5, 13.7).
/// </summary>
[Collection(KafkaCollection.Name)]
public sealed class KafkaCorrelationIntegrationTests
{
    private readonly KafkaContainerFixture _kafka;

    public KafkaCorrelationIntegrationTests(KafkaContainerFixture kafka) => _kafka = kafka;

    [Fact]
    public async Task PublishThenConsume_RealBroker_PreservesCorrelationIdAndEnvelopeFields()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var cancellationToken = cts.Token;

        var bootstrapServers = await _kafka.GetBootstrapServersAsync(cancellationToken);
        var topic = $"correlation-{Guid.NewGuid():N}";
        var settings = KafkaIntegrationFactory.Settings(bootstrapServers, topic, $"group-{Guid.NewGuid():N}");
        await KafkaTopicAdmin.CreateTopicAsync(bootstrapServers, topic, partitions: 1, cancellationToken);

        // Non-ASCII, punctuation and mixed case to prove char-for-char fidelity.
        var correlationId = $"Corr-ÄÖ漢字-{Guid.NewGuid():N}/._:~";
        var payload = new byte[] { 0, 1, 2, 3, 127, 128, 254, 255 };
        var envelope = new MessageEnvelope
        {
            MessageId = Guid.NewGuid(),
            CorrelationId = correlationId,
            MessageType = "test.contract",
            CreatedAtUtc = new DateTimeOffset(2024, 5, 6, 7, 8, 9, 123, TimeSpan.Zero),
            Headers = new Dictionary<string, string> { ["tenant"] = "acme", ["trace"] = "ünï-1" },
            Payload = payload,
        };

        using (var producer = KafkaIntegrationFactory.CreateProducer(settings))
        {
            var publisher = new KafkaMessagePublisher(producer, settings, NullLogger<KafkaMessagePublisher>.Instance);
            var result = await publisher.PublishAsync(envelope, cancellationToken);
            Assert.True(result.Succeeded, result.Error);
        }

        MessageEnvelope? received = null;
        var processor = KafkaIntegrationFactory.CreateProcessor(env =>
        {
            received = env;
            return Task.CompletedTask;
        });

        using var consumer = KafkaIntegrationFactory.CreateConsumer(settings);
        var host = new KafkaConsumerHost(consumer, processor, settings, NullLogger<KafkaConsumerHost>.Instance);
        await host.StartAsync(cancellationToken);
        try
        {
            var consumed = false;
            while (!consumed && !cancellationToken.IsCancellationRequested)
            {
                consumed = await host.ConsumeOnceAsync(cancellationToken);
            }

            Assert.True(consumed, "The published record should be consumed.");
        }
        finally
        {
            await host.StopAsync(cancellationToken);
        }

        Assert.NotNull(received);
        Assert.Equal(correlationId, received!.CorrelationId, StringComparer.Ordinal);
        Assert.Equal(envelope.MessageId, received.MessageId);
        Assert.Equal(envelope.MessageType, received.MessageType);
        Assert.Equal(envelope.CreatedAtUtc, received.CreatedAtUtc);
        Assert.Equal(payload, received.Payload.ToArray());
        foreach (var header in envelope.Headers)
        {
            Assert.True(received.Headers.TryGetValue(header.Key, out var value), $"Missing header '{header.Key}'.");
            Assert.Equal(header.Value, value, StringComparer.Ordinal);
        }
    }
}
