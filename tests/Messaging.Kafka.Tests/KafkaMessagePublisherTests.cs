using Messaging.Contracts;
using Messaging.Kafka.Streaming;
using Microsoft.Extensions.Logging.Abstractions;

namespace Messaging.Kafka.Tests;

public class KafkaMessagePublisherTests
{
    private static KafkaMessagePublisher CreatePublisher(FakeKafkaProducer producer) =>
        new(producer, KafkaTestSupport.ValidSettings(), NullLogger<KafkaMessagePublisher>.Instance);

    [Fact]
    public async Task PublishAsync_WithValidEnvelope_ProducesToTopicAndSucceeds()
    {
        var producer = new FakeKafkaProducer();
        var publisher = CreatePublisher(producer);
        var envelope = KafkaTestSupport.ValidEnvelope();

        var result = await publisher.PublishAsync(envelope, CancellationToken.None);

        Assert.True(result.Succeeded);
        Assert.Null(result.Error);
        var produced = Assert.Single(producer.Produced);
        Assert.Equal("test-topic", produced.Topic);
        Assert.Equal(envelope, produced.Envelope);
    }

    [Fact]
    public async Task PublishAsync_WithInvalidEnvelope_FailsAndDoesNotProduce()
    {
        var producer = new FakeKafkaProducer();
        var publisher = CreatePublisher(producer);
        // Empty MessageId violates the envelope invariant.
        var invalid = KafkaTestSupport.ValidEnvelope() with { MessageId = Guid.Empty };

        var result = await publisher.PublishAsync(invalid, CancellationToken.None);

        Assert.False(result.Succeeded);
        Assert.NotNull(result.Error);
        Assert.Empty(producer.Produced);
    }

    [Fact]
    public async Task PublishAsync_WhenProduceThrows_ReturnsFailedResult()
    {
        var producer = new FakeKafkaProducer(new InvalidOperationException("broker down"));
        var publisher = CreatePublisher(producer);
        var envelope = KafkaTestSupport.ValidEnvelope();

        var result = await publisher.PublishAsync(envelope, CancellationToken.None);

        Assert.False(result.Succeeded);
        Assert.Contains("Kafka produce failed", result.Error);
    }

    [Fact]
    public void Constructor_WithNullProducer_Throws()
    {
        Assert.Throws<ArgumentNullException>(() => new KafkaMessagePublisher(
            null!,
            KafkaTestSupport.ValidSettings(),
            NullLogger<KafkaMessagePublisher>.Instance));
    }
}
