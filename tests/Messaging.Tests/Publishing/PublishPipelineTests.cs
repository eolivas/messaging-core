using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Messaging.Abstractions;
using Messaging.Contracts;
using Messaging.Providers;
using Messaging.Publishing;
using Xunit;

namespace Messaging.Tests.Publishing;

// Unit tests for the publish pipeline (task 8.2).
// Covers Req 3.1 (transport-neutral publish), 3.2 (invalid envelope -> failed result, undelivered),
// and 4.4 (envelope fields preserved unchanged through delivery).
[Trait("Feature", "messaging-core")]
public class PublishPipelineTests
{
    private const MessagingMode ActiveMode = MessagingMode.PublishSubscribe;

    private static MessageEnvelope ValidEnvelope(
        Guid? messageId = null,
        string? correlationId = "order-123",
        string messageType = "test.contract",
        DateTimeOffset? createdAtUtc = null,
        IReadOnlyDictionary<string, string>? headers = null) => new()
    {
        MessageId = messageId ?? Guid.NewGuid(),
        CorrelationId = correlationId!,
        MessageType = messageType,
        CreatedAtUtc = createdAtUtc ?? DateTimeOffset.UtcNow,
        Headers = headers ?? new Dictionary<string, string>(),
        Payload = new ReadOnlyMemory<byte>(new byte[] { 1, 2, 3 }),
    };

    private static (PublishPipeline Pipeline, CapturingMessagingProvider Provider) BuildPipeline(
        PublishResult? providerResult = null)
    {
        var provider = new CapturingMessagingProvider(ActiveMode, providerResult);
        var registry = new ProviderRegistry(new IMessagingProvider[] { provider });
        var pipeline = new PublishPipeline(new CorrelationIdGate(), registry, ActiveMode);
        return (pipeline, provider);
    }

    [Fact]
    public async Task PublishAsync_ValidEnvelope_DelegatesToResolvedProvider()
    {
        var (pipeline, provider) = BuildPipeline();
        var envelope = ValidEnvelope();

        var result = await pipeline.PublishAsync(envelope, CancellationToken.None);

        Assert.True(result.Succeeded);
        Assert.Single(provider.DeliveredEnvelopes);
    }

    [Fact]
    public async Task PublishAsync_ValidEnvelope_PreservesAllFieldsUnchanged()
    {
        var (pipeline, provider) = BuildPipeline();
        var messageId = Guid.NewGuid();
        var createdAt = new DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.Zero);
        var headers = new Dictionary<string, string> { ["k1"] = "v1", ["k2"] = "v2" };
        var envelope = ValidEnvelope(
            messageId: messageId,
            correlationId: "corr-abc",
            messageType: "orders.created",
            createdAtUtc: createdAt,
            headers: headers);

        await pipeline.PublishAsync(envelope, CancellationToken.None);

        var delivered = Assert.Single(provider.DeliveredEnvelopes);
        Assert.Equal(messageId, delivered.MessageId);
        Assert.Equal("corr-abc", delivered.CorrelationId);
        Assert.Equal("orders.created", delivered.MessageType);
        Assert.Equal(createdAt, delivered.CreatedAtUtc);
        Assert.Equal(headers, delivered.Headers);
        Assert.True(envelope.Payload.Span.SequenceEqual(delivered.Payload.Span));
        Assert.Equal(envelope.ContractVersion, delivered.ContractVersion);
    }

    [Fact]
    public async Task PublishAsync_AbsentCorrelationId_GeneratesOneAndDelivers()
    {
        var (pipeline, provider) = BuildPipeline();
        var envelope = ValidEnvelope(correlationId: null);

        var result = await pipeline.PublishAsync(envelope, CancellationToken.None);

        Assert.True(result.Succeeded);
        var delivered = Assert.Single(provider.DeliveredEnvelopes);
        Assert.False(string.IsNullOrWhiteSpace(delivered.CorrelationId));
        Assert.InRange(delivered.CorrelationId.Length, 1, MessageEnvelope.MaxCorrelationIdLength);
    }

    [Fact]
    public async Task PublishAsync_OverlongCorrelationId_ReturnsFailedResultUndelivered()
    {
        var (pipeline, provider) = BuildPipeline();
        var overlong = new string('c', MessageEnvelope.MaxCorrelationIdLength + 1);
        var envelope = ValidEnvelope(correlationId: overlong);

        var result = await pipeline.PublishAsync(envelope, CancellationToken.None);

        Assert.False(result.Succeeded);
        Assert.NotNull(result.Error);
        Assert.Empty(provider.DeliveredEnvelopes);
    }

    [Fact]
    public async Task PublishAsync_EmptyMessageId_ReturnsFailedResultUndelivered()
    {
        var (pipeline, provider) = BuildPipeline();
        var envelope = ValidEnvelope(messageId: Guid.Empty);

        var result = await pipeline.PublishAsync(envelope, CancellationToken.None);

        Assert.False(result.Succeeded);
        Assert.NotNull(result.Error);
        Assert.Empty(provider.DeliveredEnvelopes);
    }

    [Fact]
    public async Task PublishAsync_EmptyMessageType_ReturnsFailedResultUndelivered()
    {
        var (pipeline, provider) = BuildPipeline();
        var envelope = ValidEnvelope(messageType: "   ");

        var result = await pipeline.PublishAsync(envelope, CancellationToken.None);

        Assert.False(result.Succeeded);
        Assert.Empty(provider.DeliveredEnvelopes);
    }

    [Fact]
    public async Task PublishAsync_NonUtcTimestamp_ReturnsFailedResultUndelivered()
    {
        var (pipeline, provider) = BuildPipeline();
        var envelope = ValidEnvelope(createdAtUtc: new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.FromHours(2)));

        var result = await pipeline.PublishAsync(envelope, CancellationToken.None);

        Assert.False(result.Succeeded);
        Assert.Empty(provider.DeliveredEnvelopes);
    }

    [Fact]
    public async Task PublishAsync_ProviderReportsFailure_ReturnsProviderResult()
    {
        var (pipeline, provider) = BuildPipeline(PublishResult.Fail("transport down"));
        var envelope = ValidEnvelope();

        var result = await pipeline.PublishAsync(envelope, CancellationToken.None);

        Assert.False(result.Succeeded);
        Assert.Equal("transport down", result.Error);
        // The envelope was still valid, so it was handed to the provider.
        Assert.Single(provider.DeliveredEnvelopes);
    }

    [Fact]
    public async Task PublishAsync_NullEnvelope_ThrowsArgumentNull()
    {
        var (pipeline, _) = BuildPipeline();

        await Assert.ThrowsAsync<ArgumentNullException>(
            () => pipeline.PublishAsync(null!, CancellationToken.None));
    }

    [Fact]
    public void Constructor_NullCorrelationIdGate_ThrowsArgumentNull()
    {
        var registry = new ProviderRegistry(Array.Empty<IMessagingProvider>());

        Assert.Throws<ArgumentNullException>(
            () => new PublishPipeline(null!, registry, ActiveMode));
    }

    [Fact]
    public void Constructor_NullProviderRegistry_ThrowsArgumentNull()
    {
        Assert.Throws<ArgumentNullException>(
            () => new PublishPipeline(new CorrelationIdGate(), null!, ActiveMode));
    }

    [Fact]
    public async Task PublishAsync_UnresolvedMode_FailsFast()
    {
        // Registry has no provider for the active mode -> resolution throws (Req 1.6, 3.7).
        var registry = new ProviderRegistry(Array.Empty<IMessagingProvider>());
        var pipeline = new PublishPipeline(new CorrelationIdGate(), registry, ActiveMode);

        await Assert.ThrowsAsync<ProviderResolutionException>(
            () => pipeline.PublishAsync(ValidEnvelope(), CancellationToken.None));
    }
}
