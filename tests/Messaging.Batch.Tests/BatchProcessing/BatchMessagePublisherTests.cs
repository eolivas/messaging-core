using System;
using System.Threading;
using System.Threading.Tasks;
using Messaging.Batch.BatchProcessing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Messaging.Batch.Tests.BatchProcessing;

public class BatchMessagePublisherTests
{
    private static BatchMessagePublisher CreatePublisher(FakeBatchDestinationWriter writer) =>
        new(writer, NullLogger<BatchMessagePublisher>.Instance);

    [Fact]
    public async Task PublishAsync_WithValidEnvelope_WritesToDestinationAndSucceeds()
    {
        var writer = new FakeBatchDestinationWriter();
        var publisher = CreatePublisher(writer);
        var envelope = BatchTestData.ValidEnvelope();

        var result = await publisher.PublishAsync(envelope, CancellationToken.None);

        Assert.True(result.Succeeded);
        Assert.Null(result.Error);
        Assert.Single(writer.Written);
        Assert.Equal(envelope, writer.Written[0]);
    }

    [Fact]
    public async Task PublishAsync_WithInvalidEnvelope_FailsAndNeverWrites()
    {
        var writer = new FakeBatchDestinationWriter();
        var publisher = CreatePublisher(writer);

        var result = await publisher.PublishAsync(BatchTestData.InvalidEnvelope(), CancellationToken.None);

        Assert.False(result.Succeeded);
        Assert.NotNull(result.Error);
        Assert.Empty(writer.Written);
    }

    [Fact]
    public async Task PublishAsync_WhenDestinationWriteFails_FailsAndIsUndelivered()
    {
        var writer = new FakeBatchDestinationWriter(new InvalidOperationException("destination down"));
        var publisher = CreatePublisher(writer);

        var result = await publisher.PublishAsync(BatchTestData.ValidEnvelope(), CancellationToken.None);

        Assert.False(result.Succeeded);
        Assert.Contains("destination down", result.Error);
        Assert.Empty(writer.Written);
    }
}
