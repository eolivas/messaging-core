using System;
using System.Collections.Generic;
using Xunit;

namespace Messaging.Contracts.Tests;

// Header-count and correlation-id boundary edge cases for Task 2.2 (Req 4.1, 4.3).
// Happy-path smoke tests live in MessageEnvelopeTests.cs; these focus on the accept/reject
// boundaries so each case asserts a single logical outcome.
public class MessageEnvelopeEdgeCaseTests
{
    private static MessageEnvelope CreateValidEnvelope() => new()
    {
        MessageId = Guid.NewGuid(),
        CorrelationId = "corr-123",
        MessageType = "Order.Created",
        CreatedAtUtc = DateTimeOffset.UtcNow,
        Headers = new Dictionary<string, string> { ["source"] = "orders" },
        Payload = new ReadOnlyMemory<byte>(new byte[] { 1, 2, 3 }),
    };

    private static IReadOnlyDictionary<string, string> BuildHeaders(int count)
    {
        var headers = new Dictionary<string, string>(count);
        for (var i = 0; i < count; i++)
        {
            headers[$"key-{i}"] = $"value-{i}";
        }

        return headers;
    }

    // ----- Header count boundaries (Req 4.1): 0..64 inclusive accepted, 65+ rejected -----

    [Fact]
    public void Validate_HeaderCountZero_DoesNotThrow()
    {
        var envelope = CreateValidEnvelope() with { Headers = BuildHeaders(0) };

        var exception = Record.Exception(() => envelope.Validate());

        Assert.Null(exception);
    }

    [Fact]
    public void Validate_HeaderCountOne_DoesNotThrow()
    {
        var envelope = CreateValidEnvelope() with { Headers = BuildHeaders(1) };

        var exception = Record.Exception(() => envelope.Validate());

        Assert.Null(exception);
    }

    [Fact]
    public void Validate_HeaderCountSixtyFour_DoesNotThrow()
    {
        var envelope = CreateValidEnvelope() with
        {
            Headers = BuildHeaders(MessageEnvelope.MaxHeaderCount),
        };

        var exception = Record.Exception(() => envelope.Validate());

        Assert.Null(exception);
    }

    [Fact]
    public void Validate_HeaderCountSixtyFive_ThrowsInvalidEnvelopeException()
    {
        var envelope = CreateValidEnvelope() with
        {
            Headers = BuildHeaders(MessageEnvelope.MaxHeaderCount + 1),
        };

        var exception = Assert.Throws<InvalidEnvelopeException>(() => envelope.Validate());

        Assert.Equal(nameof(MessageEnvelope.Headers), exception.FieldName);
    }

    [Fact]
    public void Validate_HeaderCountOneHundred_ThrowsInvalidEnvelopeException()
    {
        var envelope = CreateValidEnvelope() with { Headers = BuildHeaders(100) };

        var exception = Assert.Throws<InvalidEnvelopeException>(() => envelope.Validate());

        Assert.Equal(nameof(MessageEnvelope.Headers), exception.FieldName);
    }

    // ----- Correlation id boundaries (Req 4.3): length 1..128 inclusive accepted -----

    [Fact]
    public void Validate_EmptyCorrelationId_ThrowsInvalidEnvelopeException()
    {
        var envelope = CreateValidEnvelope() with { CorrelationId = string.Empty };

        var exception = Assert.Throws<InvalidEnvelopeException>(() => envelope.Validate());

        Assert.Equal(nameof(MessageEnvelope.CorrelationId), exception.FieldName);
    }

    [Fact]
    public void Validate_CorrelationIdLengthOne_DoesNotThrow()
    {
        var envelope = CreateValidEnvelope() with { CorrelationId = new string('a', 1) };

        var exception = Record.Exception(() => envelope.Validate());

        Assert.Null(exception);
    }

    [Fact]
    public void Validate_CorrelationIdLength128_DoesNotThrow()
    {
        var envelope = CreateValidEnvelope() with
        {
            CorrelationId = new string('a', MessageEnvelope.MaxCorrelationIdLength),
        };

        var exception = Record.Exception(() => envelope.Validate());

        Assert.Null(exception);
    }

    [Fact]
    public void Validate_CorrelationIdLength129_ThrowsInvalidEnvelopeException()
    {
        var envelope = CreateValidEnvelope() with
        {
            CorrelationId = new string('a', MessageEnvelope.MaxCorrelationIdLength + 1),
        };

        var exception = Assert.Throws<InvalidEnvelopeException>(() => envelope.Validate());

        Assert.Equal(nameof(MessageEnvelope.CorrelationId), exception.FieldName);
    }
}
