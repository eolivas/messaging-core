using System;
using System.Collections.Generic;
using Xunit;

namespace Messaging.Contracts.Tests;

// Happy-path construction and validation smoke tests for Task 2.1.
// Exhaustive header-count and field edge cases are covered separately in Task 2.2.
public class MessageEnvelopeTests
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

    [Fact]
    public void Validate_ValidEnvelope_DoesNotThrow()
    {
        var envelope = CreateValidEnvelope();

        var exception = Record.Exception(() => envelope.Validate());

        Assert.Null(exception);
    }

    [Fact]
    public void ContractVersion_WhenNotSet_DefaultsToOne()
    {
        var envelope = CreateValidEnvelope();

        Assert.Equal(1, envelope.ContractVersion);
    }

    [Fact]
    public void Validate_EmptyHeaders_DoesNotThrow()
    {
        var envelope = CreateValidEnvelope() with
        {
            Headers = new Dictionary<string, string>(),
        };

        var exception = Record.Exception(() => envelope.Validate());

        Assert.Null(exception);
    }

    [Fact]
    public void Validate_EmptyMessageId_ThrowsInvalidEnvelopeException()
    {
        var envelope = CreateValidEnvelope() with { MessageId = Guid.Empty };

        var exception = Assert.Throws<InvalidEnvelopeException>(() => envelope.Validate());

        Assert.Equal(nameof(MessageEnvelope.MessageId), exception.FieldName);
    }
}
