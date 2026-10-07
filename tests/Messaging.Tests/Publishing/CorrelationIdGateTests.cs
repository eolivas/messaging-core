using System;
using System.Collections.Generic;
using Messaging.Contracts;
using Messaging.Publishing;
using Xunit;

namespace Messaging.Tests.Publishing;

// Unit tests for the correlation id generation and validation gate (task 8.1).
// Covers Req 4.2 (generate when absent), 4.3 (reject empty/overlong), 9.3 (generated id attached).
[Trait("Feature", "messaging-core")]
public class CorrelationIdGateTests
{
    private readonly CorrelationIdGate _gate = new();

    private static MessageEnvelope EnvelopeWithCorrelationId(string correlationId) => new()
    {
        MessageId = Guid.NewGuid(),
        CorrelationId = correlationId,
        MessageType = "test.contract",
        CreatedAtUtc = DateTimeOffset.UtcNow,
        Headers = new Dictionary<string, string>(),
        Payload = ReadOnlyMemory<byte>.Empty,
    };

    [Fact]
    public void EnsureCorrelationId_NullCorrelationId_GeneratesAndAttaches()
    {
        var envelope = EnvelopeWithCorrelationId(null!);

        var result = _gate.EnsureCorrelationId(envelope);

        Assert.False(string.IsNullOrWhiteSpace(result.CorrelationId));
        Assert.InRange(result.CorrelationId.Length, 1, MessageEnvelope.MaxCorrelationIdLength);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("\t")]
    public void EnsureCorrelationId_AbsentCorrelationId_GeneratesValidId(string absent)
    {
        var envelope = EnvelopeWithCorrelationId(absent);

        var result = _gate.EnsureCorrelationId(envelope);

        Assert.False(string.IsNullOrWhiteSpace(result.CorrelationId));
        Assert.InRange(result.CorrelationId.Length, 1, MessageEnvelope.MaxCorrelationIdLength);
    }

    [Fact]
    public void EnsureCorrelationId_GeneratedEnvelope_PassesEnvelopeValidation()
    {
        var envelope = EnvelopeWithCorrelationId(null!);

        var result = _gate.EnsureCorrelationId(envelope);

        // The generated id must satisfy the envelope's own correlation-id invariant.
        var exception = Record.Exception(() => result.Validate());
        Assert.Null(exception);
    }

    [Fact]
    public void EnsureCorrelationId_PresentValidCorrelationId_PreservedUnchanged()
    {
        var envelope = EnvelopeWithCorrelationId("order-123");

        var result = _gate.EnsureCorrelationId(envelope);

        Assert.Equal("order-123", result.CorrelationId);
        Assert.Same(envelope, result);
    }

    [Fact]
    public void EnsureCorrelationId_MaxLengthCorrelationId_Accepted()
    {
        var atMax = new string('c', MessageEnvelope.MaxCorrelationIdLength);
        var envelope = EnvelopeWithCorrelationId(atMax);

        var result = _gate.EnsureCorrelationId(envelope);

        Assert.Equal(atMax, result.CorrelationId);
    }

    [Fact]
    public void EnsureCorrelationId_OverlongCorrelationId_ThrowsInvalidCorrelationId()
    {
        var overlong = new string('c', MessageEnvelope.MaxCorrelationIdLength + 1);
        var envelope = EnvelopeWithCorrelationId(overlong);

        var exception = Assert.Throws<InvalidCorrelationIdException>(
            () => _gate.EnsureCorrelationId(envelope));

        Assert.Equal(overlong, exception.SuppliedCorrelationId);
    }

    [Fact]
    public void EnsureCorrelationId_DoesNotMutateInputEnvelope()
    {
        var envelope = EnvelopeWithCorrelationId(null!);

        _gate.EnsureCorrelationId(envelope);

        Assert.Null(envelope.CorrelationId);
    }

    [Fact]
    public void EnsureCorrelationId_NullEnvelope_ThrowsArgumentNull()
    {
        Assert.Throws<ArgumentNullException>(() => _gate.EnsureCorrelationId(null!));
    }

    [Fact]
    public void GenerateCorrelationId_ProducesDistinctGloballyUniqueIds()
    {
        var ids = new HashSet<string>();
        for (var i = 0; i < 1000; i++)
        {
            Assert.True(ids.Add(CorrelationIdGate.GenerateCorrelationId()));
        }
    }
}
