using System;
using System.Collections.Generic;
using System.Text;
using Messaging.Batch.BatchProcessing;
using Messaging.Contracts;

namespace Messaging.Batch.Tests.BatchProcessing;

/// <summary>
/// Shared builders for the batch adapter tests: valid/invalid envelopes and a default settings
/// instance, so each test arranges only what it varies.
/// </summary>
internal static class BatchTestData
{
    public static BatchProviderSettings Settings(
        int windowSize = 100,
        BatchTriggerMode triggerMode = BatchTriggerMode.Scheduled) => new()
    {
        Source = "orders-batch-source",
        Destination = "orders-batch-destination",
        WindowSize = windowSize,
        TriggerMode = triggerMode,
    };

    public static MessageEnvelope ValidEnvelope(string? correlationId = null) => new()
    {
        MessageId = Guid.NewGuid(),
        CorrelationId = correlationId ?? Guid.NewGuid().ToString("N"),
        MessageType = "OrderPlaced",
        CreatedAtUtc = DateTimeOffset.UtcNow,
        Headers = new Dictionary<string, string>(),
        Payload = new ReadOnlyMemory<byte>(Encoding.UTF8.GetBytes("{}")),
        ContractVersion = 1,
    };

    /// <summary>An envelope that violates an invariant (empty MessageType) so Validate throws.</summary>
    public static MessageEnvelope InvalidEnvelope() => new()
    {
        MessageId = Guid.NewGuid(),
        CorrelationId = Guid.NewGuid().ToString("N"),
        MessageType = "   ",
        CreatedAtUtc = DateTimeOffset.UtcNow,
        Headers = new Dictionary<string, string>(),
        Payload = ReadOnlyMemory<byte>.Empty,
        ContractVersion = 1,
    };

    public static IReadOnlyList<MessageEnvelope> Window(int count)
    {
        var items = new MessageEnvelope[count];
        for (var i = 0; i < count; i++)
        {
            items[i] = ValidEnvelope();
        }

        return items;
    }
}
