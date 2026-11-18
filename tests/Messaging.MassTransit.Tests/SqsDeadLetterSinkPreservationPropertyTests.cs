using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FsCheck;
using FsCheck.Fluent;
using FsCheck.Xunit;
using Messaging.Abstractions;
using Messaging.Contracts;
using Messaging.MassTransit.AmazonSqs;
using Messaging.MassTransit.Transport;
using Xunit;

namespace Messaging.MassTransit.Tests;

// Feature: sns-sqs-provider, Property 11: Dead-letter sink preserves the original message
// Validates: Requirements 10.6.
//
// For any DeadLetterMessage, the EnvelopeMessage SqsDeadLetterSink sends equals the original
// envelope in message id, correlation id, message type, timestamp, contract version, headers and
// every payload byte. The dead-letter context travels as attributes: the reason (truncated to
// 1,024 characters), attempt count, dead-lettered-at, correlation id and (when present) the
// idempotency key. No attribute carries payload bytes. In FIFO mode the group id follows the
// publish rule and the deduplication id is "{messageId}:dlq".
[Trait("Feature", "sns-sqs-provider")]
[Trait("Property", "11")]
public class SqsDeadLetterSinkPreservationPropertyTests
{
    private const string QueueName = "orders-dlq";

    public sealed record DeadLetterCase(
        MessageEnvelope Envelope,
        string Reason,
        int AttemptCount,
        string? IdempotencyKey,
        bool Fifo,
        FifoGroupSelection Selection,
        DateTimeOffset DeadLetteredAt);

    // Feature: sns-sqs-provider, Property 11: Dead-letter sink preserves the original message
    [Property(MaxTest = 100, QuietOnSuccess = true, Arbitrary = new[] { typeof(SqsDeadLetterSinkPreservationPropertyTests) })]
    public Property SendAsync_PreservesOriginalEnvelopeAndCarriesContextAttributes(DeadLetterCase c)
    {
        var sender = new CapturingSender();
        var sink = new SqsDeadLetterSink(sender, QueueName, c.Fifo, c.Selection);
        var message = new DeadLetterMessage(
            c.Envelope, c.Envelope.CorrelationId, c.IdempotencyKey, c.Reason, c.AttemptCount, c.DeadLetteredAt);

        sink.SendAsync(message, CancellationToken.None).GetAwaiter().GetResult();

        var sent = sender.Calls.Single();
        var original = c.Envelope;
        var body = sent.Message;

        var bodyPreserved =
            body.MessageId == original.MessageId
            && body.CorrelationId == original.CorrelationId
            && body.MessageType == original.MessageType
            && body.CreatedAtUtc == original.CreatedAtUtc
            && body.ContractVersion == original.ContractVersion
            && body.Headers.Count == original.Headers.Count
            && original.Headers.All(h => body.Headers.TryGetValue(h.Key, out var v) && v == h.Value)
            && body.Payload.Length == original.Payload.Length
            && body.Payload.AsSpan().SequenceEqual(original.Payload.Span);

        var attrs = sent.Attributes;
        var expectedKeys = new HashSet<string>(StringComparer.Ordinal)
        {
            SqsDeadLetterSink.ReasonAttribute,
            SqsDeadLetterSink.AttemptCountAttribute,
            SqsDeadLetterSink.DeadLetteredAtAttribute,
            SqsDeadLetterSink.CorrelationIdAttribute,
        };
        if (!string.IsNullOrEmpty(c.IdempotencyKey))
        {
            expectedKeys.Add(SqsDeadLetterSink.IdempotencyKeyAttribute);
        }

        var reasonAttr = attrs.GetValueOrDefault(SqsDeadLetterSink.ReasonAttribute) ?? "<missing>";
        var reasonOk =
            reasonAttr.Length <= SqsDeadLetterSink.ReasonMaxLength
            && c.Reason.StartsWith(reasonAttr, StringComparison.Ordinal)
            && (c.Reason.Length <= SqsDeadLetterSink.ReasonMaxLength
                ? reasonAttr == c.Reason
                : reasonAttr.Length >= SqsDeadLetterSink.ReasonMaxLength - 1);

        var attributesOk =
            expectedKeys.SetEquals(attrs.Keys)
            && reasonOk
            && attrs[SqsDeadLetterSink.AttemptCountAttribute]
                == c.AttemptCount.ToString(CultureInfo.InvariantCulture)
            && attrs[SqsDeadLetterSink.DeadLetteredAtAttribute]
                == c.DeadLetteredAt.ToString("O", CultureInfo.InvariantCulture)
            && attrs[SqsDeadLetterSink.CorrelationIdAttribute] == original.CorrelationId
            && (string.IsNullOrEmpty(c.IdempotencyKey)
                || attrs[SqsDeadLetterSink.IdempotencyKeyAttribute] == c.IdempotencyKey);

        var fifoOk = c.Fifo
            ? sent.GroupId == FifoMessageIds.GroupId(original, c.Selection)
                && sent.DeduplicationId == $"{original.MessageId:D}:dlq"
            : sent.GroupId is null && sent.DeduplicationId is null;

        var holds = sent.QueueName == QueueName && bodyPreserved && attributesOk && fifoOk;

        return holds.ToProperty().Label(
            $"body={bodyPreserved} attrs={attributesOk} fifo={fifoOk} reasonLen={c.Reason.Length} " +
            $"payloadBytes={original.Payload.Length} fifoMode={c.Fifo}");
    }

    // ----- Generators -------------------------------------------------------------------------

    public static Arbitrary<DeadLetterCase> DeadLetterCaseArbitrary() =>
        Arb.From(CaseGen());

    private static Gen<DeadLetterCase> CaseGen() =>
        from envelope in EnvelopeMessageFidelityPropertyTests.MessageEnvelopeArbitrary().Generator
        from reason in ReasonGen()
        from attempts in Gen.Choose(1, 1000)
        from key in Gen.OneOf(Gen.Constant<string?>(null), KeyGen())
        from fifo in Gen.Elements(true, false)
        from selection in Gen.Elements(FifoGroupSelection.CorrelationId, FifoGroupSelection.MessageType)
        from seconds in Gen.Choose(0, int.MaxValue)
        select new DeadLetterCase(
            envelope, reason, attempts, key, fifo, selection,
            DateTimeOffset.UnixEpoch.AddSeconds(seconds));

    private static Gen<string?> KeyGen() =>
        from length in Gen.Choose(1, 64)
        from chars in Gen.ArrayOf(Gen.Elements("abcdef0123456789-:".ToCharArray()), length)
        select (string?)new string(chars);

    /// <summary>Reasons of 0..3000 characters, around the 1,024 cut, with occasional surrogate pairs.</summary>
    private static Gen<string> ReasonGen() =>
        from length in Gen.Frequency(
            (3, Gen.Choose(0, 1024)),
            (1, Gen.Choose(1020, 1030)),
            (3, Gen.Choose(1025, 3000)))
        from units in Gen.ArrayOf(
            Gen.Frequency(
                (9, Gen.Elements("abcXYZ019 -_.:".ToCharArray()).Select(ch => ch.ToString())),
                (1, Gen.Constant("\U0001F600"))),
            length)
        select string.Concat(units);

    // ----- Fake -------------------------------------------------------------------------------

    private sealed record SendCall(
        string QueueName,
        EnvelopeMessage Message,
        IReadOnlyDictionary<string, string> Attributes,
        string? GroupId,
        string? DeduplicationId);

    private sealed class CapturingSender : IDeadLetterQueueSender
    {
        public List<SendCall> Calls { get; } = new();

        public Task SendToDeadLetterQueueAsync(
            string queueName,
            EnvelopeMessage message,
            IReadOnlyDictionary<string, string> attributes,
            string? messageGroupId,
            string? deduplicationId,
            CancellationToken cancellationToken)
        {
            Calls.Add(new SendCall(queueName, message, attributes, messageGroupId, deduplicationId));
            return Task.CompletedTask;
        }
    }
}
