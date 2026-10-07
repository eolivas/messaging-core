using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using FsCheck;
using FsCheck.Fluent;
using FsCheck.Xunit;
using Messaging.Abstractions;
using Messaging.Contracts;
using Messaging.Providers;
using Messaging.Publishing;

namespace Messaging.Tests.Publishing;

// Feature: messaging-core, Property 4: Invalid envelopes are rejected and never delivered.
// Validates: Requirements 3.2, 4.3.
//
// For any MessageEnvelope that violates at least one envelope invariant, PublishPipeline.PublishAsync
//   REJECTS   — returns a failed PublishResult (Succeeded == false) carrying a non-empty Error
//               describing the violation (Req 3.2, 4.3), and
//   NEVER DELIVERS — hands nothing to the active provider's publisher, so the CapturingMessagingProvider
//               records zero delivered envelopes (Req 3.2).
//
// The invariants exercised mirror MessageEnvelope.Validate / the correlation id gate:
//   - empty MessageId (Guid.Empty)
//   - empty / whitespace MessageType
//   - non-UTC CreatedAtUtc (non-zero offset)
//   - overlong CorrelationId (> MaxCorrelationIdLength)   [a supplied-but-invalid value]
//   - more than MaxHeaderCount headers
//   - ContractVersion < 1
//
// NOTE on the correlation id: an *absent* (empty/whitespace) correlation id is NOT a rejection
// path — the pipeline's correlation id gate generates one when absent (task 8.1). Only a
// supplied-but-invalid (overlong) correlation id is rejected, so the generator produces only
// overlong correlation ids as a violation, never empty ones.
[Trait("Feature", "messaging-core")]
[Trait("Property", "4")]
public class PublishPipelineInvalidEnvelopePropertyTests
{
    private const MessagingMode ActiveMode = MessagingMode.PublishSubscribe;

    /// <summary>
    /// A generated envelope that violates one or more invariants, together with a human-readable
    /// list of the violations it was built to carry (used only to label counterexamples).
    /// </summary>
    public sealed record InvalidEnvelopeCase(MessageEnvelope Envelope, IReadOnlyList<string> Violations);

    [Property(MaxTest = 200, QuietOnSuccess = true, Arbitrary = new[] { typeof(PublishPipelineInvalidEnvelopePropertyTests) })]
    public Property InvalidEnvelope_IsRejected_AndNeverDelivered(InvalidEnvelopeCase testCase)
    {
        // Arrange: a pipeline fronting a capturing provider that would record any delivery.
        var provider = new CapturingMessagingProvider(ActiveMode);
        var registry = new ProviderRegistry(new IMessagingProvider[] { provider });
        var pipeline = new PublishPipeline(new CorrelationIdGate(), registry, ActiveMode);

        // Act: publishing synchronously completes (no real transport); await the result.
        var result = pipeline.PublishAsync(testCase.Envelope, CancellationToken.None).GetAwaiter().GetResult();

        // Assert: rejected with a non-empty error AND nothing was handed to the provider.
        var rejected = !result.Succeeded && !string.IsNullOrEmpty(result.Error);
        var neverDelivered = provider.DeliveredEnvelopes.Count == 0;

        return (rejected && neverDelivered)
            .ToProperty()
            .Label(
                $"violations=[{string.Join(", ", testCase.Violations)}] " +
                $"=> Succeeded={result.Succeeded}, Error='{result.Error}', " +
                $"delivered={provider.DeliveredEnvelopes.Count}")
            .Classify(testCase.Violations.Count > 1, "multiple violations");
    }

    // ----- Generators -------------------------------------------------------------------------

    public static Arbitrary<InvalidEnvelopeCase> InvalidEnvelopeCaseArbitrary() => Arb.From(InvalidEnvelopeCaseGen());

    /// <summary>
    /// Starts from a valid envelope and corrupts one or more fields so at least one invariant is
    /// violated. Each possible violation is toggled independently; the generator is constrained to
    /// guarantee at least one violation is present (otherwise the envelope would be valid and the
    /// property would not apply).
    /// </summary>
    private static Gen<InvalidEnvelopeCase> InvalidEnvelopeCaseGen() =>
        from badMessageId in Gen.Elements(false, true)
        from badMessageType in Gen.Elements(false, true)
        from badTimestamp in Gen.Elements(false, true)
        from badCorrelationId in Gen.Elements(false, true)
        from badHeaders in Gen.Elements(false, true)
        from badContractVersion in Gen.Elements(false, true)
        // Base building blocks for the corrupted fields.
        from messageTypeChoice in Gen.Elements("", "   ", "\t")
        from offsetHours in Gen.Choose(1, 14)
        from overlongExtra in Gen.Choose(1, 64)
        from headerCount in Gen.Choose(MessageEnvelope.MaxHeaderCount + 1, MessageEnvelope.MaxHeaderCount + 40)
        from contractVersion in Gen.Choose(int.MinValue, 0)
        // Force at least one violation: if the dice produced none, flip the message id to bad.
        let anyBad = badMessageId || badMessageType || badTimestamp || badCorrelationId || badHeaders || badContractVersion
        let forceMessageId = badMessageId || !anyBad
        select BuildCase(
            forceMessageId,
            badMessageType,
            badTimestamp,
            badCorrelationId,
            badHeaders,
            badContractVersion,
            messageTypeChoice,
            offsetHours,
            overlongExtra,
            headerCount,
            contractVersion);

    private static InvalidEnvelopeCase BuildCase(
        bool badMessageId,
        bool badMessageType,
        bool badTimestamp,
        bool badCorrelationId,
        bool badHeaders,
        bool badContractVersion,
        string badMessageTypeValue,
        int offsetHours,
        int overlongExtra,
        int headerCount,
        int badContractVersionValue)
    {
        var violations = new List<string>();

        var messageId = Guid.NewGuid();
        if (badMessageId)
        {
            messageId = Guid.Empty;
            violations.Add("empty MessageId");
        }

        var messageType = "orders.created";
        if (badMessageType)
        {
            messageType = badMessageTypeValue;
            violations.Add("empty/whitespace MessageType");
        }

        var createdAtUtc = DateTimeOffset.UtcNow;
        if (badTimestamp)
        {
            createdAtUtc = new DateTimeOffset(DateTime.UtcNow.Ticks, TimeSpan.FromHours(offsetHours));
            violations.Add("non-UTC CreatedAtUtc");
        }

        // Only a supplied-but-invalid (overlong) correlation id is a rejection path; an absent one
        // would be generated by the gate, not rejected.
        var correlationId = "corr-abc";
        if (badCorrelationId)
        {
            correlationId = new string('c', MessageEnvelope.MaxCorrelationIdLength + overlongExtra);
            violations.Add("overlong CorrelationId");
        }

        IReadOnlyDictionary<string, string> headers = new Dictionary<string, string>();
        if (badHeaders)
        {
            headers = Enumerable
                .Range(0, headerCount)
                .ToDictionary(i => $"h{i}", i => $"v{i}");
            violations.Add($">{MessageEnvelope.MaxHeaderCount} headers");
        }

        var contractVersion = 1;
        if (badContractVersion)
        {
            contractVersion = badContractVersionValue;
            violations.Add("ContractVersion < 1");
        }

        var envelope = new MessageEnvelope
        {
            MessageId = messageId,
            CorrelationId = correlationId,
            MessageType = messageType,
            CreatedAtUtc = createdAtUtc,
            Headers = headers,
            Payload = new ReadOnlyMemory<byte>(new byte[] { 1, 2, 3 }),
            ContractVersion = contractVersion,
        };

        return new InvalidEnvelopeCase(envelope, violations);
    }
}
