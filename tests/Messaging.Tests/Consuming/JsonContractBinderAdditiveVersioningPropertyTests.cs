using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using FsCheck;
using FsCheck.Fluent;
using FsCheck.Xunit;
using Messaging.Consuming;
using Messaging.Contracts;

namespace Messaging.Tests.Consuming;

// Feature: messaging-core, Property 9: Additive versioning stays backward-compatible.
// Validates: Requirements 4.7.
//
// Requirement 4.7: the Message_Envelope contract supports additive versioning such that a consumer
// bound to an EARLIER contract version continues to bind successfully when a producer adds NEW
// optional fields, and such that unrecognized fields are IGNORED by that consumer.
//
// This property models the earlier version with a fixed DTO (V1OrderContract) that the consumer is
// bound to via a single ContractRegistration. For any payload that represents a LATER version — the
// exact V1 fields PLUS a varying set of extra/unrecognized members (additive only: V1 fields are
// never renamed or removed) — the JsonContractBinder:
//   BINDS      — Bind never throws; the newer, additive payload stays compatible with the earlier
//                contract.
//   ROUND-TRIPS — every V1 field on the bound instance equals the value serialized into the payload.
//   IGNORES    — the extra/unknown members are silently dropped (the V1 instance carries only V1
//                data), which is observable because the bound shape has nowhere to hold them and
//                binding still succeeds and matches the V1 inputs exactly.
// Generators vary the V1 field values and the count, names, and values of the extra unknown fields,
// so the only invariant exercised is additive backward compatibility.
[Trait("Feature", "messaging-core")]
[Trait("Property", "9")]
public class JsonContractBinderAdditiveVersioningPropertyTests
{
    private const string OrderMessageType = "orders.created";

    /// <summary>
    /// The "earlier version" contract the consumer is bound to (V1). A newer producer may add
    /// optional fields beyond these; a V1-bound consumer must keep binding and ignore them (Req 4.7).
    /// Public parameterless-constructable with settable members so System.Text.Json can populate it.
    /// </summary>
    public sealed class V1OrderContract
    {
        public Guid OrderId { get; set; }
        public string CustomerName { get; set; } = string.Empty;
        public int Quantity { get; set; }
        public decimal TotalAmount { get; set; }
        public bool IsPriority { get; set; }
    }

    /// <summary>
    /// A later-version payload: the chosen V1 field values plus a set of additive, unrecognized
    /// members. <see cref="ExtraFieldNames"/> is carried so a failing case reports which extra
    /// members were present.
    /// </summary>
    public sealed record AdditivePayload(V1OrderContract V1Values, IReadOnlyList<string> ExtraFieldNames, byte[] PayloadBytes);

    [Property(MaxTest = 100, QuietOnSuccess = true, Arbitrary = new[] { typeof(JsonContractBinderAdditiveVersioningPropertyTests) })]
    public Property Bind_LaterVersionPayloadWithExtraFields_BindsAndIgnoresUnknownFields(AdditivePayload sample)
    {
        // Arrange: a binder bound ONLY to the earlier V1 contract for the message type.
        var binder = new JsonContractBinder(new[]
        {
            new ContractRegistration(OrderMessageType, typeof(V1OrderContract)),
        });

        var envelope = new MessageEnvelope
        {
            MessageId = Guid.NewGuid(),
            CorrelationId = "corr-1",
            MessageType = OrderMessageType,
            CreatedAtUtc = DateTimeOffset.UnixEpoch,
            Headers = new Dictionary<string, string>(),
            Payload = new ReadOnlyMemory<byte>(sample.PayloadBytes),
            ContractVersion = 1,
        };

        // Act: binding a later, additively-extended payload against the earlier contract.
        object bound;
        try
        {
            bound = binder.Bind(envelope);
        }
        catch (Exception ex)
        {
            // BINDS: any throw violates additive backward compatibility.
            return false
                .ToProperty()
                .Label($"Bind threw {ex.GetType().Name}: {ex.Message}. Extra fields: [{string.Join(", ", sample.ExtraFieldNames)}]");
        }

        // ROUND-TRIPS + IGNORES: the bound V1 instance carries every V1 field value and nothing else.
        var typed = bound as V1OrderContract;
        var expected = sample.V1Values;

        var roundTripped =
            typed is not null
            && typed.OrderId == expected.OrderId
            && string.Equals(typed.CustomerName, expected.CustomerName, StringComparison.Ordinal)
            && typed.Quantity == expected.Quantity
            && typed.TotalAmount == expected.TotalAmount
            && typed.IsPriority == expected.IsPriority;

        return roundTripped
            .ToProperty()
            .Classify(sample.ExtraFieldNames.Count == 0, "no extra fields")
            .Classify(sample.ExtraFieldNames.Count > 0, "with extra fields")
            .Label(
                $"boundType={bound.GetType().Name}; extraFields=[{string.Join(", ", sample.ExtraFieldNames)}]");
    }

    // ----- Generators -------------------------------------------------------------------------

    public static Arbitrary<AdditivePayload> AdditivePayloadArbitrary() => Arb.From(AdditivePayloadGen());

    /// <summary>
    /// Produces a later-version JSON payload: random V1 field values serialized together with a
    /// varying number of extra members whose names are guaranteed NOT to collide with any V1 field
    /// (additive-only — V1 fields are never renamed or removed). The payload bytes are what a newer
    /// producer would emit; a V1-bound consumer must still bind them and drop the extras.
    /// </summary>
    private static Gen<AdditivePayload> AdditivePayloadGen() =>
        from values in V1ValuesGen()
        from extras in ExtraFieldsGen()
        select BuildPayload(values, extras);

    private static Gen<V1OrderContract> V1ValuesGen() =>
        from orderId in Gen.Fresh(Guid.NewGuid)
        from customerName in TokenGen()
        from quantity in Gen.Choose(0, 100_000)
        from amountCents in Gen.Choose(0, 100_000_000)
        from isPriority in Gen.Elements(false, true)
        select new V1OrderContract
        {
            OrderId = orderId,
            CustomerName = customerName,
            Quantity = quantity,
            // Keep a finite decimal (cents) so JSON round-trips exactly.
            TotalAmount = amountCents / 100m,
            IsPriority = isPriority,
        };

    /// <summary>
    /// 0..6 extra members with names outside the V1 field set and simple scalar/array/object values,
    /// representing optional fields a newer producer added. Names are de-duplicated so the serialized
    /// object has a valid, distinct key set.
    /// </summary>
    private static Gen<IReadOnlyList<(string Name, object? Value)>> ExtraFieldsGen()
    {
        var reserved = new HashSet<string>(
            new[] { "OrderId", "CustomerName", "Quantity", "TotalAmount", "IsPriority" },
            StringComparer.OrdinalIgnoreCase);

        var nameGen = Gen.Choose(0, 10_000)
            .Select(i => $"ext_{i}")
            .Where(n => !reserved.Contains(n));

        var valueGen = Gen.OneOf(
            TokenGen().Select(s => (object?)s),
            Gen.Choose(int.MinValue, int.MaxValue).Select(i => (object?)i),
            Gen.Elements(false, true).Select(b => (object?)b),
            Gen.Constant((object?)null),
            TokenGen().Select(s => (object?)new[] { s, s }),
            TokenGen().Select(s => (object?)new Dictionary<string, string> { ["nested"] = s }));

        var entryGen = from name in nameGen
                       from value in valueGen
                       select (name, value);

        return from count in Gen.Choose(0, 6)
               from entries in Gen.ArrayOf(entryGen, count)
               select (IReadOnlyList<(string, object?)>)entries
                   .GroupBy(e => e.name, StringComparer.Ordinal)
                   .Select(g => g.First())
                   .ToList();
    }

    private static Gen<string> TokenGen() =>
        from length in Gen.Choose(0, 24)
        from chars in Gen.ArrayOf(TokenCharGen(), length)
        select new string(chars);

    private static Gen<char> TokenCharGen() => Gen.Elements(
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 -_.:".ToCharArray());

    /// <summary>
    /// Serializes the V1 values and the extra members into a single JSON object (the later-version
    /// wire form) and returns the resulting payload bytes alongside the inputs for assertion.
    /// </summary>
    private static AdditivePayload BuildPayload(
        V1OrderContract values,
        IReadOnlyList<(string Name, object? Value)> extras)
    {
        var document = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["OrderId"] = values.OrderId,
            ["CustomerName"] = values.CustomerName,
            ["Quantity"] = values.Quantity,
            ["TotalAmount"] = values.TotalAmount,
            ["IsPriority"] = values.IsPriority,
        };

        foreach (var (name, value) in extras)
        {
            // Additive only: extra names never collide with V1 fields (guaranteed by the generator).
            document[name] = value;
        }

        var bytes = JsonSerializer.SerializeToUtf8Bytes(document);
        return new AdditivePayload(values, extras.Select(e => e.Name).ToList(), bytes);
    }
}
