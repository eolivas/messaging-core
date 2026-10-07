using System;
using System.Threading;
using FsCheck;
using FsCheck.Fluent;
using FsCheck.Xunit;
using Messaging.Abstractions;
using Messaging.Persistence.Idempotency;

namespace Messaging.Persistence.Tests.Idempotency;

// Feature: messaging-core, Property 16: Idempotent consumption executes side effects exactly once.
// Validates: Requirements 7.2, 7.4.
//
// For ANY idempotency key and ANY number of redelivery attempts N (N >= 1), driving the real
// EF/PostgreSQL IIdempotencyStore through the consume gate -
//   if IsProcessedAsync is false  -> run the side effect, then RecordProcessedAsync
//   if IsProcessedAsync is true    -> skip the side effect (duplicate: acked, not re-executed)
// - the side effect executes EXACTLY ONCE across all N deliveries of the same key, and after the
// run the key is recorded as processed. The first delivery detects no prior record and runs the
// effect once (Req 7.2 duplicate detection happens BEFORE side effects); every later delivery is
// detected as a duplicate and acknowledged WITHOUT re-executing the effect (Req 7.4). Because the
// store uses INSERT ... ON CONFLICT DO NOTHING, this also exercises the atomic record-on-success.
//
// Each generated case uses a unique key (a fresh GUID combined with the generated fragment), so
// cases are isolated from one another without truncating between them - no test depends on another
// test's state or order. One real PostgreSQL container is shared via PostgresCollection.
[Trait("Feature", "messaging-core")]
[Trait("Property", "16")]
[Collection(PostgresCollection.Name)]
public class ExactlyOnceSideEffectsPropertyTests
{
    // Joining PostgresCollection makes [Fact] siblings in the collection share the one container.
    // This fixture handle talks to that SAME process-wide shared container (CreateContextAsync
    // ensure-starts it), which is how the test reaches it even though FsCheck.Xunit's [Property]
    // runner does not inject the collection fixture through the constructor.
    private static readonly PostgresContainerFixture Fixture = new();

    /// <summary>A generated case: a key fragment and a redelivery count N (N >= 1).</summary>
    public sealed record DeliveryScenario(string KeyFragment, int DeliveryCount);

    // Each generated case hits the real database, so operations are kept small (one key, a bounded
    // handful of redeliveries). MaxTest = 100 is the required minimum for a property test.
    [Property(MaxTest = 100, QuietOnSuccess = true, Arbitrary = new[] { typeof(ExactlyOnceSideEffectsPropertyTests) })]
    public Property IdempotentConsumption_OverAnyNumberOfRedeliveries_ExecutesSideEffectExactlyOnce(
        DeliveryScenario scenario)
    {
        // Arrange: a unique key per case (GUID + fragment) so cases never collide, a store over the
        // shared container, and a counter standing in for the consumer's side effect.
        var idempotencyKey = $"{Guid.NewGuid():N}:{scenario.KeyFragment}";
        var retention = TimeSpan.FromHours(24);
        var sideEffectExecutions = 0;

        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var cancellationToken = cancellation.Token;

        using var context = Fixture.CreateContextAsync(cancellationToken).GetAwaiter().GetResult();
        var store = new EfIdempotencyStore(context);

        // Act: deliver the SAME key N times through the idempotent consume gate.
        for (var delivery = 0; delivery < scenario.DeliveryCount; delivery++)
        {
            var alreadyProcessed = store
                .IsProcessedAsync(idempotencyKey, cancellationToken)
                .GetAwaiter().GetResult();

            if (alreadyProcessed)
            {
                // Duplicate (Req 7.4): acknowledge WITHOUT re-executing the side effect.
                continue;
            }

            // First (non-duplicate) delivery (Req 7.2): run the side effect, THEN record the key.
            sideEffectExecutions++;
            store
                .RecordProcessedAsync(idempotencyKey, retention, cancellationToken)
                .GetAwaiter().GetResult();
        }

        // Assert: the side effect ran exactly once and the key is now recorded as processed.
        var recordedAfterRun = store
            .IsProcessedAsync(idempotencyKey, cancellationToken)
            .GetAwaiter().GetResult();

        var exactlyOnce = sideEffectExecutions == 1 && recordedAfterRun;

        return exactlyOnce
            .ToProperty()
            .Label(
                $"key={idempotencyKey}, deliveries={scenario.DeliveryCount} => " +
                $"sideEffects={sideEffectExecutions} (expected 1), recorded={recordedAfterRun}")
            .Classify(scenario.DeliveryCount == 1, "single delivery")
            .Classify(scenario.DeliveryCount > 1, "redelivered");
    }

    // ----- Generators -------------------------------------------------------------------------

    public static Arbitrary<DeliveryScenario> DeliveryScenarioArbitrary() => Arb.From(DeliveryScenarioGen());

    /// <summary>
    /// Generates a non-empty, bounded key fragment and a redelivery count N in [1, 8]. N is kept
    /// small deliberately: every delivery is a real round trip to PostgreSQL, so the property stays
    /// fast while still covering single delivery (N = 1) and many redeliveries (N &gt; 1).
    /// </summary>
    private static Gen<DeliveryScenario> DeliveryScenarioGen() =>
        from fragment in Gen.Elements("order", "payment", "shipment", "a", "x-9", "KEY_42")
        from deliveryCount in Gen.Choose(1, 8)
        select new DeliveryScenario(fragment, deliveryCount);
}
