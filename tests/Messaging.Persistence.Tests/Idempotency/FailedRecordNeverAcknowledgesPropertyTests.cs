using System;
using System.Threading;
using FsCheck;
using FsCheck.Fluent;
using FsCheck.Xunit;
using Messaging.Abstractions;
using Messaging.Persistence;
using Messaging.Persistence.Idempotency;
using Microsoft.EntityFrameworkCore;

namespace Messaging.Persistence.Tests.Idempotency;

// Feature: messaging-core, Property 17: A failed idempotency record never acknowledges the message.
// Validates: Requirement 7.5.
//
// For ANY idempotency key, WHEN recording the processed key FAILS, the real EF/PostgreSQL
// IIdempotencyStore (EfIdempotencyStore) must NOT let the caller acknowledge and must leave the
// stored record unchanged:
//   (a) the failure is SURFACED - RecordProcessedAsync throws (IdempotencyRecordingException,
//       Req 7.5), it is never swallowed, so the consume pipeline does not acknowledge; and
//   (b) the stored record is LEFT UNCHANGED - a SEPARATE healthy context on the shared container
//       reports IsProcessedAsync(key) == false afterwards (the key was never marked processed).
//
// How the failure is forced deterministically (one-engine rule: the REAL store is exercised, no
// in-memory / faked store). The "failing" store is a real EfIdempotencyStore over a real
// MessagingDbContext whose Npgsql connection string points at a non-listening endpoint
// (127.0.0.1:1) with a 1s connect timeout. Every generated case therefore drives the real
// RecordProcessedAsync -> ExecuteSqlInterpolatedAsync INSERT code path, which fails at execution
// because nothing is listening on that port, and the store's own catch wraps it in
// IdempotencyRecordingException (Req 7.5). This is fully deterministic: there is no listener on
// port 1, so the insert ALWAYS fails the same way for every case, with no reliance on timing,
// connection-pool state, or the order of cases. Because the insert never reaches a real server, the
// key is inherently "left unchanged" - which is exactly the guarantee, and it is asserted against
// the live shared container through a separate healthy context (Fixture.CreateContextAsync).
//
// Each generated case uses a unique GUID-prefixed key so cases are isolated from one another with
// no cross-case or ordering dependence. One real PostgreSQL container is shared via
// PostgresCollection for the "left unchanged" verification.
[Trait("Feature", "messaging-core")]
[Trait("Property", "17")]
[Collection(PostgresCollection.Name)]
public class FailedRecordNeverAcknowledgesPropertyTests
{
    // Joining PostgresCollection makes [Fact] siblings share the one container. This fixture handle
    // talks to that SAME process-wide shared container (CreateContextAsync ensure-starts it), which
    // is how the property reaches it even though FsCheck.Xunit's [Property] runner does not inject
    // the collection fixture through the constructor.
    private static readonly PostgresContainerFixture Fixture = new();

    // Connection string to a non-listening endpoint: the INSERT deterministically fails at execution
    // for every case, exercising the real store's failure-wrapping (Req 7.5). A short connect timeout
    // keeps 100 cases fast; the credentials/host are not secrets (they reach nothing).
    private const string UnreachableConnectionString =
        "Host=127.0.0.1;Port=1;Database=unreachable;Username=none;Password=none;Timeout=1;Command Timeout=1";

    /// <summary>A generated case: a key fragment that is combined with a fresh GUID per case.</summary>
    public sealed record RecordScenario(string KeyFragment);

    [Property(MaxTest = 100, QuietOnSuccess = true, Arbitrary = new[] { typeof(FailedRecordNeverAcknowledgesPropertyTests) })]
    public Property FailedRecording_ForAnyKey_SurfacesFailureAndLeavesRecordUnchanged(RecordScenario scenario)
    {
        // Arrange: a unique key per case (GUID + fragment) so cases never collide; the real store
        // over a context whose connection cannot complete the INSERT (non-listening endpoint).
        var idempotencyKey = $"{Guid.NewGuid():N}:{scenario.KeyFragment}";
        var retention = TimeSpan.FromHours(24);

        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var cancellationToken = cancellation.Token;

        var failingOptions = new DbContextOptionsBuilder<MessagingDbContext>()
            .UseNpgsql(UnreachableConnectionString)
            .Options;

        // Act: record through the REAL EfIdempotencyStore; the parameterized INSERT fails because
        // nothing listens on the endpoint, so the store's catch wraps and rethrows (Req 7.5).
        Exception? surfaced;
        using (var failingContext = new MessagingDbContext(failingOptions))
        {
            IIdempotencyStore failingStore = new EfIdempotencyStore(failingContext);
            surfaced = Record.Exception(() =>
                failingStore
                    .RecordProcessedAsync(idempotencyKey, retention, cancellationToken)
                    .GetAwaiter().GetResult());
        }

        // Assert (a): the failure is surfaced - a non-swallowed exception propagated, and it is the
        // domain-specific IdempotencyRecordingException the store raises for a recording failure.
        var failureSurfaced = surfaced is IdempotencyRecordingException;

        // Assert (b): the stored record is left unchanged - verified on a SEPARATE healthy context
        // against the shared container, the key is NOT marked processed (so no acknowledgement).
        using var healthyContext = Fixture.CreateContextAsync(cancellationToken).GetAwaiter().GetResult();
        IIdempotencyStore healthyStore = new EfIdempotencyStore(healthyContext);
        var recordedAfterFailure = healthyStore
            .IsProcessedAsync(idempotencyKey, cancellationToken)
            .GetAwaiter().GetResult();

        var neverAcknowledges = failureSurfaced && !recordedAfterFailure;

        return neverAcknowledges
            .ToProperty()
            .Label(
                $"key={idempotencyKey} => surfaced={surfaced?.GetType().Name ?? "<none>"} " +
                $"(expected IdempotencyRecordingException), recordedAfterFailure={recordedAfterFailure} (expected false)");
    }

    // ----- Generators -------------------------------------------------------------------------

    public static Arbitrary<RecordScenario> RecordScenarioArbitrary() => Arb.From(RecordScenarioGen());

    /// <summary>
    /// Generates a non-empty, bounded key fragment. The real uniqueness comes from the per-case GUID
    /// prefix added in the property body; the fragment just varies the key shape across cases.
    /// </summary>
    private static Gen<RecordScenario> RecordScenarioGen() =>
        from fragment in Gen.Elements("order", "payment", "shipment", "a", "x-9", "KEY_42")
        select new RecordScenario(fragment);
}
