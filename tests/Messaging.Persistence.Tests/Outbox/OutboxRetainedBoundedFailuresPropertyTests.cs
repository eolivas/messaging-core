using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using FsCheck;
using FsCheck.Fluent;
using FsCheck.Xunit;
using Messaging.Abstractions;
using Messaging.Configuration;
using Messaging.Contracts;
using Messaging.Persistence.Outbox;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging.Abstractions;

namespace Messaging.Persistence.Tests.Outbox;

// Feature: messaging-core, Property 11: Outbox failures are retained and bounded.
// Validates: Requirements 5.4, 5.5.
//
// For ANY committed outbox entry and a publisher that ALWAYS fails - either by returning a failed
// PublishResult or by throwing (Req 5.4 covers both failure routes) - repeatedly running the real
// EF/PostgreSQL EfOutboxDispatcher:
//   RETAINED (Req 5.4): after each failing pass BELOW the attempt cap the entry is still present and
//   stays Status = Pending, its AttemptCount increments by exactly 1 per pass, LastError is set
//   (non-null), DispatchedAtUtc stays null, and it remains eligible for the next pass. The entry is
//   never lost and never marked Dispatched.
//   BOUNDED (Req 5.5): after exactly MaxDispatchAttempts failing passes the entry becomes
//   Status = Failed with AttemptCount == MaxDispatchAttempts, and a FURTHER pass does NOT pick it up
//   again - the publisher is not invoked for it and the row stays Failed with AttemptCount unchanged.
//
// Each generated case uses a fresh GUID message id, so cases are isolated from one another without
// truncating between them - no test depends on another test's state or order. One real PostgreSQL
// container is shared via PostgresCollection; the one-engine rule forbids an in-memory substitute.
[Trait("Feature", "messaging-core")]
[Trait("Property", "11")]
[Collection(PostgresCollection.Name)]
public class OutboxRetainedBoundedFailuresPropertyTests
{
    // Joining PostgresCollection makes [Fact] siblings share the one container. This fixture handle
    // talks to that SAME process-wide shared container (CreateContextAsync ensure-starts it), which
    // is how a [Property] test reaches it even though FsCheck.Xunit's runner does not inject the
    // collection fixture through the constructor.
    private static readonly PostgresContainerFixture Fixture = new();

    /// <summary>How an always-failing publisher reports its failure (both routes are Req 5.4).</summary>
    public enum FailureMode
    {
        /// <summary>Publisher returns <see cref="PublishResult.Fail(string)"/>.</summary>
        ResultFail,

        /// <summary>Publisher throws an exception.</summary>
        Throw,
    }

    /// <summary>A generated case: the attempt cap to test and how the publisher fails.</summary>
    public sealed record FailureScenario(int MaxDispatchAttempts, FailureMode FailureMode);

    // Every pass is a real round trip to PostgreSQL, so the cap is kept small (1..6) and the whole
    // sequence per case runs cap+1 passes. MaxTest = 100 is the required minimum.
    [Property(MaxTest = 100, QuietOnSuccess = true, Arbitrary = new[] { typeof(OutboxRetainedBoundedFailuresPropertyTests) })]
    public Property Dispatcher_WhenPublishAlwaysFails_RetainsEntryThenBoundsItAtTheCap(
        FailureScenario scenario)
    {
        // Arrange: a unique entry per case (fresh GUID id) so cases never collide, a dispatcher over
        // the shared container, and an always-failing publisher that counts its invocations.
        var cap = scenario.MaxDispatchAttempts;
        var messageId = Guid.NewGuid();

        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        var cancellationToken = cancellation.Token;

        var envelope = BuildEnvelope(messageId);

        // Commit the entry durably as Pending through the real recorder + SaveChanges.
        using (var seedContext = Fixture.CreateContextAsync(cancellationToken).GetAwaiter().GetResult())
        {
            new OutboxRecorder(seedContext).Record(envelope);
            seedContext.SaveChangesAsync(cancellationToken).GetAwaiter().GetResult();
        }

        var publisher = new AlwaysFailingPublisher(scenario.FailureMode);
        var outboxConfig = new OutboxConfig { MaxDispatchAttempts = cap };

        var failures = new List<string>();

        // Act + Assert (interleaved): run cap failing passes, checking retention below the cap and
        // the bound at the cap, then one more pass to confirm exclusion.
        for (var pass = 1; pass <= cap; pass++)
        {
            RunDispatchPass(publisher, outboxConfig, cancellationToken);

            var entry = ReloadEntry(messageId, cancellationToken);
            if (entry is null)
            {
                failures.Add($"pass {pass}: entry was lost (not found)");
                break;
            }

            if (pass < cap)
            {
                // RETAINED (Req 5.4): still Pending, attempt counted, error recorded, not dispatched.
                if (entry.Status != OutboxStatus.Pending)
                {
                    failures.Add($"pass {pass}/{cap}: expected Pending but was {entry.Status}");
                }

                if (entry.AttemptCount != pass)
                {
                    failures.Add($"pass {pass}/{cap}: expected AttemptCount {pass} but was {entry.AttemptCount}");
                }

                if (entry.LastError is null)
                {
                    failures.Add($"pass {pass}/{cap}: expected LastError to be set but it was null");
                }

                if (entry.DispatchedAtUtc is not null)
                {
                    failures.Add($"pass {pass}/{cap}: expected DispatchedAtUtc null but was {entry.DispatchedAtUtc}");
                }
            }
            else
            {
                // BOUNDED (Req 5.5): at the cap the entry is Failed with AttemptCount == cap.
                if (entry.Status != OutboxStatus.Failed)
                {
                    failures.Add($"pass {pass}/{cap} (cap): expected Failed but was {entry.Status}");
                }

                if (entry.AttemptCount != cap)
                {
                    failures.Add($"pass {pass}/{cap} (cap): expected AttemptCount {cap} but was {entry.AttemptCount}");
                }

                if (entry.DispatchedAtUtc is not null)
                {
                    failures.Add($"pass {pass}/{cap} (cap): expected DispatchedAtUtc null but was {entry.DispatchedAtUtc}");
                }
            }
        }

        // Bounded/excluded (Req 5.5): one MORE pass must NOT touch the Failed entry.
        var invocationsAtCap = publisher.InvocationCount;
        RunDispatchPass(publisher, outboxConfig, cancellationToken);

        if (publisher.InvocationCount != invocationsAtCap)
        {
            failures.Add(
                $"after cap: publisher was invoked again ({invocationsAtCap} -> " +
                $"{publisher.InvocationCount}); Failed entry must be excluded from further dispatch");
        }

        var finalEntry = ReloadEntry(messageId, cancellationToken);
        if (finalEntry is null)
        {
            failures.Add("after cap: entry was lost (not found)");
        }
        else
        {
            if (finalEntry.Status != OutboxStatus.Failed)
            {
                failures.Add($"after cap: expected Failed but was {finalEntry.Status}");
            }

            if (finalEntry.AttemptCount != cap)
            {
                failures.Add($"after cap: expected AttemptCount {cap} unchanged but was {finalEntry.AttemptCount}");
            }
        }

        var retainedAndBounded = failures.Count == 0;

        return retainedAndBounded
            .ToProperty()
            .Label(
                $"cap={cap}, mode={scenario.FailureMode}, id={messageId} => " +
                (retainedAndBounded ? "retained then bounded" : string.Join("; ", failures)))
            .Classify(scenario.FailureMode == FailureMode.ResultFail, "fail-by-result")
            .Classify(scenario.FailureMode == FailureMode.Throw, "fail-by-throw")
            .Classify(cap == 1, "cap=1 (immediate bound)")
            .Classify(cap > 1, "cap>1 (retained then bounded)");
    }

    // ----- Helpers ----------------------------------------------------------------------------

    /// <summary>Runs one dispatch pass with a fresh context so persisted state is observed cleanly.</summary>
    private static void RunDispatchPass(
        AlwaysFailingPublisher publisher,
        OutboxConfig outboxConfig,
        CancellationToken cancellationToken)
    {
        using var context = Fixture.CreateContextAsync(cancellationToken).GetAwaiter().GetResult();
        var dispatcher = new EfOutboxDispatcher(
            context,
            publisher,
            outboxConfig,
            NullLogger<EfOutboxDispatcher>.Instance);

        dispatcher.DispatchPendingAsync(cancellationToken).GetAwaiter().GetResult();
    }

    /// <summary>Reloads the persisted row from a FRESH context (no change-tracker carryover).</summary>
    private static OutboxEntry? ReloadEntry(Guid messageId, CancellationToken cancellationToken)
    {
        using var context = Fixture.CreateContextAsync(cancellationToken).GetAwaiter().GetResult();
        return context.OutboxEntries
            .AsNoTracking()
            .FirstOrDefaultAsync(entry => entry.Id == messageId, cancellationToken)
            .GetAwaiter().GetResult();
    }

    /// <summary>Builds a valid envelope with the given id for recording into the outbox.</summary>
    private static MessageEnvelope BuildEnvelope(Guid messageId) => new()
    {
        MessageId = messageId,
        CorrelationId = messageId.ToString("N"),
        MessageType = "messaging-core.tests.retained-bounded",
        CreatedAtUtc = DateTimeOffset.UtcNow,
        Headers = new Dictionary<string, string>(),
        Payload = new byte[] { 1, 2, 3, 4 },
        ContractVersion = 1,
    };

    /// <summary>
    /// A publisher that NEVER confirms. Depending on the generated mode it either returns a failed
    /// <see cref="PublishResult"/> or throws, exercising both failure routes the dispatcher must
    /// treat as a retained attempt (Req 5.4). It counts invocations so the test can assert a Failed
    /// entry is excluded from further dispatch (Req 5.5). Named to avoid colliding with any helper
    /// in the sibling task-17.2 test file.
    /// </summary>
    private sealed class AlwaysFailingPublisher : IMessagePublisher
    {
        private readonly FailureMode _failureMode;
        private int _invocationCount;

        public AlwaysFailingPublisher(FailureMode failureMode) => _failureMode = failureMode;

        /// <summary>Number of times the dispatcher asked this publisher to publish.</summary>
        public int InvocationCount => Volatile.Read(ref _invocationCount);

        public Task<PublishResult> PublishAsync(MessageEnvelope envelope, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _invocationCount);

            if (_failureMode == FailureMode.Throw)
            {
                throw new InvalidOperationException("Simulated transient publish failure (test).");
            }

            return Task.FromResult(PublishResult.Fail("Simulated publish rejection (test)."));
        }
    }

    // ----- Generators -------------------------------------------------------------------------

    public static Arbitrary<FailureScenario> FailureScenarioArbitrary() => Arb.From(FailureScenarioGen());

    /// <summary>
    /// Generates a small attempt cap in [1, 6] and one of the two failure routes. The cap is kept
    /// small deliberately: each case runs cap+1 real dispatch passes against PostgreSQL, so the
    /// property stays fast while still covering the immediate bound (cap = 1) and the
    /// retain-then-bound path (cap &gt; 1) across both failure modes.
    /// </summary>
    private static Gen<FailureScenario> FailureScenarioGen() =>
        from cap in Gen.Choose(1, 6)
        from mode in Gen.Elements(FailureMode.ResultFail, FailureMode.Throw)
        select new FailureScenario(cap, mode);
}
