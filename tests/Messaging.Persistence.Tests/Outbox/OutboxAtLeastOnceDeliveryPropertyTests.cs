using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
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

// Feature: messaging-core, Property 10: Outbox delivers every committed entry at least once.
// Validates: Requirements 5.2, 5.3, 5.6.
//
// For ANY set of N committed (recorded + saved) outbox entries (N >= 1), running the real
// EF/PostgreSQL EfOutboxDispatcher against a publisher that CONFIRMS publication guarantees:
//   - Every committed entry is published through IMessagePublisher at least once; every committed
//     MessageId appears in the fake publisher's captured set (Req 5.2, 5.6).
//   - After the pass, every one of those entries is persisted as Dispatched with a non-null
//     DispatchedAtUtc, and NONE remains Pending (Req 5.3).
//   - Marking Dispatched happens only after confirmation: a second DispatchPendingAsync pass does
//     not re-publish any already-dispatched entry (its publish count does not grow), demonstrating
//     "marked dispatched only after confirm" + "excluded from future attempts" (Req 5.3, 5.6).
//
// Each generated case uses fresh GUID MessageIds/CorrelationIds, so cases are isolated from one
// another without truncating between them - no test depends on another test's state or order. One
// real PostgreSQL container is shared via PostgresCollection; the one-engine rule forbids an
// in-memory substitute, so the dispatcher's pending scan and per-entry SaveChanges run against the
// actual engine.
[Trait("Feature", "messaging-core")]
[Trait("Property", "10")]
[Collection(PostgresCollection.Name)]
public class OutboxAtLeastOnceDeliveryPropertyTests
{
    // Joining PostgresCollection makes [Fact] siblings in the collection share the one container.
    // This fixture handle talks to that SAME process-wide shared container (CreateContextAsync
    // ensure-starts it), which is how the test reaches it even though FsCheck.Xunit's [Property]
    // runner does not inject the collection fixture through the constructor.
    private static readonly PostgresContainerFixture Fixture = new();

    /// <summary>A generated case: how many committed entries to dispatch (N in [1, 8]).</summary>
    public sealed record OutboxScenario(int EntryCount);

    // Each generated case hits the real database (record + save, then up to two dispatch passes),
    // so entry counts are kept small. MaxTest = 100 is the required minimum for a property test.
    [Property(MaxTest = 100, QuietOnSuccess = true, Arbitrary = new[] { typeof(OutboxAtLeastOnceDeliveryPropertyTests) })]
    public Property DispatchPending_OverAnyCommittedEntrySet_DeliversEveryEntryAtLeastOnce(
        OutboxScenario scenario)
    {
        // Arrange: fresh unique MessageIds/CorrelationIds per case so cases never collide, a small
        // committed set of Pending entries over the shared container, and a confirming publisher
        // that captures every published MessageId.
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        var cancellationToken = cancellation.Token;

        var committedIds = CommitPendingEntries(scenario.EntryCount, cancellationToken);

        var publisher = new ConfirmingCapturingPublisher();
        var outboxConfig = new OutboxConfig();

        // Act (first pass): dispatch all pending entries through the confirming publisher.
        using (var dispatchContext = Fixture.CreateContextAsync(cancellationToken).GetAwaiter().GetResult())
        {
            var dispatcher = new EfOutboxDispatcher(
                dispatchContext,
                publisher,
                outboxConfig,
                NullLogger<EfOutboxDispatcher>.Instance);

            dispatcher.DispatchPendingAsync(cancellationToken).GetAwaiter().GetResult();
        }

        // Assert (delivery, Req 5.2/5.6): every committed id was published at least once.
        var publishedAfterFirstPass = publisher.PublishedIds;
        var everyEntryPublished = committedIds.All(id => publishedAfterFirstPass.Contains(id));

        // Assert (status, Req 5.3): every committed entry is persisted Dispatched with a timestamp,
        // and none remains Pending. Reload from a fresh context so we read persisted state, not the
        // dispatcher's tracked instances.
        var persisted = LoadPersistedEntries(committedIds, cancellationToken);
        var allPersisted = persisted.Count == committedIds.Count;
        var allDispatched = persisted.Values.All(entry =>
            entry.Status == OutboxStatus.Dispatched && entry.DispatchedAtUtc is not null);
        var nonePending = persisted.Values.All(entry => entry.Status != OutboxStatus.Pending);

        // Act (second pass, Req 5.3/5.6): dispatched entries are excluded from future scans, so a
        // second pass must not re-publish any of them.
        var publishCountAfterFirstPass = publisher.TotalPublishCount;
        using (var secondPassContext = Fixture.CreateContextAsync(cancellationToken).GetAwaiter().GetResult())
        {
            var dispatcher = new EfOutboxDispatcher(
                secondPassContext,
                publisher,
                outboxConfig,
                NullLogger<EfOutboxDispatcher>.Instance);

            dispatcher.DispatchPendingAsync(cancellationToken).GetAwaiter().GetResult();
        }

        var noRepublishOnSecondPass =
            committedIds.All(id => publisher.PublishCountFor(id) == 1) &&
            publisher.TotalPublishCount == publishCountAfterFirstPass;

        var holds = everyEntryPublished && allPersisted && allDispatched && nonePending && noRepublishOnSecondPass;

        return holds
            .ToProperty()
            .Label(
                $"entries={scenario.EntryCount} => " +
                $"everyPublished={everyEntryPublished}, allPersisted={allPersisted}, " +
                $"allDispatched={allDispatched}, nonePending={nonePending}, " +
                $"noRepublishOnSecondPass={noRepublishOnSecondPass}")
            .Classify(scenario.EntryCount == 1, "single entry")
            .Classify(scenario.EntryCount > 1, "many entries");
    }

    /// <summary>
    /// Records <paramref name="entryCount"/> envelopes as Pending outbox entries and commits them
    /// ONCE on a fresh context, simulating the committed transaction that makes them durable (the
    /// atomicity of recording-with-the-state-change is exercised by tasks 17.1/22.1; here we only
    /// need durable Pending rows for the dispatcher to deliver). Returns the committed MessageIds.
    /// </summary>
    private static IReadOnlyList<Guid> CommitPendingEntries(int entryCount, CancellationToken cancellationToken)
    {
        using var context = Fixture.CreateContextAsync(cancellationToken).GetAwaiter().GetResult();
        var recorder = new OutboxRecorder(context);

        var ids = new List<Guid>(entryCount);
        var baseCreatedAt = DateTimeOffset.UtcNow;

        for (var index = 0; index < entryCount; index++)
        {
            // Fresh GUIDs isolate this case from every other; a small, distinct per-entry timestamp
            // gives the dispatcher's ORDER BY created_at_utc a stable order.
            var messageId = Guid.NewGuid();
            var envelope = new MessageEnvelope
            {
                MessageId = messageId,
                CorrelationId = Guid.NewGuid().ToString("N"),
                MessageType = "messaging.outbox.property10.contract",
                CreatedAtUtc = baseCreatedAt.AddMilliseconds(index),
                Headers = new Dictionary<string, string>(),
                Payload = new byte[] { (byte)(index & 0xFF), 0x0A, 0x0B },
                ContractVersion = 1,
            };

            recorder.Record(envelope);
            ids.Add(messageId);
        }

        // Commit once: the originating "transaction" that makes the entries durable and Pending.
        context.SaveChangesAsync(cancellationToken).GetAwaiter().GetResult();

        return ids;
    }

    /// <summary>
    /// Loads the persisted outbox rows for the given ids from a fresh (no-tracking) context, so the
    /// assertions read durable state rather than the dispatcher's in-memory tracked instances.
    /// </summary>
    private static IReadOnlyDictionary<Guid, OutboxEntry> LoadPersistedEntries(
        IReadOnlyList<Guid> ids,
        CancellationToken cancellationToken)
    {
        using var context = Fixture.CreateContextAsync(cancellationToken).GetAwaiter().GetResult();

        var idSet = ids.ToHashSet();

        return context.OutboxEntries
            .AsNoTracking()
            .Where(entry => idSet.Contains(entry.Id))
            .ToListAsync(cancellationToken)
            .GetAwaiter().GetResult()
            .ToDictionary(entry => entry.Id);
    }

    // ----- Generators -------------------------------------------------------------------------

    public static Arbitrary<OutboxScenario> OutboxScenarioArbitrary() => Arb.From(OutboxScenarioGen());

    /// <summary>
    /// Generates an entry count N in [1, 8]. N is kept small deliberately: every entry is a real
    /// round trip to PostgreSQL (record + save + up to two dispatch passes), so the property stays
    /// fast while still covering a single entry (N = 1) and many entries (N &gt; 1).
    /// </summary>
    private static Gen<OutboxScenario> OutboxScenarioGen() =>
        from entryCount in Gen.Choose(1, 8)
        select new OutboxScenario(entryCount);

    // ----- Test doubles -----------------------------------------------------------------------

    /// <summary>
    /// A fake <see cref="IMessagePublisher"/> that CONFIRMS every publish and records each published
    /// <see cref="MessageEnvelope.MessageId"/>, so the test can assert at-least-once delivery and
    /// that a dispatched entry is never re-published on a later pass. Thread-safe because the
    /// publisher instance is reused across both dispatch passes.
    /// </summary>
    private sealed class ConfirmingCapturingPublisher : IMessagePublisher
    {
        private readonly ConcurrentQueue<Guid> _published = new();

        /// <summary>The distinct set of MessageIds published so far.</summary>
        public IReadOnlySet<Guid> PublishedIds => _published.ToHashSet();

        /// <summary>Total number of publish calls that confirmed (counts re-publishes).</summary>
        public int TotalPublishCount => _published.Count;

        /// <summary>How many times a specific MessageId was published.</summary>
        public int PublishCountFor(Guid messageId) => _published.Count(id => id == messageId);

        public Task<PublishResult> PublishAsync(MessageEnvelope envelope, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(envelope);

            _published.Enqueue(envelope.MessageId);
            return Task.FromResult(PublishResult.Ok());
        }
    }
}
