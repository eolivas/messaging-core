using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Messaging.Abstractions;

namespace Messaging.Tests.Fakes;

/// <summary>
/// A trivial in-memory <see cref="IIdempotencyStore"/> test double for exercising the consume
/// idempotency gate without the EF-backed store (which is built in task 16). It records keys handed
/// to <see cref="RecordProcessedAsync"/> and answers <see cref="IsProcessedAsync"/> from that set, so
/// a test can seed "already processed" keys and observe duplicate handling.
/// </summary>
/// <remarks>
/// Honest capture over mocking: it stores real keys and returns real answers. One public type per
/// file, mirroring the other fakes under <c>Fakes/</c>.
/// </remarks>
internal sealed class FakeIdempotencyStore : IIdempotencyStore
{
    private readonly HashSet<string> _processedKeys = new(StringComparer.Ordinal);
    private readonly List<string> _lookups = new();

    /// <summary>Keys that were queried via <see cref="IsProcessedAsync"/>, in order.</summary>
    public IReadOnlyList<string> Lookups => _lookups;

    /// <summary>Seeds a key as already processed so a later lookup reports it as a duplicate.</summary>
    public void SeedProcessed(string idempotencyKey) => _processedKeys.Add(idempotencyKey);

    public Task<bool> IsProcessedAsync(string idempotencyKey, CancellationToken cancellationToken)
    {
        _lookups.Add(idempotencyKey);
        return Task.FromResult(_processedKeys.Contains(idempotencyKey));
    }

    public Task RecordProcessedAsync(string idempotencyKey, TimeSpan retention, CancellationToken cancellationToken)
    {
        _processedKeys.Add(idempotencyKey);
        return Task.CompletedTask;
    }
}
