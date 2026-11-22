using System.Collections.Concurrent;
using Messaging.Abstractions;
namespace Demo.Infrastructure.Messaging;

/// <summary>
/// In-memory idempotency store. Register one instance per microservice host so each
/// consumer keeps its own processed-key set. Entries expire after their retention.
/// </summary>
public sealed class InMemoryIdempotencyStore : IIdempotencyStore
{
    private readonly ConcurrentDictionary<string, DateTimeOffset> _expiries = new();
    private readonly TimeProvider _time;

    public InMemoryIdempotencyStore(TimeProvider? time = null)
    {
        _time = time ?? TimeProvider.System;
    }

    public Task<bool> IsProcessedAsync(string idempotencyKey, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(idempotencyKey);
        cancellationToken.ThrowIfCancellationRequested();
        if (!_expiries.TryGetValue(idempotencyKey, out var expiry))
        {
            return Task.FromResult(false);
        }
        if (expiry > _time.GetUtcNow())
        {
            return Task.FromResult(true);
        }
        _expiries.TryRemove(new KeyValuePair<string, DateTimeOffset>(idempotencyKey, expiry));
        return Task.FromResult(false);
    }

    public Task RecordProcessedAsync(string idempotencyKey, TimeSpan retention, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(idempotencyKey);
        cancellationToken.ThrowIfCancellationRequested();
        _expiries[idempotencyKey] = _time.GetUtcNow().Add(retention);
        return Task.CompletedTask;
    }
}
