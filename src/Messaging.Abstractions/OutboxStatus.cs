namespace Messaging.Abstractions;

/// <summary>
/// Lifecycle state of a reliable-publish outbox entry (Req 5.3, 5.4, 5.5).
/// </summary>
/// <remarks>
/// This is a pure value type with no layer dependencies, so it lives in the Domain package
/// (<c>Messaging.Abstractions</c>) even though the configuration model that references the
/// outbox is an Application concern. Both the <see cref="IOutboxDispatcher"/> port (Domain) and
/// the EF outbox store (Infrastructure) depend on it; placing it in Domain keeps the dependency
/// direction inward. EF maps this enum to the <c>smallint</c> <c>status</c> column, so the
/// explicit numeric values below are part of the persisted contract and must not be reordered.
/// </remarks>
public enum OutboxStatus
{
    /// <summary>Recorded and awaiting dispatch; included in the dispatcher's pending scan (Req 5.2).</summary>
    Pending = 0,

    /// <summary>Confirmed by the publisher and excluded from future dispatch attempts (Req 5.3).</summary>
    Dispatched = 1,

    /// <summary>Exhausted the dispatch-attempt cap and excluded from future attempts (Req 5.5).</summary>
    Failed = 2,
}
