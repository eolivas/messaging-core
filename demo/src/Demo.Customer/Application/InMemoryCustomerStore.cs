using System.Collections.Concurrent;
using Demo.Customer.Domain;

namespace Demo.Customer.Application;

/// <summary>
/// <see cref="ICustomerStore"/> backed by a <see cref="ConcurrentDictionary{TKey,TValue}"/>.
/// Holds at most <see cref="MaxEntries"/> customers; the oldest is evicted first.
/// </summary>
public sealed class InMemoryCustomerStore : ICustomerStore
{
    public const int MaxEntries = 10_000;

    private readonly ConcurrentDictionary<string, MockCustomer> _customers = new(StringComparer.Ordinal);
    private readonly Queue<string> _order = new();
    private readonly object _gate = new();

    public int Count => _customers.Count;

    public bool TryAdd(MockCustomer customer)
    {
        ArgumentNullException.ThrowIfNull(customer);

        lock (_gate)
        {
            if (!_customers.TryAdd(customer.LeadId, customer))
            {
                return false;
            }

            _order.Enqueue(customer.LeadId);
            while (_order.Count > MaxEntries)
            {
                _customers.TryRemove(_order.Dequeue(), out _);
            }

            return true;
        }
    }
}
