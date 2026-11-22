using Demo.Customer.Domain;

namespace Demo.Customer.Application;

/// <summary>Bounded in-memory customer store keyed by lead id.</summary>
public interface ICustomerStore
{
    /// <summary>Adds the customer when its LeadId is new. Returns false when already present.</summary>
    bool TryAdd(MockCustomer customer);

    int Count { get; }
}
