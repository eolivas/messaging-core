namespace Demo.SalesLeads.Application;

/// <summary>Mock Sales Leads microservice: accepts a quote and publishes one event.</summary>
public interface ISalesLeadsService
{
    Task<AcceptQuoteResult> AcceptQuoteAsync(CancellationToken cancellationToken);
}
