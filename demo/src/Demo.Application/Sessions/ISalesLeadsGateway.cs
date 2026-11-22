namespace Demo.Application.Sessions;

/// <summary>Port: triggers the Sales Leads microservice of the active Session.</summary>
public interface ISalesLeadsGateway
{
    Task<AcceptQuoteOutcome> AcceptQuoteAsync(CancellationToken cancellationToken);
}
