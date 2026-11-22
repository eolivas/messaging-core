using Demo.Application.Sessions;

namespace Demo.Application.Tests.Fakes;

internal sealed class FakeSalesLeadsGateway : ISalesLeadsGateway
{
    public int Calls { get; private set; }

    public Func<CancellationToken, Task<AcceptQuoteOutcome>> OnAccept { get; set; } =
        _ => Task.FromResult(new AcceptQuoteOutcome(true, "corr-1", "lead-1", null));

    public Task<AcceptQuoteOutcome> AcceptQuoteAsync(CancellationToken cancellationToken)
    {
        Calls++;
        return OnAccept(cancellationToken);
    }
}
