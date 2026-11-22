using Demo.Application.Common;
using Demo.Application.Cqrs;

namespace Demo.Application.Sessions.Queries;

public sealed class GetSessionStatusQueryHandler
    : IQueryHandler<GetSessionStatusQuery, SessionStatusDto>
{
    private readonly SessionStatusProvider _status;

    public GetSessionStatusQueryHandler(SessionStatusProvider status)
    {
        _status = status ?? throw new ArgumentNullException(nameof(status));
    }

    public async Task<Result<SessionStatusDto>> HandleAsync(
        GetSessionStatusQuery query, CancellationToken cancellationToken) =>
        Result<SessionStatusDto>.Success(
            await _status.GetAsync(cancellationToken).ConfigureAwait(false));
}
