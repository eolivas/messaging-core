namespace Demo.Application.Sessions;

public sealed record SessionStatusDto(
    string State,
    string? ProviderId,
    string? ProviderName,
    string Health,
    IReadOnlyList<ServiceStatusDto> Services,
    ProblemSummaryDto? LastError);
