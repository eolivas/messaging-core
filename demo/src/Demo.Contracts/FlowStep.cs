namespace Demo.Contracts;

/// <summary>A single telemetry step in the message flow, emitted by a mock microservice or the pipeline.</summary>
public sealed record FlowStep(
    MicroserviceId Service,
    FlowStepKind Kind,
    string? CorrelationId,
    string? LeadId,
    DateTimeOffset OccurredAtUtc,
    int? Attempt = null,
    string? FailureKind = null,
    string? Reason = null);
