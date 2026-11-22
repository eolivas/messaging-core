using System.Collections.Concurrent;
using Demo.Contracts;

namespace Demo.IntegrationTests.Support;

/// <summary>Collects every flow step a Session emits, so tests can assert on what each microservice did.</summary>
public sealed class RecordingFlowStepSink : IFlowStepSink
{
    private readonly ConcurrentQueue<FlowStep> _steps = new();

    public IReadOnlyList<FlowStep> Steps => _steps.ToArray();

    public void Emit(FlowStep step)
    {
        ArgumentNullException.ThrowIfNull(step);
        _steps.Enqueue(step);
    }

    public int Count(MicroserviceId service, FlowStepKind kind, string correlationId) =>
        _steps.Count(s => s.Service == service && s.Kind == kind && s.CorrelationId == correlationId);

    public int CountForLead(MicroserviceId service, FlowStepKind kind, string leadId) =>
        _steps.Count(s => s.Service == service && s.Kind == kind && s.LeadId == leadId);
}
