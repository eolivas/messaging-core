namespace Demo.Contracts;

/// <summary>Port through which services report flow steps.</summary>
public interface IFlowStepSink
{
    void Emit(FlowStep step);
}
