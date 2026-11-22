namespace Demo.Contracts;

public enum FlowStepKind
{
    Published,
    Delivered,
    Consumed,
    CustomerCreated,
    EmailSent,
    Retrying,
    DeadLettered,
    Failed
}
