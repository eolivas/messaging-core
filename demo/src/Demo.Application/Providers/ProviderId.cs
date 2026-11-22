namespace Demo.Application.Providers;

/// <summary>Stable identifier of a messaging provider, as used in routes and DTOs.</summary>
public readonly record struct ProviderId(string Value)
{
    public static readonly ProviderId AmazonSnsSqs = new("amazon-sns-sqs");
    public static readonly ProviderId RabbitMq = new("rabbitmq");
    public static readonly ProviderId Kafka = new("kafka");
    public static readonly ProviderId AzureServiceBus = new("azure-service-bus");

    public override string ToString() => Value;
}
