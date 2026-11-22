namespace Demo.Api.Configuration;

/// <summary>
/// Connection and credential settings of the BFF, bound from the "Demo" section.
/// <c>appsettings.Development.json</c> holds throwaway local values; environment variables
/// (for example <c>Demo__FrontendOrigin</c>) override them.
/// </summary>
public sealed class DemoOptions
{
    public const string SectionName = "Demo";

    public string? FrontendOrigin { get; set; }

    public RabbitMqOptions RabbitMq { get; set; } = new();

    public KafkaOptions Kafka { get; set; } = new();

    public AmazonSqsOptions AmazonSqs { get; set; } = new();

    public sealed class RabbitMqOptions
    {
        public string? ConnectionString { get; set; }
    }

    public sealed class KafkaOptions
    {
        public string? BootstrapServers { get; set; }

        public string? Topic { get; set; }
    }

    public sealed class AmazonSqsOptions
    {
        public string? Region { get; set; }

        public string? ServiceUrl { get; set; }

        public string? AccessKeyId { get; set; }

        public string? SecretAccessKey { get; set; }
    }
}
