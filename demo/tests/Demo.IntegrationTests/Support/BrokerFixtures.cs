using Demo.Application.Providers;
using DotNet.Testcontainers.Containers;
using Testcontainers.Kafka;
using Testcontainers.LocalStack;
using Testcontainers.RabbitMq;

namespace Demo.IntegrationTests.Support;

/// <summary>
/// A real broker in a container. The broker must be up within 60 s. A broker that is not
/// reachable makes <see cref="InitializeAsync"/> throw, which fails every test using the
/// fixture. Nothing is skipped (Req 14.9).
/// </summary>
public abstract class BrokerFixture : IAsyncLifetime
{
    public static readonly TimeSpan StartTimeout = TimeSpan.FromSeconds(60);

    public abstract ProviderId Provider { get; }

    public abstract IReadOnlyDictionary<string, string> Settings { get; }

    protected abstract IContainer Container { get; }

    public async Task InitializeAsync()
    {
        using var cts = new CancellationTokenSource(StartTimeout);
        try
        {
            await Container.StartAsync(cts.Token);
        }
        // Testcontainers reports the cancelled readiness wait as a TimeoutException.
        catch (Exception ex) when (ex is OperationCanceledException or TimeoutException)
        {
            throw new InvalidOperationException(
                $"The {Provider.Value} broker was not reachable within {StartTimeout.TotalSeconds:0} seconds.", ex);
        }
    }

    public Task DisposeAsync() => Container.DisposeAsync().AsTask();
}

public sealed class RabbitMqFixture : BrokerFixture
{
    private readonly RabbitMqContainer _container = new RabbitMqBuilder()
        .WithImage("rabbitmq:3.13.7-management-alpine")
        .Build();

    public override ProviderId Provider => ProviderId.RabbitMq;

    protected override IContainer Container => _container;

    public override IReadOnlyDictionary<string, string> Settings => new Dictionary<string, string>
    {
        ["MassTransit:Host"] = $"{_container.Hostname}:{_container.GetMappedPublicPort(5672)}",
        ["MassTransit:ConnectionString"] = _container.GetConnectionString(),
    };
}

public sealed class LocalStackFixture : BrokerFixture
{
    private readonly LocalStackContainer _container = new LocalStackBuilder()
        .WithImage("localstack/localstack:4.14.0")
        .Build();

    public override ProviderId Provider => ProviderId.AmazonSnsSqs;

    protected override IContainer Container => _container;

    public override IReadOnlyDictionary<string, string> Settings => new Dictionary<string, string>
    {
        ["AmazonSqs:ServiceUrl"] = _container.GetConnectionString(),
        ["AmazonSqs:Region"] = "us-east-1",
        ["AmazonSqs:AccessKeyId"] = "test",
        ["AmazonSqs:SecretAccessKey"] = "test",
        // Short long-poll keeps the stop path quick in tests.
        ["AmazonSqs:WaitTimeSeconds"] = "1",
    };
}

public sealed class KafkaFixture : BrokerFixture
{
    private readonly KafkaContainer _container = new KafkaBuilder()
        .WithImage("confluentinc/cp-kafka:7.6.1")
        .Build();

    public override ProviderId Provider => ProviderId.Kafka;

    protected override IContainer Container => _container;

    public override IReadOnlyDictionary<string, string> Settings => new Dictionary<string, string>
    {
        ["Kafka:BootstrapServers"] = _container.GetBootstrapAddress(),
    };
}
