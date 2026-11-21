using Testcontainers.LocalStack;

namespace Messaging.MassTransit.Tests;

/// <summary>
/// Shared LocalStack (SNS + SQS) Testcontainer fixture. Mirrors <see cref="RabbitMqContainerFixture"/>:
/// one container per process, started on first use and reaped by Testcontainers' cleanup
/// (only containers this process started). Never sets <c>LOCALSTACK_AUTH_TOKEN</c>.
/// </summary>
public sealed class LocalStackContainerFixture : IAsyncLifetime
{
    /// <summary>Dummy credential; LocalStack accepts any value.</summary>
    public const string AccessKeyId = "test";

    /// <summary>Dummy credential; LocalStack accepts any value.</summary>
    public const string SecretAccessKey = "test";

    /// <summary>Region used by tests against the container.</summary>
    public const string Region = "us-east-1";

    private const string ContainerName = "LocalStack";
    private static readonly TimeSpan StartupTimeout = TimeSpan.FromSeconds(60);
    private static readonly SemaphoreSlim StartGate = new(1, 1);
    private static LocalStackContainer? _sharedContainer;

    /// <summary>Starts the shared container (eagerly for the collection fixture).</summary>
    public Task InitializeAsync() => EnsureStartedAsync();

    /// <summary>No-op: the container is process-scoped and reaped by Testcontainers cleanup.</summary>
    public Task DisposeAsync() => Task.CompletedTask;

    /// <summary>Starts the container if needed and returns the edge endpoint URL.</summary>
    /// <exception cref="TimeoutException">Startup exceeded 60 seconds.</exception>
    public async Task<string> GetServiceUrlAsync(CancellationToken cancellationToken = default)
    {
        var container = await EnsureStartedAsync(cancellationToken).ConfigureAwait(false);
        return container.GetConnectionString();
    }

    private static async Task<LocalStackContainer> EnsureStartedAsync(
        CancellationToken cancellationToken = default)
    {
        if (_sharedContainer is not null)
        {
            return _sharedContainer;
        }

        await StartGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_sharedContainer is null)
            {
                var tag = LocalStackImageTag.Resolve(
                    Environment.GetEnvironmentVariable(LocalStackImageTag.EnvironmentVariable));

                var container = new LocalStackBuilder()
                    .WithImage($"localstack/localstack:{tag}")
                    .WithEnvironment("SERVICES", "sns,sqs")
                    .WithCleanUp(true)
                    .Build();

                using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                timeout.CancelAfter(StartupTimeout);
                try
                {
                    await container.StartAsync(timeout.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
                {
                    throw new TimeoutException(
                        $"{ContainerName} container (localstack/localstack:{tag}) did not start "
                        + $"within {StartupTimeout.TotalSeconds:0} seconds.");
                }

                _sharedContainer = container;
            }
        }
        finally
        {
            StartGate.Release();
        }

        return _sharedContainer;
    }
}
