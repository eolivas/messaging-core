using Testcontainers.Kafka;

namespace Messaging.Kafka.Tests;

/// <summary>
/// Shared Kafka Testcontainer fixture for the Streaming (Kafka) integration test (task 22.3). One
/// real Kafka broker is started ONCE and shared across every test that joins the
/// <see cref="KafkaCollection"/>, so the suite does not pay broker start-up cost per test.
/// </summary>
/// <remarks>
/// <para>
/// The one-engine / real-dependency rule (design + <c>infrastructure.md</c> steering) forbids a
/// fake broker here: the at-least-once guarantee (Req 6.7) is a <b>broker</b> behaviour. Only a real
/// Kafka proves that a record whose processing fails before the offset is committed is redelivered
/// to a consumer in the same group rather than silently skipped. This fixture therefore stands up a
/// real broker through <see cref="KafkaContainer"/>.
/// </para>
/// <para>
/// The container is owned at process scope and reaped by Testcontainers' Resource Reaper (Ryuk) at
/// process exit, so there is exactly one broker for the whole run even though the fixture is a
/// collection fixture. No secrets are committed: the broker is a local, throwaway container with no
/// credentials.
/// </para>
/// </remarks>
public sealed class KafkaContainerFixture : IAsyncLifetime
{
    private static readonly SemaphoreSlim StartGate = new(1, 1);
    private static KafkaContainer? _sharedContainer;

    /// <summary>Starts the shared broker (eagerly for the collection fixture).</summary>
    public Task InitializeAsync() => EnsureStartedAsync();

    /// <summary>
    /// No-op: the single shared container is owned at process scope and reaped by Testcontainers'
    /// Resource Reaper at process exit, so it is not torn down per collection.
    /// </summary>
    public Task DisposeAsync() => Task.CompletedTask;

    /// <summary>
    /// Ensures the shared broker is up, then returns its live bootstrap-servers list
    /// (host:port) for wiring the native Confluent.Kafka producer/consumer.
    /// </summary>
    /// <param name="cancellationToken">Token to cancel start-up.</param>
    public async Task<string> GetBootstrapServersAsync(CancellationToken cancellationToken = default)
    {
        var container = await EnsureStartedAsync(cancellationToken).ConfigureAwait(false);
        return container.GetBootstrapAddress();
    }

    /// <summary>
    /// Starts the single shared broker on first call; later calls return the already-started
    /// container. Guarded by a semaphore so concurrent callers (parallel test classes) start exactly
    /// one container.
    /// </summary>
    private static async Task<KafkaContainer> EnsureStartedAsync(CancellationToken cancellationToken = default)
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
                var container = new KafkaBuilder()
                    .WithImage("confluentinc/cp-kafka:7.6.1")
                    .WithCleanUp(true)
                    .Build();

                await container.StartAsync(cancellationToken).ConfigureAwait(false);
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

/// <summary>
/// Binds the Kafka integration test(s) to the shared <see cref="KafkaContainerFixture"/> so a single
/// real broker is reused across the collection.
/// </summary>
[CollectionDefinition(Name)]
public sealed class KafkaCollection : ICollectionFixture<KafkaContainerFixture>
{
    public const string Name = "kafka-integration";
}
