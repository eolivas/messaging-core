using Messaging.Abstractions;
using Messaging.Configuration;
using Messaging.Consuming;
using Messaging.Contracts;
using Messaging.Kafka.Streaming;
using Messaging.Observability;
using Messaging.Resilience;
using Messaging.Startup;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Messaging.Kafka.Tests;

/// <summary>
/// Verifies the <c>AddKafkaMessagingProvider</c> registration extension (Req 3.6): it registers a
/// resolvable <see cref="IMessagingProvider"/> for <see cref="MessagingMode.Streaming"/>, accumulates
/// into the provider enumerable the registry consumes, keeps the provider a stable singleton, and
/// rejects null arguments fail-fast. It also verifies the composition from task 10.1: production seams
/// are <c>TryAdd</c>ed (a pre-registered fake wins), the hosted service is exposed as
/// <see cref="IHostedService"/>, security settings default to Plaintext, and an invalid Streaming
/// configuration surfaces every error together without leaking credential values.
/// </summary>
public class KafkaMessagingProviderServiceCollectionExtensionsTests
{
    private static ServiceCollection ServicesWithSeams()
    {
        var services = new ServiceCollection();

        // Logging the shells (publisher, consumer host) take via ILogger<T>.
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));

        // Deep native-client seam ports owned by the provider's later spec; the caller supplies them.
        services.AddSingleton<IKafkaProducer>(new FakeKafkaProducer());
        services.AddSingleton<IKafkaConsumer>(new FakeKafkaConsumer());
        services.AddSingleton<IStreamMessageProcessor>(new FakeStreamMessageProcessor());
        services.AddSingleton<IKafkaTransportProbe>(new FakeKafkaTransportProbe(new TransportHealth(true, null)));

        return services;
    }

    [Fact]
    public void AddKafkaMessagingProvider_RegistersResolvableStreamingProvider()
    {
        var services = ServicesWithSeams();

        services.AddKafkaMessagingProvider(KafkaTestSupport.ValidSettings());

        using var provider = services.BuildServiceProvider();
        var messagingProvider = provider.GetRequiredService<IMessagingProvider>();

        Assert.IsType<KafkaMessagingProvider>(messagingProvider);
        Assert.Equal(MessagingMode.Streaming, messagingProvider.Mode);
    }

    [Fact]
    public void AddKafkaMessagingProvider_RegistersProviderIntoTheEnumerableTheRegistryConsumes()
    {
        var services = ServicesWithSeams();

        services.AddKafkaMessagingProvider(KafkaTestSupport.ValidSettings());

        using var provider = services.BuildServiceProvider();
        var providers = provider.GetServices<IMessagingProvider>().ToArray();

        Assert.Single(providers);
        Assert.Equal(MessagingMode.Streaming, providers[0].Mode);
    }

    [Fact]
    public void AddKafkaMessagingProvider_RegistersProviderAsSingleton()
    {
        var services = ServicesWithSeams();

        services.AddKafkaMessagingProvider(KafkaTestSupport.ValidSettings());

        using var provider = services.BuildServiceProvider();
        var first = provider.GetRequiredService<IMessagingProvider>();
        var second = provider.GetRequiredService<IMessagingProvider>();

        Assert.Same(first, second);
    }

    [Fact]
    public void AddKafkaMessagingProvider_ExposesPublisherConsumerHostAndRequiredSettings()
    {
        var services = ServicesWithSeams();

        services.AddKafkaMessagingProvider(KafkaTestSupport.ValidSettings());

        using var provider = services.BuildServiceProvider();
        var messagingProvider = provider.GetRequiredService<IMessagingProvider>();

        Assert.NotNull(messagingProvider.Publisher);
        Assert.NotNull(messagingProvider.ConsumerHost);
        Assert.Contains(
            KafkaProviderSettings.BootstrapServersSettingName,
            messagingProvider.RequiredSettings.Select(s => s.Name));
    }

    [Fact]
    public void AddKafkaMessagingProvider_WithNullServices_Throws()
    {
        IServiceCollection services = null!;

        Assert.Throws<ArgumentNullException>(() =>
            services.AddKafkaMessagingProvider(KafkaTestSupport.ValidSettings()));
    }

    [Fact]
    public void AddKafkaMessagingProvider_WithNullSettings_Throws()
    {
        var services = new ServiceCollection();

        Assert.Throws<ArgumentNullException>(() =>
            services.AddKafkaMessagingProvider(null!));
    }

    // ---- Task 10.2: DI composition of the production seams ----

    private static ServiceCollection ServicesWithCore(bool withIdempotencyGate = false)
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton(new ObservabilityContext(
            new NoOpLog(),
            new NoOpMetrics(),
            new MessagingTraceSource(tracingEnabled: false),
            new PayloadRedactor()));
        var router = new DeadLetterRouter(
            new KafkaIntegrationFactory.CapturingDeadLetterSink(),
            new NoOpDeadLetterMetrics(),
            new NoOpDeadLetterLog());
        services.AddSingleton(router);
        services.AddSingleton(new RetryExecutor(
            new FailureClassifier(),
            new BackoffCalculator(static (min, _) => min),
            router,
            delay: static (_, _) => Task.CompletedTask));
        services.AddSingleton(new RetryPolicyConfig());
        services.AddSingleton(new LifecycleConfig());
        services.AddSingleton(new ConsumerHostStateMachine());
        services.AddSingleton<IContractBinder, AcceptAllBinder>();
        services.AddSingleton<IHandlerInvoker, NoOpInvoker>();
        if (withIdempotencyGate)
        {
            services.AddSingleton(new IdempotencyGate(
                new DefaultIdempotencyKeyProvider(), new NeverProcessedStore(), new IdempotencyConfig()));
        }

        return services;
    }

    private static MessagingConfiguration StreamingConfiguration(IReadOnlyDictionary<string, string> providerSettings) =>
        new() { Mode = MessagingMode.Streaming, ProviderSettings = providerSettings };

    [Fact]
    public void AddKafkaMessagingProvider_WithoutPreRegisteredSeams_ResolvesProductionTypes()
    {
        var services = ServicesWithCore();
        services.AddKafkaMessagingProvider(KafkaTestSupport.ValidSettings());
        using var provider = services.BuildServiceProvider();

        Assert.IsType<ConfluentKafkaProducer>(provider.GetRequiredService<IKafkaProducer>());
        Assert.IsType<ConfluentKafkaConsumer>(provider.GetRequiredService<IKafkaConsumer>());
        Assert.IsType<ConfluentKafkaTransportProbe>(provider.GetRequiredService<IKafkaTransportProbe>());
        Assert.IsType<KafkaStreamMessageProcessor>(provider.GetRequiredService<IStreamMessageProcessor>());
        Assert.NotNull(provider.GetRequiredService<KafkaClientConfigFactory>());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void AddKafkaMessagingProvider_ConsumePipelineComposition_ResolvesWithAndWithoutIdempotencyGate(bool withGate)
    {
        var services = ServicesWithCore(withGate);
        services.AddKafkaMessagingProvider(KafkaTestSupport.ValidSettings());
        using var provider = services.BuildServiceProvider();

        var processor = provider.GetRequiredService<IStreamMessageProcessor>();

        Assert.IsType<KafkaStreamMessageProcessor>(processor);
    }

    [Fact]
    public void AddKafkaMessagingProvider_PreRegisteredFakeSeams_WinOverProductionDefaults()
    {
        var producer = new FakeKafkaProducer();
        var consumer = new FakeKafkaConsumer();
        var processor = new FakeStreamMessageProcessor();
        var probe = new FakeKafkaTransportProbe(new TransportHealth(true, null));
        var services = ServicesWithCore();
        services.AddSingleton<IKafkaProducer>(producer);
        services.AddSingleton<IKafkaConsumer>(consumer);
        services.AddSingleton<IStreamMessageProcessor>(processor);
        services.AddSingleton<IKafkaTransportProbe>(probe);
        services.AddKafkaMessagingProvider(KafkaTestSupport.ValidSettings());
        using var provider = services.BuildServiceProvider();

        Assert.Same(producer, provider.GetRequiredService<IKafkaProducer>());
        Assert.Same(consumer, provider.GetRequiredService<IKafkaConsumer>());
        Assert.Same(processor, provider.GetRequiredService<IStreamMessageProcessor>());
        Assert.Same(probe, provider.GetRequiredService<IKafkaTransportProbe>());
    }

    [Fact]
    public void AddKafkaMessagingProvider_HostedService_ResolvesToSharedKafkaConsumerHostedService()
    {
        var services = ServicesWithSeams();
        services.AddSingleton(new LifecycleConfig());
        services.AddKafkaMessagingProvider(KafkaTestSupport.ValidSettings());
        using var provider = services.BuildServiceProvider();

        var hosted = provider.GetServices<IHostedService>().ToArray();

        var service = Assert.IsType<KafkaConsumerHostedService>(Assert.Single(hosted));
        Assert.Same(provider.GetRequiredService<KafkaConsumerHostedService>(), service);
    }

    [Fact]
    public void AddKafkaMessagingProvider_WithoutSecuritySettings_DefaultsToPlaintext()
    {
        var services = ServicesWithSeams();
        services.AddKafkaMessagingProvider(KafkaTestSupport.ValidSettings());
        using var provider = services.BuildServiceProvider();

        var security = provider.GetRequiredService<KafkaSecuritySettings>();

        Assert.Equal(KafkaSecurityProtocol.Plaintext, security.SecurityProtocol);
        Assert.Equal(KafkaSaslMechanism.None, security.SaslMechanism);
        Assert.Null(security.SaslUsername);
        Assert.Null(security.SaslPassword);
    }

    [Fact]
    public void AddKafkaMessagingProvider_WithSuppliedSecuritySettings_UsesThatInstance()
    {
        var supplied = new KafkaSecuritySettings
        {
            SecurityProtocol = KafkaSecurityProtocol.SaslSsl,
            SaslMechanism = KafkaSaslMechanism.Plain,
            SaslUsername = "user",
            SaslPassword = "pw",
        };
        var services = ServicesWithSeams();
        services.AddKafkaMessagingProvider(KafkaTestSupport.ValidSettings(), supplied);
        using var provider = services.BuildServiceProvider();

        Assert.Same(supplied, provider.GetRequiredService<KafkaSecuritySettings>());
    }

    [Fact]
    public void ResolveProvider_StreamingConfigurationWithInvalidSettings_ThrowsOneExceptionWithAllErrors()
    {
        var services = ServicesWithSeams();
        services.AddSingleton(StreamingConfiguration(new Dictionary<string, string>
        {
            [KafkaProviderSettings.SecurityProtocolSettingName] = "SaslSsl",
            [KafkaProviderSettings.SaslMechanismSettingName] = "Plain",
        }));
        services.AddKafkaMessagingProvider(KafkaTestSupport.ValidSettings());
        using var provider = services.BuildServiceProvider();

        var ex = Assert.Throws<MessagingConfigurationException>(() => provider.GetRequiredService<IMessagingProvider>());

        var names = ex.Errors.Select(e => e.SettingName).ToArray();
        Assert.Contains(KafkaProviderSettings.BootstrapServersSettingName, names);
        Assert.Contains(KafkaProviderSettings.ConsumerGroupIdSettingName, names);
        Assert.Contains(KafkaProviderSettings.TopicSettingName, names);
        Assert.Contains(KafkaProviderSettings.SaslUsernameSettingName, names);
        Assert.Contains(KafkaProviderSettings.SaslPasswordSettingName, names);
        Assert.Equal(names.Length, names.Distinct().Count());
    }

    [Fact]
    public void ResolveProvider_StreamingConfigurationWithInvalidSettings_MessageOmitsCredentialValues()
    {
        const string secretPassword = "s3cr3t-P@ssw0rd-value";
        var services = ServicesWithSeams();
        services.AddSingleton(StreamingConfiguration(new Dictionary<string, string>
        {
            [KafkaProviderSettings.SecurityProtocolSettingName] = "SaslSsl",
            [KafkaProviderSettings.SaslMechanismSettingName] = "Plain",
            [KafkaProviderSettings.SaslPasswordSettingName] = secretPassword,
        }));
        services.AddKafkaMessagingProvider(KafkaTestSupport.ValidSettings());
        using var provider = services.BuildServiceProvider();

        var ex = Assert.Throws<MessagingConfigurationException>(() => provider.GetRequiredService<IMessagingProvider>());

        Assert.Contains(KafkaProviderSettings.SaslUsernameSettingName, ex.Message);
        Assert.DoesNotContain(secretPassword, ex.Message);
    }

    [Fact]
    public void ResolveProvider_StreamingConfigurationWithValidSettings_Resolves()
    {
        var services = ServicesWithSeams();
        services.AddSingleton(StreamingConfiguration(new Dictionary<string, string>
        {
            [KafkaProviderSettings.BootstrapServersSettingName] = "localhost:9092",
            [KafkaProviderSettings.ConsumerGroupIdSettingName] = "g",
            [KafkaProviderSettings.TopicSettingName] = "t",
        }));
        services.AddKafkaMessagingProvider(KafkaTestSupport.ValidSettings());
        using var provider = services.BuildServiceProvider();

        Assert.IsType<KafkaMessagingProvider>(provider.GetRequiredService<IMessagingProvider>());
    }

    private sealed class AcceptAllBinder : IContractBinder
    {
        public bool IsKnown(string messageType) => true;

        public object Bind(MessageEnvelope envelope) => new object();
    }

    private sealed class NoOpInvoker : IHandlerInvoker
    {
        public Task InvokeAsync(
            MessageEnvelope envelope,
            object boundMessage,
            ConsumeContext context,
            CancellationToken cancellationToken) => Task.CompletedTask;
    }

    private sealed class NeverProcessedStore : IIdempotencyStore
    {
        public Task<bool> IsProcessedAsync(string idempotencyKey, CancellationToken cancellationToken) =>
            Task.FromResult(false);

        public Task RecordProcessedAsync(string idempotencyKey, TimeSpan retention, CancellationToken cancellationToken) =>
            Task.CompletedTask;
    }

    private sealed class NoOpLog : IMessagingLog
    {
        public void MessageEvent(MessagingEventType eventType, string correlationId, DateTimeOffset timestampUtc)
        {
        }
    }

    private sealed class NoOpMetrics : IMessagingMetrics
    {
        public void RecordPublished() { }

        public void RecordConsumed() { }

        public void RecordRetry() { }

        public void RecordDeadLetter() { }
    }

    private sealed class NoOpDeadLetterMetrics : IDeadLetterMetrics
    {
        public void RecordDeadLetter() { }
    }

    private sealed class NoOpDeadLetterLog : IDeadLetterLog
    {
        public void DeadLettered(string correlationId, string failureReason, int attemptCount) { }

        public void DeadLetterRoutingFailed(string correlationId, string failureReason, int attemptCount, Exception error) { }
    }
}
