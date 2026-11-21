using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using FsCheck;
using FsCheck.Fluent;
using FsCheck.Xunit;
using Messaging.Abstractions;
using Messaging.Configuration;
using Messaging.Consuming;
using Messaging.Lifecycle;
using Messaging.MassTransit.AmazonSqs;
using Messaging.MassTransit.Configuration;
using Messaging.MassTransit.Publishing;
using Messaging.MassTransit.Transport;
using Messaging.Observability;
using Microsoft.Extensions.Logging;

namespace Messaging.MassTransit.Tests;

// Feature: sns-sqs-provider, Property 14: No secret in diagnostics.
// Validates: Requirements 13.8, 14.3, 14.4, 14.5.
//
// For any access key, secret key, session token, and ServiceUrl carrying userinfo, no diagnostic the
// provider emits contains any of those values (or the userinfo, or the full ServiceUrl). The property
// captures text from every surface that could carry them, using only the unit-test fakes:
//
//   validation  : ConfigurationError names/messages/ranges and the aggregated exception message
//   bus         : bus start and stop logs, and a start that fails in the bus or in the topology
//   topology    : manager logs plus the failed-operation and missing-resource exception messages
//   probe       : TransportHealth.UnavailableDependency and the probe's logs (unreachable and throwing)
//   publish     : the failed PublishResult.Error and the publisher's logs
//   metrics     : instrument names, tag keys, and tag values of every measurement
[Trait("Feature", "sns-sqs-provider")]
[Trait("Property", "14")]
public class AmazonSqsNoSecretInDiagnosticsPropertyTests
{
    private const string Region = "eu-west-1";
    private const string ServiceHost = "sqs.eu-west-1.example.com";

    private static readonly ContractRegistration[] Contracts = { new("order.created", typeof(object)) };

    // Feature: sns-sqs-provider, Property 14: No secret in diagnostics
    [Property(MaxTest = 100, QuietOnSuccess = true, Arbitrary = new[] { typeof(AmazonSqsNoSecretInDiagnosticsPropertyTests) })]
    public Property Diagnostics_ForAnyCredentialsAndServiceUrlUserinfo_NeverContainASecret(SecretSet secrets)
    {
        var captured = new List<string>();
        CaptureValidation(secrets, captured);
        CaptureBusAndTopology(secrets, captured);
        CaptureProbe(secrets, captured);
        CapturePublishFailure(secrets, captured);
        CaptureMetrics(secrets, captured);

        var leaks = secrets.Needles
            .SelectMany(needle => captured
                .Where(text => text.Contains(needle, StringComparison.Ordinal))
                .Select(text => $"'{needle}' in '{text}'"))
            .ToList();

        return (leaks.Count == 0)
            .ToProperty()
            .Label($"captured={captured.Count} leaks=[{string.Join(" | ", leaks)}]");
    }

    // ----- Surfaces -------------------------------------------------------------------------------

    private static void CaptureValidation(SecretSet secrets, List<string> captured)
    {
        // Invalid on several secret-bearing settings at once: partial credentials, an http ServiceUrl
        // with userinfo on a public host, and a malformed ServiceUrl variant.
        foreach (var serviceUrl in new[] { secrets.PublicHttpServiceUrl, secrets.RelativeLookingServiceUrl })
        {
            var configuration = new MessagingConfiguration
            {
                Mode = MessagingMode.PublishSubscribe,
                ProviderSettings = new Dictionary<string, string>
                {
                    [AmazonSqsProviderSettings.RegionSettingName] = "not-a-region",
                    [AmazonSqsProviderSettings.EndpointNameSettingName] = "orders",
                    [AmazonSqsProviderSettings.ServiceUrlSettingName] = serviceUrl,
                    [AmazonSqsProviderSettings.AccessKeyIdSettingName] = secrets.AccessKey,
                    [AmazonSqsProviderSettings.SessionTokenSettingName] = secrets.SessionToken,
                },
            };

            foreach (var error in AmazonSqsStartupValidator.Validate(configuration))
            {
                captured.Add(error.SettingName);
                captured.Add(error.Message);
                captured.Add(error.PermittedValuesOrRange ?? string.Empty);
            }

            try
            {
                AmazonSqsStartupValidator.ValidateOrThrow(configuration);
            }
            catch (Exception ex)
            {
                captured.Add(ex.ToString());
            }
        }

        // The secret key alone (no access key) is the other half of the partial-credential error.
        var secretOnly = new MessagingConfiguration
        {
            Mode = MessagingMode.PublishSubscribe,
            ProviderSettings = new Dictionary<string, string>
            {
                [AmazonSqsProviderSettings.RegionSettingName] = Region,
                [AmazonSqsProviderSettings.EndpointNameSettingName] = "orders",
                [AmazonSqsProviderSettings.SecretAccessKeySettingName] = secrets.SecretKey,
            },
        };
        foreach (var error in AmazonSqsStartupValidator.Validate(secretOnly))
        {
            captured.Add(error.SettingName);
            captured.Add(error.Message);
            captured.Add(error.PermittedValuesOrRange ?? string.Empty);
        }
    }

    private static void CaptureBusAndTopology(SecretSet secrets, List<string> captured)
    {
        var settings = secrets.ProviderSettings(autoCreate: true);
        var verifyOnly = secrets.ProviderSettings(autoCreate: false);

        // Success path with a started and stopped bus.
        RunBus(settings, new FakeSnsSqsTopologyClient(), new FakeBusControl(), captured, stop: true);

        // Topology operation failure (the injected failure is benign; the wrapper must stay clean).
        var failing = new FakeSnsSqsTopologyClient();
        failing.FailOn(SnsSqsTopologyOperation.CreateQueue, new InvalidOperationException("access denied"));
        RunBus(settings, failing, new FakeBusControl(), captured, stop: false);

        // Verify-only with nothing provisioned: the missing-resource exception.
        RunBus(verifyOnly, new FakeSnsSqsTopologyClient(), new FakeBusControl(), captured, stop: false);

        // The bus itself fails to start.
        RunBus(
            settings,
            new FakeSnsSqsTopologyClient(),
            new FakeBusControl(throwOnStart: new InvalidOperationException("bus failed")),
            captured,
            stop: false);
    }

    private static void RunBus(
        AmazonSqsProviderSettings settings,
        FakeSnsSqsTopologyClient client,
        FakeBusControl bus,
        List<string> captured,
        bool stop)
    {
        var topologyLogger = new CapturingLogger<SnsSqsTopologyManager>(captured);
        var adapterLogger = new CapturingLogger<AmazonSqsBusAdapter>(captured);
        using var metrics = new AmazonSqsMetrics();
        var adapter = new AmazonSqsBusAdapter(
            bus,
            settings,
            MessagingMode.PublishSubscribe,
            MassTransitRole.Both,
            Contracts,
            new SnsSqsTopologyManager(client, settings, topologyLogger),
            client,
            new ObservabilityContext(
                new NoOpMessagingLog(),
                new NoOpMessagingMetrics(),
                new MessagingTraceSource(tracingEnabled: false),
                new PayloadRedactor()),
            metrics,
            new InFlightTracker(),
            shutdownTimeoutSeconds: 30,
            adapterLogger);

        try
        {
            adapter.StartBusAsync(CancellationToken.None).GetAwaiter().GetResult();
            if (stop)
            {
                adapter.StopBusAsync(CancellationToken.None).GetAwaiter().GetResult();
            }
        }
        catch (Exception ex)
        {
            captured.Add(ex.ToString());
        }
    }

    private static void CaptureProbe(SecretSet secrets, List<string> captured)
    {
        var providerSettings = new MassTransitProviderSettings(
            MassTransitTransport.AmazonSqs, connectionString: null, host: null, secrets.ProviderSettings(true));
        captured.Add(providerSettings.Host);

        foreach (var bus in new[]
        {
            new FakeMassTransitBusAdapter(reachable: false),
            new FakeMassTransitBusAdapter(throwOnProbe: new InvalidOperationException("ping failed")),
        })
        {
            var logger = new CapturingLogger<MassTransitTransportProbe>(captured);
            var health = new MassTransitTransportProbe(bus, providerSettings, logger)
                .ProbeAsync(CancellationToken.None).GetAwaiter().GetResult();
            captured.Add(health.UnavailableDependency ?? string.Empty);
        }

        // Required-settings descriptors are diagnostics too: they must not carry values.
        foreach (var descriptor in providerSettings.ToRequiredSettings())
        {
            captured.Add(descriptor.Name);
            captured.Add(descriptor.PermittedValuesOrRange ?? string.Empty);
        }
    }

    private static void CapturePublishFailure(SecretSet secrets, List<string> captured)
    {
        var logger = new CapturingLogger<MassTransitPublisher>(captured);
        var bus = new FakeMassTransitBusAdapter(throwOnPublish: new InvalidOperationException("send failed"));
        var result = new MassTransitPublisher(bus, logger)
            .PublishAsync(MassTransitTestSupport.ValidEnvelope(), CancellationToken.None).GetAwaiter().GetResult();

        captured.Add(result.Error ?? string.Empty);

        // The AWS-facing exception types carry only names, never settings values.
        captured.Add(new TopologyOperationFailedException("sqs:CreateQueue", "orders").Message);
        captured.Add(new TopologyResourceMissingException(new[] { "orders", "orders_error" }).Message);
        captured.Add(new TopologyConflictException("orders", "Standard").Message);
        captured.Add(new PublishNotPermittedException().Message);
        captured.Add(new MessageDeliveryRejectedException().Message);
        captured.Add(new MessageTooLargeException(300000, 262144).Message);
        _ = secrets;
    }

    private static void CaptureMetrics(SecretSet secrets, List<string> captured)
    {
        var settings = secrets.ProviderSettings(autoCreate: true);
        var topic = SnsSqsNamingConvention.TopicName("order.created", settings);
        var queue = SnsSqsNamingConvention.QueueName(settings.EndpointName, settings);

        var client = new FakeSnsSqsTopologyClient();
        client.PresetQueue(queue, new Dictionary<string, string> { ["ApproximateNumberOfMessages"] = "3" });

        var measurements = new ConcurrentQueue<string>();
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == AmazonSqsMetrics.MeterName)
            {
                measurements.Enqueue(instrument.Meter.Name);
                measurements.Enqueue(instrument.Name);
                measurements.Enqueue(instrument.Unit ?? string.Empty);
                measurements.Enqueue(instrument.Description ?? string.Empty);
                l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((_, _, tags, _) =>
        {
            foreach (var tag in tags)
            {
                measurements.Enqueue(tag.Key);
                measurements.Enqueue(tag.Value?.ToString() ?? string.Empty);
            }
        });
        listener.Start();

        using var metrics = new AmazonSqsMetrics();
        metrics.RecordPublished(topic);
        metrics.RecordConsumed(queue);
        metrics.RecordDeadLettered(queue);
        metrics.RecordFailed(queue);
        metrics.SampleQueueDepthAsync(client, new[] { queue }, CancellationToken.None).GetAwaiter().GetResult();
        listener.RecordObservableInstruments();

        captured.AddRange(measurements);
    }

    // ----- Generators -----------------------------------------------------------------------------

    public static Arbitrary<SecretSet> SecretSetArbitrary() =>
        Arb.From(
            from accessKey in TokenGen()
            from secretKey in TokenGen()
            from sessionToken in TokenGen()
            from user in TokenGen()
            from password in TokenGen()
            select new SecretSet(accessKey, secretKey, sessionToken, user, password));

    /// <summary>A 12..24 character alphanumeric token, long enough that a coincidental match is negligible.</summary>
    private static Gen<string> TokenGen() =>
        from length in Gen.Choose(12, 24)
        from chars in Gen.ArrayOf(
            Gen.Elements("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789".ToCharArray()),
            length)
        select new string(chars);

    /// <summary>The generated secret-bearing values for one case.</summary>
    public sealed record SecretSet(
        string AccessKey, string SecretKey, string SessionToken, string User, string Password)
    {
        public string UserInfo => $"{User}:{Password}";

        /// <summary>A valid https ServiceUrl carrying userinfo.</summary>
        public string ServiceUrl => $"https://{UserInfo}@{ServiceHost}:4566";

        /// <summary>An http ServiceUrl with userinfo on a public host (rejected by validation).</summary>
        public string PublicHttpServiceUrl => $"http://{UserInfo}@{ServiceHost}:4566";

        /// <summary>A malformed ServiceUrl that still contains the userinfo text.</summary>
        public string RelativeLookingServiceUrl => $"{UserInfo}@{ServiceHost}";

        /// <summary>Every value that must never appear in a diagnostic.</summary>
        public IReadOnlyList<string> Needles => new[]
        {
            AccessKey, SecretKey, SessionToken, User, Password, UserInfo, ServiceUrl, PublicHttpServiceUrl,
        };

        public AmazonSqsProviderSettings ProviderSettings(bool autoCreate) => new()
        {
            Region = Region,
            EndpointName = "orders",
            ServiceUrl = ServiceUrl,
            AccessKeyId = AccessKey,
            SecretAccessKey = SecretKey,
            SessionToken = SessionToken,
            AutoCreateTopology = autoCreate,
        };
    }

    // ----- Capturing logger -----------------------------------------------------------------------

    /// <summary>
    /// Records, for every log call, the rendered message, each structured state key and value, and the
    /// full exception text, so a leak through any of them is detected.
    /// </summary>
    private sealed class CapturingLogger<T> : ILogger<T>
    {
        private readonly List<string> _sink;

        public CapturingLogger(List<string> sink) => _sink = sink;

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            _sink.Add(formatter(state, exception));
            if (state is IEnumerable<KeyValuePair<string, object?>> pairs)
            {
                foreach (var pair in pairs)
                {
                    _sink.Add(pair.Key);
                    _sink.Add(pair.Value?.ToString() ?? string.Empty);
                }
            }

            if (exception is not null)
            {
                _sink.Add(exception.ToString());
            }
        }
    }
}
