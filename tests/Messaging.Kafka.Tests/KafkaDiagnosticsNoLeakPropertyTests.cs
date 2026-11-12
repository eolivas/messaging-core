using System.Globalization;
using System.Text;
using Confluent.Kafka;
using FsCheck;
using FsCheck.Fluent;
using FsCheck.Xunit;
using Messaging.Abstractions;
using Messaging.Configuration;
using Messaging.Contracts;
using Messaging.Kafka.Streaming;
using Messaging.Observability;
using Microsoft.Extensions.Logging;

namespace Messaging.Kafka.Tests;

// Feature: kafka-provider, Property 6: Redaction totality - no secret or payload in diagnostics.
// Validates: Requirements 1.5, 10.3, 10.4, 11.3, 11.4, 11.5, 12.6, 12.10.
//
// For ANY SASL credential values, payload bytes and header values, none of them appears in any adapter
// diagnostic surface that can be driven without a broker: captured logs (publisher, consumer host,
// hosted service, ConfluentKafkaConsumer), startup validation errors, MissingRequiredHeaderException /
// KafkaOffsetCommitException / KafkaProduceException messages, KafkaSecuritySettings.ToString(),
// PublishResult errors and the probe dependency name; and a sensitive-keyed value is always
// replaced by PayloadRedactor.RedactionMarker.
//
// Each generated value is built from its own alphabet, mutually disjoint from every fixed diagnostic
// text and enum name (letters, digits, '.', '\'', '-', ':', '_', '{', '}', '=', ',', ' '). Any character of
// those alphabets in a diagnostic can therefore only have come from a leaked secret / payload / header
// value, which is a strictly stronger check than whole-value substring matching.
[Trait("Feature", "kafka-provider")]
[Trait("Property", "6")]
public class KafkaDiagnosticsNoLeakPropertyTests
{
    private const string SecretAlphabet = "!@#$%^&*~";
    private const string PayloadAlphabet = "\u00A4\u00A7\u00B6\u2020\u2021\u2022\u2030";
    private const string HeaderAlphabet = "\u00AB\u00BB\u2039\u203A";

    private static readonly string[] SensitiveKeys = { "password", "Password", "TOKEN", "secret", "ApiKey", "authorization", "credentials" };
    private static readonly string[] RequiredHeaders =
    {
        KafkaWireFormat.MessageIdHeader,
        KafkaWireFormat.CorrelationIdHeader,
        KafkaWireFormat.MessageTypeHeader,
        KafkaWireFormat.CreatedAtHeader,
        KafkaWireFormat.ContractVersionHeader,
    };

    public sealed record Scenario(
        string Username,
        string Password,
        string Payload,
        string HeaderValue,
        bool Ssl,
        int MechanismIndex,
        int DroppedHeaderIndex,
        string SensitiveKey)
    {
        // Never print generated secret material in a failure report.
        public override string ToString() =>
            $"Scenario {{ user={Username.Length}c, pass={Password.Length}c, payload={Payload.Length}c, " +
            $"header={HeaderValue.Length}c, ssl={Ssl}, mech={MechanismIndex}, dropped={DroppedHeaderIndex}, key={SensitiveKey} }}";
    }

    [Property(MaxTest = 100, QuietOnSuccess = true, Arbitrary = new[] { typeof(KafkaDiagnosticsNoLeakPropertyTests) })]
    public Property Diagnostics_ForAnySecretsAndPayloads_NeverContainThem(Scenario s)
    {
        var leaks = new List<string>();

        void Check(string surface, string? text)
        {
            if (text is not null && ContainsForbidden(text))
            {
                leaks.Add(surface);
            }
        }

        var security = new KafkaSecuritySettings
        {
            SecurityProtocol = s.Ssl ? KafkaSecurityProtocol.SaslSsl : KafkaSecurityProtocol.SaslPlaintext,
            SaslMechanism = Mechanisms[s.MechanismIndex],
            SaslUsername = s.Username,
            SaslPassword = s.Password,
        };

        // 1. KafkaSecuritySettings.ToString()
        Check("KafkaSecuritySettings.ToString", security.ToString());

        // 2. Startup validation errors (a broken bootstrap forces errors; blank/invalid SASL fields add more).
        foreach (var settings in StartupSettings(s))
        {
            var errors = KafkaStartupValidator.Validate(
                new MessagingConfiguration { Mode = MessagingMode.Streaming, ProviderSettings = settings });
            foreach (var e in errors)
            {
                Check("KafkaStartupValidator", $"{e.SettingName}|{e.Message}|{e.PermittedValuesOrRange}");
            }
        }

        // 3. MissingRequiredHeaderException: absent header and unparseable header, with payload present.
        var payloadBytes = Encoding.UTF8.GetBytes(s.Payload);
        var present = ValidHeaders(s);
        present.Remove(RequiredHeaders[s.DroppedHeaderIndex]);
        AssertThrowsMissingHeader(present, payloadBytes, "absent", Check);

        var unparseable = ValidHeaders(s);
        unparseable.Remove(KafkaWireFormat.CreatedAtHeader);
        unparseable.Add(KafkaWireFormat.CreatedAtHeader, Encoding.UTF8.GetBytes(s.HeaderValue + s.Payload + s.Password));
        AssertThrowsMissingHeader(unparseable, payloadBytes, "unparseable", Check);

        // 4. KafkaOffsetCommitException and KafkaProduceException messages.
        Check("KafkaOffsetCommitException", new KafkaOffsetCommitException(3, 42, new InvalidOperationException(s.Password)).Message);
        Check("KafkaProduceException", new KafkaProduceException("orders", "persistence status NotPersisted").Message);

        // 5. Transport probe dependency name, even when the check itself faults with a secret.
        var probe = new ConfluentKafkaTransportProbe(
            new KafkaClientConfigFactory(KafkaTestSupport.ValidSettings(), security),
            _ => Task.FromException<int>(new InvalidOperationException(s.Username + s.Password)));
        var health = probe.ProbeAsync(CancellationToken.None).GetAwaiter().GetResult();
        Check("Probe.UnavailableDependency", health.UnavailableDependency);
        if (health.Reachable || health.UnavailableDependency != ConfluentKafkaTransportProbe.DependencyName)
        {
            leaks.Add("Probe.UnexpectedHealth");
        }

        // 6. Publisher logs and failed PublishResult for a produce failure.
        var envelope = Envelope(s, payloadBytes);
        var publisherLog = new CapturingLogger<KafkaMessagePublisher>();
        var publisher = new KafkaMessagePublisher(
            new FakeKafkaProducer(new KafkaProduceException("test-topic", "persistence status NotPersisted")),
            KafkaTestSupport.ValidSettings(),
            publisherLog);
        var publishResult = publisher.PublishAsync(envelope, CancellationToken.None).GetAwaiter().GetResult();
        Check("KafkaMessagePublisher.PublishResult", publishResult.Error);
        publisherLog.Entries.ForEach(t => Check("KafkaMessagePublisher.Log", t));

        // 7. Hosted service failure log + consumer host logs, with a processor whose exception echoes the payload.
        var (hostedLogs, hostLogs) = RunHostedServiceFailure(s, envelope);
        hostedLogs.ForEach(t => Check("KafkaConsumerHostedService.Log", t));
        hostLogs.ForEach(t => Check("KafkaConsumerHost.Log", t));

        // 8. ConfluentKafkaConsumer (constructible without a broker): logs + commit-failure exception.
        var consumerLog = new CapturingLogger<ConfluentKafkaConsumer>();
        var factory = new KafkaClientConfigFactory(KafkaTestSupport.ValidSettings(), security);
        using (var consumer = new ConfluentKafkaConsumer(factory, consumerLog))
        {
            try
            {
                consumer.CommitAsync(new KafkaConsumeResult(envelope, 0, 7), CancellationToken.None)
                    .GetAwaiter().GetResult();
                leaks.Add("ConfluentKafkaConsumer.CommitDidNotThrow");
            }
            catch (KafkaOffsetCommitException ex)
            {
                Check("ConfluentKafkaConsumer.KafkaOffsetCommitException", ex.Message);
            }
        }

        consumerLog.Entries.ForEach(t => Check("ConfluentKafkaConsumer.Log", t));

        // 9. ConfluentKafkaProducer construction/disposal logs (no produce: that would need a broker).
        var producerLog = new CapturingLogger<ConfluentKafkaProducer>();
        using (var producer = new ConfluentKafkaProducer(factory, KafkaTestSupport.ValidSettings(), producerLog))
        {
        }

        producerLog.Entries.ForEach(t => Check("ConfluentKafkaProducer.Log", t));

        // 10. Redaction totality: a sensitive-keyed value is always the marker.
        var redactor = new PayloadRedactor();
        var redacted = redactor.RedactValue(s.SensitiveKey, s.Password);
        var redactedMap = redactor.Redact(new Dictionary<string, string> { [s.SensitiveKey] = s.Password });
        if (redacted != PayloadRedactor.RedactionMarker
            || redactedMap[s.SensitiveKey] != PayloadRedactor.RedactionMarker)
        {
            leaks.Add("PayloadRedactor.NotMarker");
        }

        return leaks.Count == 0
            ? true.ToProperty()
            : false.ToProperty().Label($"leaking surfaces: {string.Join(", ", leaks.Distinct())}; {s}");
    }

    // ----- Surfaces ---------------------------------------------------------------------------

    private static readonly KafkaSaslMechanism[] Mechanisms =
    {
        KafkaSaslMechanism.Plain,
        KafkaSaslMechanism.ScramSha256,
        KafkaSaslMechanism.ScramSha512,
    };

    private static IEnumerable<IReadOnlyDictionary<string, string>> StartupSettings(Scenario s)
    {
        var protocol = s.Ssl ? "SaslSsl" : "SaslPlaintext";

        // Broken bootstrap + SASL without mechanism (credentials supplied).
        yield return new Dictionary<string, string>
        {
            [KafkaProviderSettings.BootstrapServersSettingName] = " ",
            [KafkaProviderSettings.SecurityProtocolSettingName] = protocol,
            [KafkaProviderSettings.SaslUsernameSettingName] = s.Username,
            [KafkaProviderSettings.SaslPasswordSettingName] = s.Password,
        };

        // Fully configured except the group/topic; mechanism present, one credential blank.
        yield return new Dictionary<string, string>
        {
            [KafkaProviderSettings.BootstrapServersSettingName] = "broker-1:9092",
            [KafkaProviderSettings.SecurityProtocolSettingName] = protocol,
            [KafkaProviderSettings.SaslMechanismSettingName] = "Plain",
            [KafkaProviderSettings.SaslUsernameSettingName] = s.Username,
            [KafkaProviderSettings.SaslPasswordSettingName] = " ",
        };

        // Out-of-set selectors alongside credentials.
        yield return new Dictionary<string, string>
        {
            [KafkaProviderSettings.SecurityProtocolSettingName] = "Kerberos",
            [KafkaProviderSettings.SaslMechanismSettingName] = "scramsha256",
            [KafkaProviderSettings.SaslUsernameSettingName] = s.Username,
            [KafkaProviderSettings.SaslPasswordSettingName] = s.Password,
        };
    }

    private static Headers ValidHeaders(Scenario s)
    {
        var headers = new Headers
        {
            { KafkaWireFormat.MessageIdHeader, Encoding.UTF8.GetBytes(Guid.NewGuid().ToString("D")) },
            { KafkaWireFormat.CorrelationIdHeader, Encoding.UTF8.GetBytes("corr-1") },
            { KafkaWireFormat.MessageTypeHeader, Encoding.UTF8.GetBytes("test.contract") },
            { KafkaWireFormat.CreatedAtHeader, Encoding.UTF8.GetBytes(DateTimeOffset.UtcNow.ToString("O", CultureInfo.InvariantCulture)) },
            { KafkaWireFormat.ContractVersionHeader, Encoding.UTF8.GetBytes("1") },
            { "x-user", Encoding.UTF8.GetBytes(s.HeaderValue) },
        };
        return headers;
    }

    private static void AssertThrowsMissingHeader(
        Headers headers, byte[] payload, string label, Action<string, string?> check)
    {
        try
        {
            KafkaWireFormat.ToEnvelope(new Message<Ignore, byte[]> { Value = payload, Headers = headers });
            check($"MissingRequiredHeaderException.{label}.DidNotThrow", PayloadAlphabet);
        }
        catch (MissingRequiredHeaderException ex)
        {
            check($"MissingRequiredHeaderException.{label}", ex.Message);
        }
    }

    private static MessageEnvelope Envelope(Scenario s, byte[] payload) =>
        KafkaTestSupport.ValidEnvelope() with
        {
            Payload = new ReadOnlyMemory<byte>(payload),
            Headers = new Dictionary<string, string> { ["x-user"] = s.HeaderValue },
        };

    private static (List<string> Hosted, List<string> Host) RunHostedServiceFailure(Scenario s, MessageEnvelope envelope)
    {
        var hostedLog = new CapturingLogger<KafkaConsumerHostedService>();
        var hostLog = new CapturingLogger<KafkaConsumerHost>();
        var failed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var backoff = TimeSpan.FromSeconds(2);

        var failure = new InvalidOperationException($"processing failed for {s.Payload} {s.HeaderValue} {s.Password}");
        failure.Data[KafkaConsumerHostedService.CorrelationIdDataKey] = envelope.CorrelationId;

        var consumer = new FakeKafkaConsumer(new KafkaConsumeResult(envelope, 0, 5));
        var host = new KafkaConsumerHost(
            consumer, new FakeStreamMessageProcessor(failure), KafkaTestSupport.ValidSettings(), hostLog);
        var service = new KafkaConsumerHostedService(
            host,
            new LifecycleConfig { ShutdownTimeoutSeconds = 30 },
            hostedLog,
            backoff,
            (span, ct) =>
            {
                if (span == backoff)
                {
                    failed.TrySetResult();
                }

                var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                ct.Register(() => tcs.TrySetCanceled(ct));
                return tcs.Task;
            });

        service.StartAsync(CancellationToken.None).GetAwaiter().GetResult();
        try
        {
            failed.Task.WaitAsync(TimeSpan.FromSeconds(10)).GetAwaiter().GetResult();
        }
        finally
        {
            service.StopAsync(CancellationToken.None).GetAwaiter().GetResult();
        }

        return (hostedLog.Entries, hostLog.Entries);
    }

    private static bool ContainsForbidden(string text) =>
        text.IndexOfAny((SecretAlphabet + PayloadAlphabet + HeaderAlphabet).ToCharArray()) >= 0;

    // ----- Capturing logger -------------------------------------------------------------------

    private sealed class CapturingLogger<T> : ILogger<T>
    {
        private readonly List<string> _entries = new();

        /// <summary>Snapshot of formatted messages, including the exception message chain when one is logged.</summary>
        public List<string> Entries
        {
            get
            {
                lock (_entries)
                {
                    return _entries.ToList();
                }
            }
        }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            var text = new StringBuilder(formatter(state, exception));
            for (var ex = exception; ex is not null; ex = ex.InnerException)
            {
                text.Append(' ').Append(ex.GetType().Name).Append(": ").Append(ex.Message);
            }

            lock (_entries)
            {
                _entries.Add(text.ToString());
            }
        }
    }

    // ----- Generators -------------------------------------------------------------------------

    public static Arbitrary<Scenario> ScenarioArbitrary() => Arb.From(ScenarioGen());

    private static Gen<string> Text(string alphabet) =>
        from chars in Gen.NonEmptyListOf(Gen.Elements(alphabet.ToCharArray()))
        select new string(chars.ToArray());

    private static Gen<Scenario> ScenarioGen() =>
        from user in Text(SecretAlphabet)
        from password in Text(SecretAlphabet)
        from payload in Text(PayloadAlphabet)
        from header in Text(HeaderAlphabet)
        from ssl in Gen.Elements(true, false)
        from mech in Gen.Elements(0, 1, 2)
        from dropped in Gen.Elements(0, 1, 2, 3, 4)
        from key in Gen.Elements(SensitiveKeys)
        select new Scenario(user, password, payload, header, ssl, mech, dropped, key);
}
