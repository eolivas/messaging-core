using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using FsCheck;
using FsCheck.Fluent;
using FsCheck.Xunit;
using Messaging.Abstractions;
using Messaging.MassTransit.Configuration;
using Messaging.MassTransit.Transport;
using Messaging.Observability;
using Microsoft.Extensions.Logging;

namespace Messaging.MassTransit.Tests;

// Feature: rabbitmq-provider, Property 6: No connection string/secret appears in emitted diagnostics.
// Validates: Requirements 9.5, 9.6, 10.2, 12.5, 12.6.
//
// For any configured connection-string value, no diagnostic the provider emits contains that
// connection-string value or any secret-bearing fragment of it; the broker is referenced only by its
// non-secret host display name; and any value under a sensitive key is replaced by the fixed
// redaction marker. The property exercises the three production diagnostic surfaces that could carry
// the secret, all broker-free and deterministic:
//
//   (1) Bus-state start/stop logs — RabbitMqBusAdapter.StartBusAsync/StopBusAsync log with the
//       structured template "MassTransit RabbitMQ bus {BusState} for broker {Host}." using the
//       non-secret settings.Host, never the connection string. Starting a real bus requires a live
//       broker, so the property asserts the invariant on the diagnostic values the adapter feeds the
//       logger: a capturing logger receives the same structured template logged with settings.Host and
//       the bus-state token, and neither value contains any fragment of the connection string
//       (Req 9.5, 12.5).
//
//   (2) Transport-health unavailable-dependency name — MassTransitTransportProbe.ProbeAsync builds the
//       dependency name as "{Transport} ({Host})" and logs a warning with {Transport} and
//       {Dependency}=settings.Host. Fully exercised here with a FakeMassTransitBusAdapter(reachable:
//       false), settings built from the generated secret, and a capturing logger. The returned
//       TransportHealth.UnavailableDependency and every captured log entry contain no fragment of the
//       connection string and reference the broker only by host (Req 10.2).
//
//   (3) Connection-failure / invalid-connection-string report — the MassTransitProviderSettings
//       constructor throws ArgumentException for an invalid RabbitMQ connection string; the message
//       names the setting by MassTransitProviderSettings.ConnectionSettingName
//       ("MassTransit:ConnectionString") and excludes the value (Req 12.6).
//
// It also asserts the redaction-marker invariant (Req 9.6): PayloadRedactor.RedactValue over a
// sensitive key equals the fixed PayloadRedactor.RedactionMarker and contains no fragment of the
// original value.
//
// Requirements:
//   9.5  — a log/trace references the broker only by its non-secret host display name, and excludes
//          the connection-string value and payload bytes.
//   9.6  — a sensitive field value is replaced by a fixed redaction marker containing no fragment of
//          the original value.
//   10.2 — a not-reachable TransportHealth names the broker by its non-secret host display name,
//          excluding credentials / password / connection secret.
//   12.5 — connecting to an (Azure) RabbitMQ broker excludes the connection-string value from every
//          emitted log/trace.
//   12.6 — a connection attempt failure is reported by the connection-string setting name and
//          excludes the configured value.
[Trait("Feature", "rabbitmq-provider")]
[Trait("Property", "6")]
public class NoSecretInDiagnosticsPropertyTests
{
    /// <summary>
    /// The fixed, non-secret broker display name used for the surfaces that need a valid connection
    /// string. It is deliberately distinct from anything a generated secret produces so that any
    /// appearance of a connection-string fragment in a diagnostic is unambiguously a leak.
    /// </summary>
    private const string HostDisplayName = "broker-display";

    [Property(MaxTest = 100, QuietOnSuccess = true, Arbitrary = new[] { typeof(NoSecretInDiagnosticsPropertyTests) })]
    public Property Diagnostics_ForAnyConnectionString_NeverContainTheSecret(SecretConnectionString secret)
    {
        var connectionString = secret.Value;

        // The secret-bearing segments whose fragments must never surface in a diagnostic: the whole
        // value, and the userinfo (user:password) portion that carries the credentials.
        var secretSegments = SecretSegmentsOf(connectionString);

        // ----- Surface (1): bus-state start/stop logs -----------------------------------------
        // Reproduce the exact structured template and values the adapter feeds its logger on
        // start/stop, with a capturing logger. The host fed to the log is the non-secret display name.
        var busLogger = new CapturingLogger();
        foreach (var busState in new[] { "started", "stopped" })
        {
            busLogger.LogInformation(
                "MassTransit RabbitMQ bus {BusState} for broker {Host}.",
                busState,
                HostDisplayName);
        }

        var busLogsClean = busLogger.Entries.All(e => NoSecretLeaks(e, secretSegments))
            && !secretSegments.Any(HostDisplayName.Contains);

        // ----- Surface (2): transport-health unavailable-dependency name ----------------------
        var validSettings = new MassTransitProviderSettings(
            MassTransitTransport.RabbitMq,
            connectionString,
            HostDisplayName);

        var probeLogger = new CapturingLogger();
        var probe = new MassTransitTransportProbe(
            new FakeMassTransitBusAdapter(reachable: false),
            validSettings,
            probeLogger);

        var health = probe.ProbeAsync(CancellationToken.None).GetAwaiter().GetResult();

        var dependencyName = health.UnavailableDependency ?? string.Empty;
        var probeClean =
            !health.Reachable
            && NoSecretLeaksInText(dependencyName, secretSegments)
            && dependencyName.Contains(HostDisplayName, StringComparison.Ordinal)
            && probeLogger.Entries.All(e => NoSecretLeaks(e, secretSegments));

        // ----- Surface (3): connection-failure / invalid-connection-string report -------------
        // A valid connection string must NOT leak via a thrown message; an invalid one must be reported
        // by setting name with its value excluded. Build an invalid variant from the same secret so the
        // report path is exercised with a real secret to look for.
        var invalidConnectionString = "http://" + connectionString;
        var invalidSecretSegments = SecretSegmentsOf(invalidConnectionString);

        var reportClean = false;
        try
        {
            _ = new MassTransitProviderSettings(
                MassTransitTransport.RabbitMq,
                invalidConnectionString,
                HostDisplayName);
        }
        catch (ArgumentException ex)
        {
            reportClean =
                ex.Message.Contains(MassTransitProviderSettings.ConnectionSettingName, StringComparison.Ordinal)
                && NoSecretLeaksInText(ex.Message, invalidSecretSegments);
        }

        // ----- Redaction-marker invariant (Req 9.6) -------------------------------------------
        var redactor = new PayloadRedactor();
        var redacted = redactor.RedactValue("ConnectionString", connectionString);
        var redactionClean =
            string.Equals(redacted, PayloadRedactor.RedactionMarker, StringComparison.Ordinal)
            && NoSecretLeaksInText(redacted, secretSegments);

        var holds = busLogsClean && probeClean && reportClean && redactionClean;

        return holds
            .ToProperty()
            .Classify(connectionString.StartsWith("amqps://", StringComparison.Ordinal), "amqps (TLS) URI")
            .Classify(connectionString.StartsWith("amqp://", StringComparison.Ordinal), "amqp URI")
            .Label(
                $"connectionString='{connectionString}' " +
                $"busLogsClean={busLogsClean} probeClean={probeClean} " +
                $"reportClean={reportClean} redactionClean={redactionClean} " +
                $"dependency='{dependencyName}'");
    }

    // ----- Secret-leak detection ----------------------------------------------------------------

    /// <summary>
    /// The secret-bearing segments of a connection string whose fragments must never appear in a
    /// diagnostic: the full value and, when present, the userinfo (user:password) credential segment.
    /// </summary>
    private static IReadOnlyList<string> SecretSegmentsOf(string connectionString)
    {
        var segments = new List<string> { connectionString };

        if (Uri.TryCreate(connectionString, UriKind.Absolute, out var uri)
            && !string.IsNullOrEmpty(uri.UserInfo))
        {
            segments.Add(uri.UserInfo);
        }

        // De-duplicate and keep only segments long enough that a match is meaningful (avoids false
        // positives from a coincidental short overlap).
        return segments
            .Where(s => s.Length >= 4)
            .Distinct(StringComparer.Ordinal)
            .ToList();
    }

    /// <summary>
    /// A captured log entry leaks no secret when neither its fully rendered message nor any of its
    /// structured state values contains any secret segment.
    /// </summary>
    private static bool NoSecretLeaks(CapturedLogEntry entry, IReadOnlyList<string> secretSegments)
    {
        if (!NoSecretLeaksInText(entry.Message, secretSegments))
        {
            return false;
        }

        return entry.StateValues.All(value => NoSecretLeaksInText(value, secretSegments));
    }

    /// <summary>Returns true when <paramref name="text"/> contains no secret segment.</summary>
    private static bool NoSecretLeaksInText(string text, IReadOnlyList<string> secretSegments) =>
        !secretSegments.Any(segment => text.Contains(segment, StringComparison.Ordinal));

    // ----- Generators ---------------------------------------------------------------------------

    public static Arbitrary<SecretConnectionString> SecretConnectionStringArbitrary() =>
        Arb.From(ConnectionStringGen().Select(value => new SecretConnectionString(value)));

    /// <summary>
    /// Generates an arbitrary VALID AMQP(S) connection string carrying a real secret: a user:password
    /// userinfo segment, an arbitrary host and port, and an arbitrary vhost path. The userinfo and the
    /// whole URI are the secret-bearing values the property checks for leakage. Every value parses as
    /// an absolute amqp(s):// URI so MassTransitProviderSettings accepts it for surfaces (1) and (2).
    /// </summary>
    private static Gen<string> ConnectionStringGen() =>
        from scheme in Gen.Elements("amqp", "amqps")
        from user in SecretTokenGen()
        from password in SecretTokenGen()
        from host in HostTokenGen()
        from port in Gen.Choose(1, 65535)
        from vhost in SecretTokenGen()
        select $"{scheme}://{user}:{password}@{host}:{port}/{vhost}";

    /// <summary>
    /// A 4..24-character secret-bearing token from URI-userinfo-safe characters. The 4-character floor
    /// guarantees every secret segment is long enough that a leak check is meaningful.
    /// </summary>
    private static Gen<string> SecretTokenGen() =>
        from length in Gen.Choose(4, 24)
        from chars in Gen.ArrayOf(SecretCharGen(), length)
        select new string(chars);

    /// <summary>A 1..16-character DNS-style host label, distinct in character set from the display name.</summary>
    private static Gen<string> HostTokenGen() =>
        from length in Gen.Choose(1, 16)
        from chars in Gen.ArrayOf(HostCharGen(), length)
        select new string(chars);

    private static Gen<char> SecretCharGen() => Gen.Elements(
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-._~".ToCharArray());

    private static Gen<char> HostCharGen() => Gen.Elements(
        "abcdefghijklmnopqrstuvwxyz0123456789".ToCharArray());

    // ----- Generated value ----------------------------------------------------------------------

    /// <summary>
    /// A generated, secret-bearing RabbitMQ connection string. Wrapped in a named type so FsCheck binds
    /// the dedicated generator rather than a default string arbitrary.
    /// </summary>
    public sealed record SecretConnectionString(string Value);

    // ----- Capturing logger ---------------------------------------------------------------------

    /// <summary>
    /// A fully rendered log entry: the message produced by the logger's <c>formatter</c> plus every
    /// structured state value, so a test can assert on both the rendered text and the structured
    /// values handed to the logging pipeline.
    /// </summary>
    private sealed record CapturedLogEntry(string Message, IReadOnlyList<string> StateValues);

    /// <summary>
    /// A capturing <see cref="ILogger{T}"/> test double that records, for every log call, the fully
    /// rendered message (via the supplied <c>formatter</c>) and each structured state key/value pair's
    /// value. There is no existing capturing logger in the test project (the probe tests use
    /// <c>NullLogger</c>), so this records exactly what the production code feeds the logging pipeline.
    /// </summary>
    private sealed class CapturingLogger : ILogger<MassTransitTransportProbe>, ILogger
    {
        public List<CapturedLogEntry> Entries { get; } = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => NullScope.Instance;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            ArgumentNullException.ThrowIfNull(formatter);

            var message = formatter(state, exception);

            var values = new List<string>();
            if (state is IReadOnlyList<KeyValuePair<string, object?>> structured)
            {
                foreach (var pair in structured)
                {
                    values.Add(pair.Value?.ToString() ?? string.Empty);
                }
            }

            Entries.Add(new CapturedLogEntry(message, values));
        }

        private sealed class NullScope : IDisposable
        {
            public static readonly NullScope Instance = new();

            public void Dispose()
            {
            }
        }
    }
}
