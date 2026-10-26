using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using FsCheck;
using FsCheck.Fluent;
using FsCheck.Xunit;
using MassTransit;
using Messaging.Contracts;
using Messaging.MassTransit.Transport;
using Messaging.Observability;

namespace Messaging.MassTransit.Tests;

// Feature: rabbitmq-provider, Property 2: Correlation id is carried in the published headers.
// Validates: Requirements 6.4, 9.7.
//
// For any valid MessageEnvelope, the headers the bus adapter hands to the transport on publish/send
// contain a correlation-id entry whose value is character-for-character equal to the envelope's
// correlation id, so a consume trace links to the originating publish trace by an equal correlation
// id value.
//
// This drives the exact header-merge step the production publish/send path runs: the observability
// facade's OnPublish produces the trace/correlation headers (keyed by
// MessagingTraceSource.CorrelationIdHeader), and RabbitMqBusAdapter.MergeTraceHeaders copies them
// onto the outgoing message's transport SendHeaders. A fake SendHeaders captures every Set call so
// the test can assert the correlation id arrived at the transport unchanged — no broker required.
[Trait("Feature", "rabbitmq-provider")]
[Trait("Property", "2")]
public class CorrelationIdInPublishedHeadersPropertyTests
{
    [Property(MaxTest = 100, QuietOnSuccess = true, Arbitrary = new[] { typeof(CorrelationIdInPublishedHeadersPropertyTests) })]
    public Property MergeTraceHeaders_ForAnyEnvelope_CarriesCorrelationIdToTheTransportUnchanged(
        MessageEnvelope envelope)
    {
        // Arrange: a real observability facade wired to inert sinks produces the trace/correlation
        // headers exactly as the adapter obtains them on the publish/send path.
        var observability = BuildObservability();
        var captured = new CapturingSendHeaders();

        // Act: reproduce the adapter's publish/send header step — OnPublish yields the trace headers,
        // MergeTraceHeaders copies them onto the outgoing transport headers.
        using (observability.OnPublish(envelope.CorrelationId, out var traceHeaders))
        {
            RabbitMqBusAdapter.MergeTraceHeaders(captured, traceHeaders);
        }

        // Assert: the transport received a correlation-id entry equal, char for char, to the envelope's.
        var present = captured.TryGetValue(MessagingTraceSource.CorrelationIdHeader, out var value);
        var holds = present && string.Equals(value, envelope.CorrelationId, StringComparison.Ordinal);

        return holds
            .ToProperty()
            .Classify(envelope.CorrelationId.Length == 1, "min-length correlation id")
            .Classify(
                envelope.CorrelationId.Length == MessageEnvelope.MaxCorrelationIdLength,
                "max-length correlation id")
            .Label(
                $"correlationId='{envelope.CorrelationId}' " +
                $"captured='{(present ? value : "<absent>")}'");
    }

    /// <summary>
    /// A real <see cref="ObservabilityContext"/> wired to inert sinks: it is only driven to produce
    /// the trace headers, never asserted on, so a throwing or logging sink cannot affect the property.
    /// </summary>
    private static ObservabilityContext BuildObservability() =>
        new(
            new NoOpMessagingLog(),
            new NoOpMessagingMetrics(),
            new MessagingTraceSource(tracingEnabled: false),
            new PayloadRedactor());

    // ----- Generators -------------------------------------------------------------------------

    public static Arbitrary<MessageEnvelope> MessageEnvelopeArbitrary() =>
        Arb.From(EnvelopeGen());

    /// <summary>
    /// Generates an arbitrary valid envelope. Only the correlation id drives this property, so it is
    /// generated across its full 1..128-character range; the remaining fields are populated with valid
    /// values so the envelope is well-formed.
    /// </summary>
    private static Gen<MessageEnvelope> EnvelopeGen() =>
        from messageId in Gen.Fresh(Guid.NewGuid).Where(id => id != Guid.Empty)
        from correlationId in CorrelationIdGen()
        from messageType in NonWhitespaceTokenGen()
        from contractVersion in Gen.Choose(1, 50)
        select new MessageEnvelope
        {
            MessageId = messageId,
            CorrelationId = correlationId,
            MessageType = messageType,
            CreatedAtUtc = DateTimeOffset.UtcNow,
            Headers = new Dictionary<string, string>(StringComparer.Ordinal),
            Payload = ReadOnlyMemory<byte>.Empty,
            ContractVersion = contractVersion,
        };

    /// <summary>A correlation id of 1..128 characters (inclusive), spanning the permitted range.</summary>
    private static Gen<string> CorrelationIdGen() =>
        from length in Gen.Choose(1, MessageEnvelope.MaxCorrelationIdLength)
        from chars in Gen.ArrayOf(TokenCharGen(), length)
        select new string(chars);

    private static Gen<string> NonWhitespaceTokenGen() =>
        from length in Gen.Choose(1, 32)
        from chars in Gen.ArrayOf(TokenCharGen(), length)
        select new string(chars);

    /// <summary>Visible ASCII token characters (no whitespace), keeping generated values readable.</summary>
    private static Gen<char> TokenCharGen() => Gen.Elements(
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.:".ToCharArray());

    /// <summary>
    /// A capturing <see cref="SendHeaders"/> double that records every string value handed to the
    /// transport via <see cref="Set(string, string?)"/>, so the test can assert exactly what reached
    /// the transport header sink. Only the string <c>Set</c> overload is exercised by the production
    /// merge step; the remaining members satisfy the interface and are unused.
    /// </summary>
    private sealed class CapturingSendHeaders : SendHeaders
    {
        // Values are stored non-null: the production merge step only ever sets real string values.
        private readonly Dictionary<string, string> _set = new(StringComparer.Ordinal);

        public bool TryGetValue(string key, out string? value)
        {
            if (_set.TryGetValue(key, out var v))
            {
                value = v;
                return true;
            }

            value = null;
            return false;
        }

        public void Set(string key, string? value)
        {
            ArgumentNullException.ThrowIfNull(key);
            if (value is null)
            {
                _set.Remove(key);
                return;
            }

            _set[key] = value;
        }

        public void Set(string key, object? value, bool overwrite = true)
        {
            ArgumentNullException.ThrowIfNull(key);
            if (overwrite || !_set.ContainsKey(key))
            {
                Set(key, value?.ToString());
            }
        }

        public IEnumerable<KeyValuePair<string, object>> GetAll() =>
            _set.Select(kvp => new KeyValuePair<string, object>(kvp.Key, kvp.Value));

        public bool TryGetHeader(string key, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out object? value)
        {
            if (_set.TryGetValue(key, out var v))
            {
                value = v;
                return true;
            }

            value = null;
            return false;
        }

        public T? Get<T>(string key, T? defaultValue = default) where T : class =>
            _set.TryGetValue(key, out var v) ? (v as T) ?? defaultValue : defaultValue;

        public T? Get<T>(string key, T? defaultValue = default) where T : struct =>
            defaultValue;

        public IEnumerator<HeaderValue> GetEnumerator() =>
            _set.Select(kvp => new HeaderValue(kvp.Key, kvp.Value)).GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
