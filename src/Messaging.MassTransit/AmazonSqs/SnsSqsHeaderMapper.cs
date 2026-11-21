using System.Text.RegularExpressions;

namespace Messaging.MassTransit.AmazonSqs;

/// <summary>
/// Pure partition of envelope headers between SNS/SQS message attributes and the message body.
/// SNS and SQS allow at most 10 message attributes, so three are reserved and at most seven user
/// headers become <c>h.{key}</c> attributes; every other header stays in the body.
/// </summary>
public static partial class SnsSqsHeaderMapper
{
    /// <summary>Reserved attribute: correlation id.</summary>
    public const string CorrelationIdAttribute = "x-correlation-id";

    /// <summary>Reserved attribute: W3C trace context.</summary>
    public const string TraceParentAttribute = "traceparent";

    /// <summary>Reserved attribute: message type.</summary>
    public const string MessageTypeAttribute = "x-message-type";

    /// <summary>Prefix of user header attribute names.</summary>
    public const string UserHeaderPrefix = "h.";

    /// <summary>The SNS/SQS limit on message attributes.</summary>
    public const int MaxAttributes = 10;

    /// <summary>The number of attributes available to user headers.</summary>
    public const int MaxUserAttributes = MaxAttributes - 3;

    /// <summary>The maximum length of an eligible header value.</summary>
    public const int MaxValueLength = 1024;

    private static readonly string[] ReservedKeys =
    {
        CorrelationIdAttribute,
        TraceParentAttribute,
        MessageTypeAttribute,
    };

    /// <summary>Partitions the headers into attribute headers and body headers.</summary>
    /// <param name="headers">The envelope headers.</param>
    /// <param name="traceHeaders">The trace and correlation headers produced on publish.</param>
    /// <returns>The partition and the transport-header allow-list.</returns>
    /// <exception cref="ArgumentNullException">If an argument is null.</exception>
    public static SnsSqsHeaderSplit Split(
        IReadOnlyDictionary<string, string> headers,
        IReadOnlyDictionary<string, string> traceHeaders)
    {
        ArgumentNullException.ThrowIfNull(headers);
        ArgumentNullException.ThrowIfNull(traceHeaders);

        var reserved = new Dictionary<string, string>(capacity: 2);
        foreach (var key in new[] { CorrelationIdAttribute, TraceParentAttribute })
        {
            if (traceHeaders.TryGetValue(key, out var value))
            {
                reserved[key] = value;
            }
        }

        var selected = headers
            .Where(header => IsEligible(header.Key, header.Value))
            .OrderBy(header => header.Key, StringComparer.Ordinal)
            .Take(MaxUserAttributes)
            .ToList();

        var attributes = new Dictionary<string, string>(selected.Count);
        var selectedKeys = new HashSet<string>(StringComparer.Ordinal);
        foreach (var header in selected)
        {
            attributes[UserHeaderPrefix + header.Key] = header.Value;
            selectedKeys.Add(header.Key);
        }

        var body = headers
            .Where(header => !selectedKeys.Contains(header.Key))
            .ToDictionary(header => header.Key, header => header.Value, StringComparer.Ordinal);

        var allowed = new HashSet<string>(ReservedKeys, StringComparer.Ordinal);
        allowed.UnionWith(attributes.Keys);

        return new SnsSqsHeaderSplit(reserved, attributes, body, allowed);
    }

    private static bool IsEligible(string key, string value) =>
        !string.IsNullOrEmpty(value)
        && value.Length <= MaxValueLength
        && KeyPattern().IsMatch(key)
        && Array.IndexOf(ReservedKeys, key) < 0;

    [GeneratedRegex("^[A-Za-z0-9_-]{1,200}$", RegexOptions.CultureInvariant)]
    private static partial Regex KeyPattern();
}
