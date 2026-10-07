namespace Messaging.Observability;

/// <summary>
/// A pure, deterministic redactor that replaces secret or sensitive values with a single fixed marker
/// before anything is handed to a log entry (Req 9.7, code-conventions security). A value flagged as
/// sensitive — either because its key name matches a known-sensitive name or because the caller
/// explicitly flags it — is swapped for <see cref="RedactionMarker"/>, so the original sensitive value
/// never appears in a written log entry; sensitive values are referenced by key name only.
/// </summary>
/// <remarks>
/// <para>
/// Matching is by <em>key name</em>, case-insensitive, against a configurable set of sensitive names
/// (defaulting to the common secret-bearing field names). This keeps the redactor a pure function of
/// its inputs — no I/O, no mutation of the source — so it is trivially unit- and property-testable
/// (task 15.3) and safe to call on the hot path. The core never logs raw payload bytes at all; this
/// redactor guards the structured key/value fields that <em>could</em> carry a secret (for example
/// header values or a flattened payload view an adapter chooses to log).
/// </para>
/// <para>
/// The marker is intentionally a constant with no fragment of the original value, so redaction cannot
/// leak length or prefix information. Redaction is total: an unknown or null value under a sensitive
/// key is still replaced with the marker rather than passed through.
/// </para>
/// </remarks>
public sealed class PayloadRedactor
{
    /// <summary>The fixed marker written in place of any sensitive value (Req 9.7).</summary>
    public const string RedactionMarker = "***REDACTED***";

    private static readonly IReadOnlySet<string> DefaultSensitiveKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
    {
        "password",
        "passwd",
        "pwd",
        "secret",
        "token",
        "accesstoken",
        "access_token",
        "refreshtoken",
        "refresh_token",
        "apikey",
        "api_key",
        "authorization",
        "auth",
        "credential",
        "credentials",
        "privatekey",
        "private_key",
        "connectionstring",
        "connection_string",
        "ssn",
        "creditcard",
        "credit_card",
        "cardnumber",
        "card_number",
        "cvv",
        "pin",
    };

    private readonly IReadOnlySet<string> _sensitiveKeys;

    /// <summary>Creates a redactor using the default set of known-sensitive key names.</summary>
    public PayloadRedactor()
        : this(DefaultSensitiveKeys)
    {
    }

    /// <summary>
    /// Creates a redactor over a caller-supplied set of sensitive key names. The set is copied with a
    /// case-insensitive comparer so matching is stable regardless of the caller's comparer and the
    /// redactor is unaffected by later mutation of the source collection.
    /// </summary>
    /// <param name="sensitiveKeys">The key names whose values must be redacted.</param>
    /// <exception cref="ArgumentNullException">If <paramref name="sensitiveKeys"/> is null.</exception>
    public PayloadRedactor(IEnumerable<string> sensitiveKeys)
    {
        ArgumentNullException.ThrowIfNull(sensitiveKeys);
        _sensitiveKeys = new HashSet<string>(sensitiveKeys, StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Returns whether the value under <paramref name="key"/> is sensitive and must be redacted
    /// (Req 9.7). Matching is case-insensitive on the key name.
    /// </summary>
    /// <param name="key">The field or header key name.</param>
    /// <returns><see langword="true"/> if a value under this key must be redacted.</returns>
    public bool IsSensitive(string key)
    {
        ArgumentNullException.ThrowIfNull(key);
        return _sensitiveKeys.Contains(key);
    }

    /// <summary>
    /// Returns the value to log for <paramref name="key"/>: the fixed <see cref="RedactionMarker"/>
    /// when the key is sensitive, otherwise <paramref name="value"/> unchanged (Req 9.7).
    /// </summary>
    /// <param name="key">The field or header key name.</param>
    /// <param name="value">The original value associated with the key.</param>
    /// <returns>The redaction marker for sensitive keys; the original value otherwise.</returns>
    public string RedactValue(string key, string value)
    {
        ArgumentNullException.ThrowIfNull(key);
        return IsSensitive(key) ? RedactionMarker : value;
    }

    /// <summary>
    /// Produces a redacted copy of <paramref name="fields"/> safe to write to a log entry: every value
    /// whose key is sensitive is replaced with the fixed <see cref="RedactionMarker"/>; all other
    /// entries are preserved (Req 9.7). The input is not mutated.
    /// </summary>
    /// <param name="fields">The structured key/value fields a caller intends to log.</param>
    /// <returns>A new dictionary with sensitive values redacted.</returns>
    /// <exception cref="ArgumentNullException">If <paramref name="fields"/> is null.</exception>
    public IReadOnlyDictionary<string, string> Redact(IReadOnlyDictionary<string, string> fields)
    {
        ArgumentNullException.ThrowIfNull(fields);

        var redacted = new Dictionary<string, string>(fields.Count);
        foreach (var (key, value) in fields)
        {
            redacted[key] = IsSensitive(key) ? RedactionMarker : value;
        }

        return redacted;
    }
}
