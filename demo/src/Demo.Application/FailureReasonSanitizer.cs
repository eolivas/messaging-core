using System.Text.RegularExpressions;

namespace Demo.Application;

/// <summary>
/// Makes a transport failure reason safe for flow steps, logs and responses.
/// Redacts first, then truncates, so a cut can never expose part of a secret.
/// </summary>
public static class FailureReasonSanitizer
{
    public const int MaxLength = 200;
    public const string Redacted = "***";

    private static readonly TimeSpan RegexTimeout = TimeSpan.FromMilliseconds(250);

    // scheme://user:pass@host -> scheme://host
    private static readonly Regex UriUserInfo = new(
        @"(?<scheme>[A-Za-z][A-Za-z0-9+.\-]*://)[^/\s@]*@",
        RegexOptions.Compiled | RegexOptions.CultureInvariant,
        RegexTimeout);

    // Password=...; AccountKey=...; SharedAccessKey=...; token=...
    private static readonly Regex KeyValueSecret = new(
        @"(?<key>\b(?:password|pwd|passwd|secret|(?:[A-Za-z]+)?key|sharedaccesskey|sharedaccesssignature|sas|token|access[_-]?token|api[_-]?key|client[_-]?secret|sasl\.password|sasl_password|credentials?)\s*[=:]\s*)(?<value>""[^""]*""|'[^']*'|[^;,\s&]+)",
        RegexOptions.Compiled | RegexOptions.CultureInvariant | RegexOptions.IgnoreCase,
        RegexTimeout);

    private static readonly Regex BearerToken = new(
        @"\b(?<scheme>Bearer|Basic)\s+[A-Za-z0-9\-._~+/]+=*",
        RegexOptions.Compiled | RegexOptions.CultureInvariant | RegexOptions.IgnoreCase,
        RegexTimeout);

    private static readonly Regex Jwt = new(
        @"\beyJ[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]*",
        RegexOptions.Compiled | RegexOptions.CultureInvariant,
        RegexTimeout);

    // AWS access key ids and long opaque base64/hex-like runs.
    private static readonly Regex OpaqueToken = new(
        @"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b|(?<![A-Za-z0-9])[A-Za-z0-9+/_\-]{32,}={0,2}(?![A-Za-z0-9])",
        RegexOptions.Compiled | RegexOptions.CultureInvariant,
        RegexTimeout);

    public static string Sanitize(string? reason)
    {
        if (string.IsNullOrWhiteSpace(reason))
        {
            return string.Empty;
        }

        string text = reason;
        try
        {
            text = UriUserInfo.Replace(text, "${scheme}");
            text = KeyValueSecret.Replace(text, "${key}" + Redacted);
            text = BearerToken.Replace(text, "${scheme} " + Redacted);
            text = Jwt.Replace(text, Redacted);
            text = OpaqueToken.Replace(text, Redacted);
        }
        catch (RegexMatchTimeoutException)
        {
            // Fail closed: if redaction cannot finish, expose nothing from the input.
            return "Failure reason withheld.";
        }

        text = text.Trim();
        return text.Length <= MaxLength ? text : text[..MaxLength];
    }
}
