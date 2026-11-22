using System.Text.RegularExpressions;

namespace Demo.SalesLeads.Application;

/// <summary>
/// Local sanitizer for failure reasons. Redacts URI user-info, key/value secrets and
/// token-like values first, then truncates to 200 characters (Req 11.6).
/// </summary>
internal static class ReasonSanitizer
{
    internal const int MaxLength = 200;
    internal const string Redacted = "***";

    private static readonly TimeSpan Timeout = TimeSpan.FromMilliseconds(250);

    private static readonly Regex UriUserInfo = new(
        @"(?<scheme>[A-Za-z][A-Za-z0-9+.\-]*://)[^/\s@]*@",
        RegexOptions.Compiled | RegexOptions.CultureInvariant,
        Timeout);

    private static readonly Regex KeyValueSecret = new(
        @"(?<key>\b(?:password|pwd|passwd|secret|[A-Za-z]*key|token|credentials?)\s*[=:]\s*)(?<value>""[^""]*""|'[^']*'|[^;,\s&]+)",
        RegexOptions.Compiled | RegexOptions.CultureInvariant | RegexOptions.IgnoreCase,
        Timeout);

    private static readonly Regex Bearer = new(
        @"\b(?<scheme>Bearer|Basic)\s+[A-Za-z0-9\-._~+/]+=*",
        RegexOptions.Compiled | RegexOptions.CultureInvariant | RegexOptions.IgnoreCase,
        Timeout);

    private static readonly Regex Opaque = new(
        @"\beyJ[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]*|\b(?:AKIA|ASIA)[A-Z0-9]{16}\b|(?<![A-Za-z0-9])[A-Za-z0-9+/_\-]{32,}={0,2}(?![A-Za-z0-9])",
        RegexOptions.Compiled | RegexOptions.CultureInvariant,
        Timeout);

    internal static string Sanitize(string? reason)
    {
        if (string.IsNullOrWhiteSpace(reason))
        {
            return "Publish failed.";
        }

        string text = reason;
        try
        {
            text = UriUserInfo.Replace(text, "${scheme}");
            text = KeyValueSecret.Replace(text, "${key}" + Redacted);
            text = Bearer.Replace(text, "${scheme} " + Redacted);
            text = Opaque.Replace(text, Redacted);
        }
        catch (RegexMatchTimeoutException)
        {
            return "Failure reason withheld.";
        }

        text = text.Trim();
        return text.Length <= MaxLength ? text : text[..MaxLength];
    }
}
