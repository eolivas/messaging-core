namespace Demo.Application.Providers.Queries;

/// <summary>Keeps an unsupported-provider reason within 1 to 200 characters.</summary>
internal static class UnsupportedReason
{
    public const int MaxLength = 200;

    public const string Default =
        "The messaging library has no implementation for this provider.";

    public static string Normalize(string? reason)
    {
        if (string.IsNullOrWhiteSpace(reason))
        {
            return Default;
        }

        var trimmed = reason.Trim();
        return trimmed.Length <= MaxLength ? trimmed : trimmed[..MaxLength];
    }
}
