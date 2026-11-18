using System.Text.RegularExpressions;

namespace Messaging.MassTransit.Tests;

/// <summary>
/// Resolves the LocalStack image tag used by the integration fixture. Only patch-level
/// versions (<c>major.minor.patch</c>) are accepted so runs are reproducible; floating tags
/// such as <c>latest</c> or <c>4</c> are rejected.
/// </summary>
public static class LocalStackImageTag
{
    /// <summary>Pinned default tag (last 4.x release that runs without an auth token).</summary>
    public const string DefaultTag = "4.14.0";

    /// <summary>Environment variable that overrides <see cref="DefaultTag"/>.</summary>
    public const string EnvironmentVariable = "LOCALSTACK_IMAGE_TAG";

    private static readonly Regex PatchVersion =
        new(@"\A\d+\.\d+\.\d+\z", RegexOptions.CultureInvariant | RegexOptions.ExplicitCapture);

    /// <summary>
    /// Returns <see cref="DefaultTag"/> when <paramref name="override"/> is null or empty;
    /// otherwise returns it if it is <c>major.minor.patch</c>.
    /// </summary>
    /// <exception cref="ArgumentException">The override is not a patch-level version.</exception>
    public static string Resolve(string? @override)
    {
        if (string.IsNullOrEmpty(@override))
        {
            return DefaultTag;
        }

        if (!PatchVersion.IsMatch(@override))
        {
            throw new ArgumentException(
                $"LocalStack image tag '{@override}' is not allowed. Use a patch-level version "
                + "such as '4.14.0'; floating tags (latest, bare major) and partial versions are rejected.",
                nameof(@override));
        }

        return @override;
    }
}
