using System.Text;
using Messaging.Abstractions;

namespace Messaging.Startup;

/// <summary>
/// The fail-fast startup exception thrown when <see cref="MessagingConfiguration"/> validation finds
/// one or more problems (Req 1.2, 1.3, 2.1, 2.2, 2.3, 2.4). It aggregates <em>every</em>
/// <see cref="ConfigurationError"/> from the single validation pass into one descriptive message so
/// the operator sees the complete problem set at once rather than fixing errors one restart at a
/// time, and it prevents the Messaging Core from entering a processing-ready state (Req 2.2).
/// </summary>
/// <remarks>
/// A domain-specific exception (never a bare <see cref="Exception"/>) per the error-handling
/// conventions: the type names the failure category (invalid messaging configuration) and carries the
/// structured <see cref="Errors"/> so a caller can inspect each failure programmatically in addition
/// to reading the formatted message. The message lists each error by its configuration setting name
/// together with the permitted set/range where applicable (Req 2.3).
/// </remarks>
public sealed class MessagingConfigurationException : Exception
{
    /// <summary>
    /// Creates the exception from the complete set of validation errors found in one pass (Req 2.2).
    /// </summary>
    /// <param name="errors">Every configuration error found; must contain at least one error.</param>
    /// <exception cref="ArgumentNullException">If <paramref name="errors"/> is null.</exception>
    /// <exception cref="ArgumentException">If <paramref name="errors"/> is empty.</exception>
    public MessagingConfigurationException(IReadOnlyList<ConfigurationError> errors)
        : base(FormatMessage(errors))
    {
        ArgumentNullException.ThrowIfNull(errors);
        if (errors.Count == 0)
        {
            throw new ArgumentException(
                "A configuration exception must carry at least one error.", nameof(errors));
        }

        Errors = errors;
    }

    /// <summary>Every validation failure that prevented startup, reported together (Req 2.2).</summary>
    public IReadOnlyList<ConfigurationError> Errors { get; }

    /// <summary>
    /// Builds a single descriptive message that enumerates every error by setting name, including the
    /// permitted set/range when the failure is a range/set violation (Req 2.3, 2.4).
    /// </summary>
    private static string FormatMessage(IReadOnlyList<ConfigurationError> errors)
    {
        ArgumentNullException.ThrowIfNull(errors);

        var builder = new StringBuilder();
        builder.Append("Messaging configuration is invalid; startup aborted with ")
            .Append(errors.Count)
            .Append(errors.Count == 1 ? " error:" : " errors:");

        foreach (var error in errors)
        {
            builder.Append(Environment.NewLine)
                .Append(" - ")
                .Append(error.SettingName)
                .Append(": ")
                .Append(error.Message);

            if (!string.IsNullOrWhiteSpace(error.PermittedValuesOrRange))
            {
                builder.Append(" (permitted: ")
                    .Append(error.PermittedValuesOrRange)
                    .Append(')');
            }
        }

        return builder.ToString();
    }
}
