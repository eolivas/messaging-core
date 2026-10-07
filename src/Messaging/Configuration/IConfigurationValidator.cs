using Messaging.Abstractions;

namespace Messaging.Configuration;

/// <summary>
/// Validates <see cref="MessagingConfiguration"/> at startup, aggregating every error in a single
/// pass and never stopping at the first (Req 2.2, 2.3, 2.4, 2.5).
/// </summary>
/// <remarks>
/// Layering decision: although the design diagram groups this port with the Domain ports, it
/// depends on <see cref="MessagingConfiguration"/>, which is an Application-layer type. Placing
/// the interface in Domain would force Domain to reference an Application type and invert the
/// dependency direction. Per Clean Architecture (and the project convention that ports may be
/// declared in Domain <em>or</em> Application), this port therefore lives in the Application
/// project alongside the configuration model it validates. The pure value types it returns —
/// <see cref="ConfigurationValidationResult"/> and <see cref="ConfigurationError"/> — carry no
/// configuration dependency and remain in Domain.
/// </remarks>
public interface IConfigurationValidator
{
    /// <summary>
    /// Validates the configuration and returns every error found; the result is valid only when it
    /// contains zero errors (Req 2.2, 2.3, 2.4, 2.5, 2.6).
    /// </summary>
    /// <param name="configuration">The configuration to validate.</param>
    ConfigurationValidationResult Validate(MessagingConfiguration configuration);
}
