using Messaging.Abstractions;
using Messaging.Configuration;
using Messaging.Providers;

namespace Messaging.Startup;

/// <summary>
/// The pure Application-layer startup gate that validates <see cref="MessagingConfiguration"/>
/// fail-fast and resolves the single active <see cref="IMessagingProvider"/> for the configured
/// <see cref="MessagingMode"/> before the Messaging Core is allowed to process any message
/// (Req 1.1, 1.4, 1.5, 1.6, 2.1). It contains no hosting dependency, so the whole startup decision —
/// aggregate-and-fail-fast on invalid config, then resolve the active provider — is unit-testable
/// without a host (the hosted glue that invokes it on startup is a separate thin adapter).
/// </summary>
/// <remarks>
/// <para>
/// Startup sequence (mirrors the design's fail-fast flow):
/// </para>
/// <list type="number">
///   <item><description>
///   <b>Mode presence/recognition (Req 1.2, 1.3).</b> A missing or unrecognized mode is reported by a
///   mode-only validation pass and throws <see cref="MessagingConfigurationException"/> before any
///   resolution is attempted — there is no provider to look up for an absent mode.
///   </description></item>
///   <item><description>
///   <b>Provider resolution (Req 1.5, 1.6, 3.5, 3.7).</b> The registry resolves the provider for the
///   configured mode. Zero or ambiguous mappings surface as a <see cref="ProviderResolutionException"/>
///   so startup fails fast with the unresolved/ambiguous mode.
///   </description></item>
///   <item><description>
///   <b>Full validation incl. provider-declared settings (Req 2.2–2.6).</b> With the active provider
///   known, the configuration is validated again against that provider's
///   <see cref="IMessagingProvider.RequiredSettings"/>, aggregating every remaining error (numeric
///   bounds + provider-specific settings) into one <see cref="MessagingConfigurationException"/>.
///   </description></item>
/// </list>
/// <para>
/// The resolved provider is returned so the caller can expose the active mode on readiness (Req 1.7)
/// and so the publish/consume pipeline binds the same single provider. Resolution is stable for the
/// instance lifetime because the registry is immutable and the mode cannot change after startup
/// (Req 1.4); re-running <see cref="Validate"/> yields the same provider.
/// </para>
/// <para>
/// SRP/DIP: this type owns only the validate-then-resolve decision. It depends on the
/// <see cref="IProviderRegistry"/> port and composes <see cref="ConfigurationValidator"/> for the
/// aggregating pass; it never references a concrete provider assembly.
/// </para>
/// </remarks>
public sealed class MessagingStartupValidator
{
    private readonly MessagingConfiguration _configuration;
    private readonly IProviderRegistry _providerRegistry;

    /// <summary>
    /// Creates the startup validator over the configured settings and the provider registry built
    /// from the DI-registered providers.
    /// </summary>
    /// <param name="configuration">The messaging configuration to validate (Req 2.1).</param>
    /// <param name="providerRegistry">The registry that resolves the active provider (Req 1.5).</param>
    /// <exception cref="ArgumentNullException">If any dependency is null.</exception>
    public MessagingStartupValidator(
        MessagingConfiguration configuration,
        IProviderRegistry providerRegistry)
    {
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        _providerRegistry = providerRegistry ?? throw new ArgumentNullException(nameof(providerRegistry));
    }

    /// <summary>
    /// Runs the fail-fast startup gate: validates the configuration, resolves the active provider for
    /// the configured mode, and validates provider-declared settings, returning the resolved provider
    /// so the active mode can be exposed on readiness (Req 1.5, 1.6, 1.7, 2.1).
    /// </summary>
    /// <returns>The single active <see cref="IMessagingProvider"/> for the configured mode.</returns>
    /// <exception cref="MessagingConfigurationException">
    /// If the configuration is missing values or has values outside their permitted set/range; every
    /// error is aggregated into the thrown exception (Req 1.2, 1.3, 2.2, 2.3, 2.4).
    /// </exception>
    /// <exception cref="ProviderResolutionException">
    /// If the configured mode maps to zero or to more than one registered provider (Req 1.6, 3.5, 3.7).
    /// </exception>
    public IMessagingProvider Validate()
    {
        // Stage 1: mode presence/recognition. Resolving a provider is only meaningful once the mode is
        // a recognized value, so a missing/unrecognized mode fails here before resolution (Req 1.2, 1.3).
        var modeOnly = new ConfigurationValidator();
        var modeResult = modeOnly.Validate(_configuration);
        if (!modeResult.IsValid)
        {
            throw new MessagingConfigurationException(modeResult.Errors);
        }

        // Mode is guaranteed present and recognized by a valid mode-only pass above.
        var mode = _configuration.Mode!.Value;

        // Stage 2: resolve the single active provider for the configured mode. Zero/ambiguous mappings
        // throw ProviderResolutionException, failing startup fast (Req 1.5, 1.6, 3.5, 3.7).
        var provider = _providerRegistry.Resolve(mode);

        // Stage 3: full validation against the active provider's declared required settings, so a
        // missing/invalid provider-specific setting is reported alongside any numeric-bound error in a
        // single aggregated failure (Req 2.5, 2.2, 2.3).
        var fullValidator = new ConfigurationValidator(provider.RequiredSettings);
        var fullResult = fullValidator.Validate(_configuration);
        if (!fullResult.IsValid)
        {
            throw new MessagingConfigurationException(fullResult.Errors);
        }

        return provider;
    }
}
