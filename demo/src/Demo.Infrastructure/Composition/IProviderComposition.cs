using Demo.Application.Providers;
using Messaging.Abstractions;
using Microsoft.Extensions.DependencyInjection;

namespace Demo.Infrastructure.Composition;

/// <summary>
/// One implementation per messaging provider. It owns the provider's defaults, its settings
/// validation and the DI wiring for the publisher and consumer roles of a microservice host,
/// so a new provider is added by adding a class and a catalog entry.
/// </summary>
public interface IProviderComposition
{
    ProviderId Id { get; }

    MessagingMode Mode { get; }

    /// <summary>Raw defaults with secret flags. Masking happens in Application.</summary>
    ProviderSettingsSnapshot Defaults();

    /// <summary>Validates effective settings without touching the network.</summary>
    IReadOnlyList<SettingError> Validate(IReadOnlyDictionary<string, string> effective);

    /// <summary>Registers the publisher role (Sales Leads MS).</summary>
    void ConfigurePublisher(IServiceCollection services, CompositionContext context);

    /// <summary>Registers the consumer role for one consuming microservice.</summary>
    void ConfigureConsumer(IServiceCollection services, ConsumerIdentity identity, CompositionContext context);
}
