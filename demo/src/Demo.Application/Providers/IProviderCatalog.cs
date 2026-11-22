namespace Demo.Application.Providers;

/// <summary>Port: the providers the demo knows about, their defaults and their settings validation.</summary>
public interface IProviderCatalog
{
    IReadOnlyList<ProviderDescriptor> List();

    /// <summary>Raw defaults with secret flags. Returns null for an unknown provider.</summary>
    ProviderSettingsSnapshot? GetDefaults(ProviderId id);

    IReadOnlyList<SettingError> Validate(ProviderId id, IReadOnlyDictionary<string, string> effective);
}
