using Demo.Application.Common;
using Demo.Application.Cqrs;

namespace Demo.Application.Providers.Queries;

/// <summary>
/// Applies overrides to the provider defaults and reports the library
/// validator errors for the effective settings. Overrides that target a
/// secret, read-only or unknown setting are rejected before validation.
/// </summary>
public sealed class ValidateProviderSettingsQueryHandler
    : IQueryHandler<ValidateProviderSettingsQuery, ValidateSettingsResponse>
{
    private readonly IProviderCatalog _catalog;

    public ValidateProviderSettingsQueryHandler(IProviderCatalog catalog)
    {
        _catalog = catalog ?? throw new ArgumentNullException(nameof(catalog));
    }

    public Task<Result<ValidateSettingsResponse>> HandleAsync(
        ValidateProviderSettingsQuery query, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(query);
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(Handle(query));
    }

    private Result<ValidateSettingsResponse> Handle(ValidateProviderSettingsQuery query)
    {
        var lookup = ProviderLookup.Find(_catalog, query.ProviderId);
        if (lookup.IsFailure)
        {
            return lookup.Error;
        }

        var id = lookup.Value.Id;
        var snapshot = _catalog.GetDefaults(id);
        if (snapshot is null)
        {
            return AppError.Unavailable(
                "The defaults for this provider could not be loaded.");
        }

        var byName = snapshot.Settings
            .ToDictionary(s => s.Name, StringComparer.Ordinal);
        var effective = snapshot.Settings
            .ToDictionary(s => s.Name, s => s.Value, StringComparer.Ordinal);

        var overrides = query.Overrides
            ?? new Dictionary<string, string>(StringComparer.Ordinal);
        var rejected = ApplyOverrides(overrides, byName, effective);
        if (rejected.Count > 0)
        {
            return Result<ValidateSettingsResponse>.Success(
                new ValidateSettingsResponse(false, rejected));
        }

        var errors = _catalog.Validate(id, effective)
            .Select(e => ToDto(e, byName))
            .ToList();

        return Result<ValidateSettingsResponse>.Success(
            new ValidateSettingsResponse(errors.Count == 0, errors));
    }

    private static List<SettingErrorDto> ApplyOverrides(
        IReadOnlyDictionary<string, string> overrides,
        IReadOnlyDictionary<string, SettingEntry> byName,
        Dictionary<string, string> effective)
    {
        var rejected = new List<SettingErrorDto>();
        foreach (var (name, value) in overrides)
        {
            if (!byName.TryGetValue(name, out var entry))
            {
                rejected.Add(new SettingErrorDto(name, "The setting does not exist.", null));
            }
            else if (entry.IsSecret)
            {
                rejected.Add(new SettingErrorDto(
                    name, "Secret settings cannot be overridden.", null));
            }
            else if (!entry.Editable)
            {
                rejected.Add(new SettingErrorDto(
                    name, "The setting is read-only.", entry.PermittedValuesOrRange));
            }
            else
            {
                effective[name] = value;
            }
        }

        return rejected;
    }

    // Prefer the validator's permitted values; fall back to the setting's own.
    private static SettingErrorDto ToDto(
        SettingError error, IReadOnlyDictionary<string, SettingEntry> byName) =>
        new(
            error.Field,
            error.Message,
            error.PermittedValuesOrRange
                ?? (byName.TryGetValue(error.Field, out var entry)
                    ? entry.PermittedValuesOrRange
                    : null));
}
