using Demo.Application.Common;
using Demo.Application.Cqrs;

namespace Demo.Application.Providers.Queries;

/// <summary>Returns provider defaults with every secret value masked.</summary>
public sealed class GetProviderDefaultsQueryHandler
    : IQueryHandler<GetProviderDefaultsQuery, ProviderDefaultsDto>
{
    private readonly IProviderCatalog _catalog;

    public GetProviderDefaultsQueryHandler(IProviderCatalog catalog)
    {
        _catalog = catalog ?? throw new ArgumentNullException(nameof(catalog));
    }

    public Task<Result<ProviderDefaultsDto>> HandleAsync(
        GetProviderDefaultsQuery query, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(query);
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(Handle(query));
    }

    private Result<ProviderDefaultsDto> Handle(GetProviderDefaultsQuery query)
    {
        var lookup = ProviderLookup.Find(_catalog, query.ProviderId);
        if (lookup.IsFailure)
        {
            return lookup.Error;
        }

        var id = lookup.Value.Id;

        // Null means the catalog could not build the defaults. Return an
        // error and no settings, so the caller never sees partial pairs.
        var snapshot = _catalog.GetDefaults(id);
        if (snapshot is null)
        {
            return AppError.Unavailable(
                "The defaults for this provider could not be loaded.");
        }

        var settings = snapshot.Settings
            .Select(ToDto)
            .ToList();

        return Result<ProviderDefaultsDto>.Success(
            new ProviderDefaultsDto(id.Value, snapshot.MessagingMode, settings));
    }

    private static SettingEntryDto ToDto(SettingEntry entry) => new(
        entry.Group,
        entry.Name,
        entry.IsSecret ? SecretMasker.Mask(entry.Value) : entry.Value,
        entry.IsSecret,
        entry.Editable,
        entry.PermittedValuesOrRange);
}
