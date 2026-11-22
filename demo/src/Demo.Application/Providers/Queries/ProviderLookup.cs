using Demo.Application.Common;

namespace Demo.Application.Providers.Queries;

/// <summary>
/// Resolves a route provider id to a supported descriptor.
/// Unknown ids return NotFound; unsupported providers return a validation
/// error that carries the unavailability reason.
/// </summary>
internal static class ProviderLookup
{
    public static Result<ProviderDescriptor> Find(
        IProviderCatalog catalog, string? providerId)
    {
        var id = new ProviderId(providerId ?? string.Empty);
        var descriptor = catalog.List().FirstOrDefault(d => d.Id == id);
        if (descriptor is null)
        {
            return AppError.NotFound("The provider does not exist.");
        }

        if (!descriptor.Supported)
        {
            var reason = UnsupportedReason.Normalize(descriptor.UnsupportedReason);
            return AppError.Validation(
                reason,
                new[] { new FieldError("providerId", new[] { reason }) });
        }

        return Result<ProviderDescriptor>.Success(descriptor);
    }
}
