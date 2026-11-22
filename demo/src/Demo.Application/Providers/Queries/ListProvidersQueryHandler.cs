using Demo.Application.Common;
using Demo.Application.Cqrs;

namespace Demo.Application.Providers.Queries;

/// <summary>
/// Lists the providers in the fixed display order: Amazon SNS/SQS,
/// RabbitMQ, Kafka, Azure Service Bus. Unsupported providers carry a
/// reason of 1 to 200 characters.
/// </summary>
public sealed class ListProvidersQueryHandler
    : IQueryHandler<ListProvidersQuery, IReadOnlyList<ProviderDto>>
{
    private static readonly ProviderId[] DisplayOrder =
    {
        ProviderId.AmazonSnsSqs,
        ProviderId.RabbitMq,
        ProviderId.Kafka,
        ProviderId.AzureServiceBus
    };

    private readonly IProviderCatalog _catalog;

    public ListProvidersQueryHandler(IProviderCatalog catalog)
    {
        _catalog = catalog ?? throw new ArgumentNullException(nameof(catalog));
    }

    public Task<Result<IReadOnlyList<ProviderDto>>> HandleAsync(
        ListProvidersQuery query, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        // OrderBy is stable, so ids outside the known order keep catalog order.
        IReadOnlyList<ProviderDto> providers = _catalog.List()
            .OrderBy(d => RankOf(d.Id))
            .Select(ToDto)
            .ToList();

        return Task.FromResult(
            Result<IReadOnlyList<ProviderDto>>.Success(providers));
    }

    private static int RankOf(ProviderId id)
    {
        var rank = Array.IndexOf(DisplayOrder, id);
        return rank < 0 ? DisplayOrder.Length : rank;
    }

    private static ProviderDto ToDto(ProviderDescriptor descriptor) =>
        descriptor.Supported
            ? new ProviderDto(
                descriptor.Id.Value, descriptor.DisplayName, true, null)
            : new ProviderDto(
                descriptor.Id.Value,
                descriptor.DisplayName,
                false,
                UnsupportedReason.Normalize(descriptor.UnsupportedReason));
}
