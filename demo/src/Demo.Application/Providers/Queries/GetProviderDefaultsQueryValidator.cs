using FluentValidation;

namespace Demo.Application.Providers.Queries;

public sealed class GetProviderDefaultsQueryValidator
    : AbstractValidator<GetProviderDefaultsQuery>
{
    public const int MaxProviderIdLength = 64;

    public GetProviderDefaultsQueryValidator()
    {
        RuleFor(q => q.ProviderId)
            .NotEmpty()
            .MaximumLength(MaxProviderIdLength)
            .OverridePropertyName("providerId");
    }
}
