using Demo.Application.Providers.Queries;
using FluentValidation;

namespace Demo.Application.Sessions.Commands;

/// <summary>Bounds the request shape. Setting semantics are checked by the library validators.</summary>
public sealed class StartSessionCommandValidator : AbstractValidator<StartSessionCommand>
{
    public StartSessionCommandValidator()
    {
        RuleFor(c => c.ProviderId)
            .NotEmpty()
            .MaximumLength(GetProviderDefaultsQueryValidator.MaxProviderIdLength)
            .OverridePropertyName("providerId");

        When(c => c.Overrides is not null, () =>
        {
            RuleFor(c => c.Overrides!)
                .Must(o => o.Count <= ValidateProviderSettingsQueryValidator.MaxOverrides)
                .OverridePropertyName("overrides")
                .WithMessage($"At most {ValidateProviderSettingsQueryValidator.MaxOverrides} overrides are allowed.");
            RuleFor(c => c.Overrides!)
                .Must(o => o.Keys.All(k => !string.IsNullOrWhiteSpace(k)
                    && k.Length <= ValidateProviderSettingsQueryValidator.MaxNameLength))
                .OverridePropertyName("overrides")
                .WithMessage("Setting names must be 1 to 128 characters.");
            RuleFor(c => c.Overrides!)
                .Must(o => o.Values.All(v => v is not null
                    && v.Length <= ValidateProviderSettingsQueryValidator.MaxValueLength))
                .OverridePropertyName("overrides")
                .WithMessage("Setting values must be at most 1024 characters.");
        });
    }
}
