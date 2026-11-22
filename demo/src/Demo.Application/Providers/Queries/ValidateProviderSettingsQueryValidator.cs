using FluentValidation;

namespace Demo.Application.Providers.Queries;

/// <summary>Bounds the request shape. Setting semantics are checked by the library validators.</summary>
public sealed class ValidateProviderSettingsQueryValidator
    : AbstractValidator<ValidateProviderSettingsQuery>
{
    public const int MaxOverrides = 100;
    public const int MaxNameLength = 128;
    public const int MaxValueLength = 1024;

    public ValidateProviderSettingsQueryValidator()
    {
        RuleFor(q => q.ProviderId)
            .NotEmpty()
            .MaximumLength(GetProviderDefaultsQueryValidator.MaxProviderIdLength)
            .OverridePropertyName("providerId");

        RuleFor(q => q.Overrides)
            .NotNull()
            .OverridePropertyName("overrides");

        When(q => q.Overrides is not null, () =>
        {
            RuleFor(q => q.Overrides!)
                .Must(o => o.Count <= MaxOverrides)
                .OverridePropertyName("overrides")
                .WithMessage($"At most {MaxOverrides} overrides are allowed.");

            RuleFor(q => q.Overrides!)
                .Must(o => o.Keys.All(IsValidName))
                .OverridePropertyName("overrides")
                .WithMessage($"Setting names must be 1 to {MaxNameLength} characters.");

            RuleFor(q => q.Overrides!)
                .Must(o => o.Values.All(IsValidValue))
                .OverridePropertyName("overrides")
                .WithMessage($"Setting values must be at most {MaxValueLength} characters.");
        });
    }

    private static bool IsValidName(string name) =>
        !string.IsNullOrWhiteSpace(name) && name.Length <= MaxNameLength;

    private static bool IsValidValue(string? value) =>
        value is not null && value.Length <= MaxValueLength;
}
