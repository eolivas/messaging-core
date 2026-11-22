using FluentValidation;

namespace Demo.Application.Providers.Queries;

/// <summary>The list query has no input; the validator exists so every request has one.</summary>
public sealed class ListProvidersQueryValidator : AbstractValidator<ListProvidersQuery>
{
}
