namespace Demo.Application.Common;

/// <summary>An error tied to one input field.</summary>
public sealed record FieldError(string Field, IReadOnlyList<string> Messages);
