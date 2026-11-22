namespace Demo.Application;

/// <summary>Masks secret setting values. The output never depends on the input.</summary>
public static class SecretMasker
{
    public const string Placeholder = "••••••";

    public static string Mask(string? value) => Placeholder;
}
