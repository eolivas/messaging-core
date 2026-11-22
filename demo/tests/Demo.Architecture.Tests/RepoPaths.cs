namespace Demo.Architecture.Tests;

/// <summary>Locates the repository root and the demo folder from the test output directory.</summary>
internal static class RepoPaths
{
    public static string Root { get; } = FindRoot();

    public static string Demo => Path.Combine(Root, "demo");

    private static string FindRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null && !File.Exists(Path.Combine(dir.FullName, "MessagingCore.sln")))
        {
            dir = dir.Parent;
        }

        return dir?.FullName
            ?? throw new InvalidOperationException("MessagingCore.sln not found above the test output.");
    }
}
