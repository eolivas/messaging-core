using System.Text.RegularExpressions;

namespace Demo.Architecture.Tests;

/// <summary>Compose files pin every image to an explicit tag and never use <c>latest</c> (Req 13.4).</summary>
public sealed class ComposeImagePinningTests
{
    private static readonly Regex ImageLine = new(@"^\s*image:\s*(?<image>\S+)\s*(#.*)?$", RegexOptions.Compiled);

    public static IEnumerable<object[]> ComposeFiles()
    {
        yield return new object[] { "docker-compose.yml" };
        yield return new object[] { Path.Combine("demo", "docker-compose.yml") };
    }

    [Theory]
    [MemberData(nameof(ComposeFiles))]
    public void ComposeFile_PinsEveryImageAndNeverUsesLatest(string relativePath)
    {
        var lines = File.ReadAllLines(Path.Combine(RepoPaths.Root, relativePath));
        var images = lines
            .Select(l => ImageLine.Match(l))
            .Where(m => m.Success)
            .Select(m => m.Groups["image"].Value.Trim('"', '\''))
            .ToArray();

        Assert.NotEmpty(images);
        foreach (var image in images)
        {
            Assert.False(HasNoTag(image), $"Image '{image}' in {relativePath} has no tag.");
            Assert.DoesNotContain("latest", image, StringComparison.OrdinalIgnoreCase);
        }
    }

    [Fact]
    public void DemoDirectory_ComposeFilesAreAllChecked()
    {
        var found = Directory
            .EnumerateFiles(RepoPaths.Demo, "*compose*.y*ml", SearchOption.AllDirectories)
            .Where(f => !f.Contains($"{Path.DirectorySeparatorChar}node_modules{Path.DirectorySeparatorChar}"))
            .Select(f => Path.GetRelativePath(RepoPaths.Root, f))
            .ToArray();

        var checkedFiles = ComposeFiles().Select(o => (string)o[0]).ToHashSet();
        Assert.All(found, f => Assert.Contains(f, checkedFiles));
    }

    // A tag is the part after the last ':' that follows the last '/'. "${VAR:-1.2}" counts as tagged.
    private static bool HasNoTag(string image)
    {
        var name = image[(image.LastIndexOf('/') + 1)..];
        return !name.Contains(':');
    }
}
