using System.Diagnostics;

namespace Demo.Architecture.Tests;

/// <summary>
/// The demo is additive: the library sources, tests, solution, SDK pin and root compose file are
/// untouched, and any new file lives under <c>demo/</c> (Req 1.3, 1.4). Changes inside
/// <c>demo/</c> (and the <c>.kiro/</c> spec folder) are tolerated.
/// </summary>
public sealed class RepositoryHygieneTests
{
    private static readonly string[] ProtectedPaths =
    {
        "src", "tests", "MessagingCore.sln", "global.json", "docker-compose.yml",
    };

    [Fact]
    public void ProtectedPaths_HaveNoModificationsOrNewFiles()
    {
        var args = new List<string> { "status", "--porcelain", "--untracked-files=all", "--" };
        args.AddRange(ProtectedPaths);

        var output = Git(args.ToArray());

        Assert.True(string.IsNullOrWhiteSpace(output), "Protected paths changed:\n" + output);
    }

    [Fact]
    public void NewFiles_AreOnlyUnderDemo()
    {
        var output = Git("status", "--porcelain", "--untracked-files=all");

        var newOutsideDemo = output
            .Split('\n', StringSplitOptions.RemoveEmptyEntries)
            .Where(l => l.StartsWith("??", StringComparison.Ordinal) || l.StartsWith("A", StringComparison.Ordinal))
            .Select(l => l[3..].Trim().Trim('"'))
            .Where(p => !p.StartsWith("demo/", StringComparison.Ordinal) && !p.StartsWith(".kiro/", StringComparison.Ordinal))
            .ToArray();

        Assert.True(newOutsideDemo.Length == 0, "New files outside demo/:\n" + string.Join('\n', newOutsideDemo));
    }

    private static string Git(params string[] args)
    {
        var info = new ProcessStartInfo("git")
        {
            WorkingDirectory = RepoPaths.Root,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };
        foreach (var arg in args)
        {
            info.ArgumentList.Add(arg);
        }

        using var process = Process.Start(info) ?? throw new InvalidOperationException("git did not start.");
        var stdout = process.StandardOutput.ReadToEndAsync();
        var stderr = process.StandardError.ReadToEndAsync();
        if (!process.WaitForExit(30_000))
        {
            process.Kill(true);
            throw new TimeoutException("git timed out.");
        }

        Assert.True(process.ExitCode == 0, "git failed: " + stderr.Result);
        return stdout.Result;
    }
}
