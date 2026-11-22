using System.Text.RegularExpressions;
using Demo.Application.Cqrs;

namespace Demo.Architecture.Tests;

/// <summary>Every command and query handler type has a matching <c>&lt;Handler&gt;Tests</c> class (Req 14.1).</summary>
public sealed class HandlerTestCoverageTests
{
    [Fact]
    public void EveryCommandAndQueryHandler_HasATestClass()
    {
        var handlers = typeof(ICommandHandler<,>).Assembly
            .GetTypes()
            .Where(t => t is { IsClass: true, IsAbstract: false } && ImplementsCqrsHandler(t))
            .Select(t => t.Name)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();

        Assert.NotEmpty(handlers);

        var declaredTestClasses = DeclaredTypeNamesInTestSources();
        var missing = handlers.Where(h => !declaredTestClasses.Contains(h + "Tests")).ToArray();

        Assert.True(missing.Length == 0, "Handlers without a <Handler>Tests class: " + string.Join(", ", missing));
    }

    private static bool ImplementsCqrsHandler(Type type) =>
        type.GetInterfaces().Any(i =>
            i.IsGenericType &&
            (i.GetGenericTypeDefinition() == typeof(ICommandHandler<,>) ||
             i.GetGenericTypeDefinition() == typeof(IQueryHandler<,>)));

    private static HashSet<string> DeclaredTypeNamesInTestSources()
    {
        var pattern = new Regex(@"\b(?:class|record)\s+(\w+)", RegexOptions.Compiled);
        var testsRoot = Path.Combine(RepoPaths.Demo, "tests");
        var names = new HashSet<string>(StringComparer.Ordinal);
        foreach (var file in Directory.EnumerateFiles(testsRoot, "*.cs", SearchOption.AllDirectories))
        {
            var sep = Path.DirectorySeparatorChar;
            if (file.Contains($"{sep}obj{sep}") || file.Contains($"{sep}bin{sep}"))
            {
                continue;
            }

            foreach (Match m in pattern.Matches(File.ReadAllText(file)))
            {
                names.Add(m.Groups[1].Value);
            }
        }

        return names;
    }
}
