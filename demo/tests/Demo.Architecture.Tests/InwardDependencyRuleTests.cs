using System.Reflection;
using System.Xml.Linq;
using Demo.Application.Cqrs;
using Demo.Communications;
using Demo.Contracts;
using Demo.Customer.Application;
using Demo.Infrastructure.Hosting;

namespace Demo.Architecture.Tests;

/// <summary>
/// Enforces inward dependencies between the demo projects, microservice isolation and the
/// Communications package restrictions (Req 1.1, 1.2, 5.8, 8.5, 12.2). Assembly metadata is
/// inspected so a forbidden reference is caught regardless of how it was introduced.
/// </summary>
public sealed class InwardDependencyRuleTests
{
    private const string ContractsName = "Demo.Contracts";
    private const string SalesLeadsName = "Demo.SalesLeads";
    private const string CustomerName = "Demo.Customer";
    private const string CommunicationsName = "Demo.Communications";
    private const string ApplicationName = "Demo.Application";
    private const string InfrastructureName = "Demo.Infrastructure";
    private const string ApiName = "Demo.Api";

    private static readonly string[] MicroserviceNames = { SalesLeadsName, CustomerName, CommunicationsName };

    private static Assembly ContractsAssembly => typeof(IFlowStepSink).Assembly;

    private static Assembly ApplicationAssembly => typeof(ICommandHandler<,>).Assembly;

    private static Assembly InfrastructureAssembly => typeof(MicroserviceHostFactory).Assembly;

    private static Assembly CustomerAssembly => typeof(CreateCustomerHandler).Assembly;

    private static Assembly CommunicationsAssembly => typeof(SendQuoteEmailHandler).Assembly;

    private static Assembly SalesLeadsAssembly =>
        AppDomain.CurrentDomain.GetAssemblies().FirstOrDefault(a => a.GetName().Name == SalesLeadsName)
        ?? Assembly.Load(SalesLeadsName);

    [Fact]
    public void Contracts_ReferencesNoOtherDemoProject()
    {
        Assert.Empty(DemoReferencesOf(ContractsAssembly));
    }

    [Fact]
    public void Microservices_ReferenceOnlyContractsAmongDemoProjects()
    {
        foreach (var assembly in new[] { SalesLeadsAssembly, CustomerAssembly, CommunicationsAssembly })
        {
            var offending = DemoReferencesOf(assembly).Where(n => n != ContractsName).ToArray();
            Assert.True(offending.Length == 0, $"{assembly.GetName().Name} references: {string.Join(", ", offending)}");
        }
    }

    [Fact]
    public void Microservices_DoNotReferenceEachOther()
    {
        foreach (var assembly in new[] { SalesLeadsAssembly, CustomerAssembly, CommunicationsAssembly })
        {
            var self = assembly.GetName().Name;
            var offending = ReferenceNames(assembly).Where(n => MicroserviceNames.Contains(n) && n != self).ToArray();
            Assert.True(offending.Length == 0, $"{self} references: {string.Join(", ", offending)}");
        }
    }

    [Fact]
    public void Microservices_DoNotReferenceAspNet()
    {
        foreach (var assembly in new[] { SalesLeadsAssembly, CustomerAssembly, CommunicationsAssembly })
        {
            var offending = ReferenceNames(assembly)
                .Where(n => n.StartsWith("Microsoft.AspNetCore", StringComparison.Ordinal))
                .ToArray();
            Assert.True(offending.Length == 0, $"{assembly.GetName().Name} references: {string.Join(", ", offending)}");
        }

        foreach (var project in MicroserviceNames)
        {
            var doc = LoadProject(project);
            Assert.DoesNotContain("Microsoft.NET.Sdk.Web", doc.Root!.Attribute("Sdk")?.Value ?? string.Empty);
            Assert.DoesNotContain(
                doc.Descendants("FrameworkReference"),
                e => ((string?)e.Attribute("Include") ?? string.Empty).Contains("AspNetCore"));
        }
    }

    [Fact]
    public void Application_DoesNotReferenceOuterLayersOrMicroservices()
    {
        var forbidden = new[] { InfrastructureName, ApiName }.Concat(MicroserviceNames).ToArray();
        var offending = ReferenceNames(ApplicationAssembly).Where(forbidden.Contains).ToArray();
        Assert.Empty(offending);
    }

    [Fact]
    public void Infrastructure_DoesNotReferenceApi()
    {
        Assert.DoesNotContain(ApiName, ReferenceNames(InfrastructureAssembly));
    }

    [Fact]
    public void Communications_HasNoHttpOrSmtpPackageOrAssembly()
    {
        string[] forbiddenTokens = { "MailKit", "MimeKit", "Smtp", "System.Net.Mail", "System.Net.Http", "RestSharp", "Refit" };

        var packages = LoadProject(CommunicationsName)
            .Descendants("PackageReference")
            .Select(e => (string?)e.Attribute("Include") ?? string.Empty);
        var offendingPackages = packages
            .Where(p => forbiddenTokens.Any(t => p.Contains(t, StringComparison.OrdinalIgnoreCase)))
            .ToArray();
        Assert.Empty(offendingPackages);

        var offendingAssemblies = ReferenceNames(CommunicationsAssembly)
            .Where(n => forbiddenTokens.Any(t => n.Contains(t, StringComparison.OrdinalIgnoreCase)))
            .ToArray();
        Assert.Empty(offendingAssemblies);
    }

    private static string[] ReferenceNames(Assembly assembly) =>
        assembly.GetReferencedAssemblies().Select(r => r.Name ?? string.Empty).ToArray();

    private static string[] DemoReferencesOf(Assembly assembly) =>
        ReferenceNames(assembly).Where(n => n.StartsWith("Demo.", StringComparison.Ordinal)).ToArray();

    private static XDocument LoadProject(string project) =>
        XDocument.Load(Path.Combine(RepoPaths.Demo, "src", project, project + ".csproj"));
}
