using Demo.Api.Configuration;
using Demo.Api.Endpoints;
using Demo.Api.Hosting;
using Demo.Api.Http;
using Demo.Application;
using Demo.Infrastructure.Composition;
using Microsoft.Extensions.Options;

var builder = WebApplication.CreateBuilder(args);

builder.WebHost.ConfigureKestrel(o => o.Limits.MaxRequestBodySize = ApiServiceCollectionExtensions.MaxRequestBodyBytes);

builder.Services.AddDemoHttp(builder.Configuration);
builder.Services.AddDemoApplication();
builder.Services.AddDemoInfrastructure();
builder.Services.AddHostedService<ProviderHealthMonitor>();

var app = builder.Build();

// Fail before any request is accepted: missing configuration (Req 13.9), then registrations (Req 11.2).
var startupLogger = app.Services.GetRequiredService<ILoggerFactory>().CreateLogger("Demo.Startup");
try
{
    _ = app.Services.GetRequiredService<IOptions<DemoOptions>>().Value;
}
catch (OptionsValidationException ex)
{
    // Failure messages name settings only, never values.
    startupLogger.LogCritical("Invalid configuration. {Failures}", string.Join(" ", ex.Failures));
    return 1;
}

try
{
    await app.Services.GetRequiredService<ProviderCompositionSelfCheck>().RunAsync();
}
catch (ProviderCompositionSelfCheckException)
{
    // Already logged by the self-check with the failed registration.
    return 1;
}

app.UseMiddleware<CorrelationIdMiddleware>();
app.UseExceptionHandler();
app.UseStatusCodePages();
app.UseCors(ApiServiceCollectionExtensions.CorsPolicyName);

var api = app.MapGroup("/api/v1");
api.MapProviderEndpoints();
api.MapSessionEndpoints();
api.MapEventStreamEndpoints();
app.MapHealthEndpoints();
app.Run();
return 0;

// Exposed for WebApplicationFactory in Demo.Api.Tests.
public partial class Program;
