using Demo.Api.Configuration;
using Microsoft.AspNetCore.Cors.Infrastructure;
using Microsoft.Extensions.Options;

namespace Demo.Api.Http;

/// <summary>Registers the cross-cutting HTTP behavior of the BFF.</summary>
public static class ApiServiceCollectionExtensions
{
    public const string CorsPolicyName = "DemoFrontend";
    public const long MaxRequestBodyBytes = StrictJsonBody<object>.MaxBodyBytes;

    public static IServiceCollection AddDemoHttp(this IServiceCollection services, IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configuration);

        services.AddSingleton<IValidateOptions<DemoOptions>, DemoOptionsValidator>();
        services.AddOptions<DemoOptions>().Bind(configuration.GetSection(DemoOptions.SectionName));

        services.ConfigureHttpJsonOptions(o =>
            o.SerializerOptions.Converters.Add(new System.Text.Json.Serialization.JsonStringEnumConverter()));

        services.AddProblemDetails(o => o.CustomizeProblemDetails = context =>
        {
            context.ProblemDetails.Extensions["correlationId"] = CorrelationIdMiddleware.Current(context.HttpContext);
        });
        services.AddExceptionHandler<ApiExceptionHandler>();

        // The origin is read lazily, after DemoOptions validation has passed at startup.
        services.AddOptions<CorsOptions>().Configure<IOptions<DemoOptions>>((cors, demo) =>
            cors.AddPolicy(CorsPolicyName, policy => policy
                .WithOrigins(demo.Value.FrontendOrigin!.TrimEnd('/'))
                .AllowAnyHeader()
                .AllowAnyMethod()
                .WithExposedHeaders(CorrelationIdMiddleware.HeaderName)));
        services.AddCors();

        return services;
    }
}
