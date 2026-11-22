using Microsoft.Extensions.Options;

namespace Demo.Api.Configuration;

/// <summary>
/// Collects every missing or empty setting by name. Failure messages carry names only, never values,
/// because the values are credentials or connection strings.
/// </summary>
public sealed class DemoOptionsValidator : IValidateOptions<DemoOptions>
{
    public ValidateOptionsResult Validate(string? name, DemoOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        var failures = new List<string>();
        Require(failures, "Demo:FrontendOrigin", options.FrontendOrigin);
        Require(failures, "Demo:RabbitMq:ConnectionString", options.RabbitMq.ConnectionString);
        Require(failures, "Demo:Kafka:BootstrapServers", options.Kafka.BootstrapServers);
        Require(failures, "Demo:Kafka:Topic", options.Kafka.Topic);
        Require(failures, "Demo:AmazonSqs:Region", options.AmazonSqs.Region);
        Require(failures, "Demo:AmazonSqs:ServiceUrl", options.AmazonSqs.ServiceUrl);
        Require(failures, "Demo:AmazonSqs:AccessKeyId", options.AmazonSqs.AccessKeyId);
        Require(failures, "Demo:AmazonSqs:SecretAccessKey", options.AmazonSqs.SecretAccessKey);

        if (!string.IsNullOrWhiteSpace(options.FrontendOrigin) && !IsOrigin(options.FrontendOrigin))
        {
            failures.Add("Demo:FrontendOrigin must be an absolute http or https origin without a path.");
        }

        return failures.Count == 0 ? ValidateOptionsResult.Success : ValidateOptionsResult.Fail(failures);
    }

    private static void Require(List<string> failures, string settingName, string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            failures.Add($"{settingName} is missing or empty.");
        }
    }

    private static bool IsOrigin(string value) =>
        Uri.TryCreate(value, UriKind.Absolute, out var uri)
        && (uri.Scheme == Uri.UriSchemeHttp || uri.Scheme == Uri.UriSchemeHttps)
        && (uri.AbsolutePath == "/");
}
