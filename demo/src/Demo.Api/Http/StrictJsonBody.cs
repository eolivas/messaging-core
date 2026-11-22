using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization;
using Demo.Application.Common;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.Extensions.Options;
using HttpJsonOptions = Microsoft.AspNetCore.Http.Json.JsonOptions;

namespace Demo.Api.Http;

/// <summary>
/// Minimal API body binder that rejects unknown fields (Req 12.8). The body is read once, capped at
/// <see cref="MaxBodyBytes"/> (413 beyond it), compared with the DTO's declared property names, then
/// deserialized. Binding failures throw <see cref="InvalidRequestException"/>, which the exception
/// handler maps to 400, so an endpoint cannot forget to check. Only top-level fields are compared;
/// the request DTOs are flat (a dictionary's keys are data, not fields).
/// </summary>
public sealed class StrictJsonBody<T>
    where T : class
{
    public const int MaxBodyBytes = 1024 * 1024;
    private const string BodyField = "body";

    private StrictJsonBody(T value)
    {
        Value = value;
    }

    public T Value { get; }

    public static async ValueTask<StrictJsonBody<T>?> BindAsync(HttpContext context, ParameterInfo parameter)
    {
        ArgumentNullException.ThrowIfNull(context);

        var bytes = await ReadBodyAsync(context).ConfigureAwait(false);
        if (bytes.Length == 0)
        {
            throw Invalid(BodyField, "A JSON body is required.");
        }

        JsonDocument document;
        try
        {
            document = JsonDocument.Parse(bytes);
        }
        catch (JsonException)
        {
            throw Invalid(BodyField, "The body is not valid JSON.");
        }

        using (document)
        {
            if (document.RootElement.ValueKind != JsonValueKind.Object)
            {
                throw Invalid(BodyField, "The body must be a JSON object.");
            }

            var declared = DeclaredNames();
            var unknown = document.RootElement.EnumerateObject()
                .Select(p => p.Name)
                .Where(n => !declared.Contains(n))
                .Distinct(StringComparer.Ordinal)
                .Select(n => new FieldError(n, ["Unknown field."]))
                .ToList();
            if (unknown.Count > 0)
            {
                throw new InvalidRequestException(unknown);
            }

            var options = context.RequestServices.GetRequiredService<IOptions<HttpJsonOptions>>().Value.SerializerOptions;
            try
            {
                var value = document.RootElement.Deserialize<T>(options)
                    ?? throw Invalid(BodyField, "A JSON body is required.");
                return new StrictJsonBody<T>(value);
            }
            catch (JsonException ex)
            {
                var path = (ex.Path ?? string.Empty).TrimStart('$', '.');
                throw Invalid(path.Length == 0 ? BodyField : path, "The value has the wrong type or format.");
            }
        }
    }

    private static async Task<byte[]> ReadBodyAsync(HttpContext context)
    {
        var request = context.Request;
        var limit = context.Features.Get<IHttpMaxRequestBodySizeFeature>()?.MaxRequestBodySize ?? MaxBodyBytes;
        limit = Math.Min(limit, MaxBodyBytes);
        if (request.ContentLength > limit)
        {
            throw new BadHttpRequestException("The request body is too large.", StatusCodes.Status413PayloadTooLarge);
        }

        using var buffer = new MemoryStream();
        var chunk = new byte[8192];
        int read;
        while ((read = await request.Body.ReadAsync(chunk, context.RequestAborted).ConfigureAwait(false)) > 0)
        {
            if (buffer.Length + read > limit)
            {
                throw new BadHttpRequestException("The request body is too large.", StatusCodes.Status413PayloadTooLarge);
            }

            buffer.Write(chunk, 0, read);
        }

        return buffer.ToArray();
    }

    private static HashSet<string> DeclaredNames() =>
        typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.GetCustomAttribute<JsonIgnoreAttribute>() is null)
            .Select(p => p.GetCustomAttribute<JsonPropertyNameAttribute>()?.Name
                ?? JsonNamingPolicy.CamelCase.ConvertName(p.Name))
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

    private static InvalidRequestException Invalid(string field, string message) =>
        new([new FieldError(field, [message])]);
}
