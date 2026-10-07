using System.Text.Json;
using Messaging.Abstractions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace Messaging.Persistence.Outbox;

/// <summary>
/// EF mapping for <see cref="OutboxEntry"/> onto the <c>messaging_outbox</c> table (Req 5).
/// Maps the columns and types from the design's data model: <c>id uuid</c> PK, jsonb headers,
/// bytea payload, smallint status, and the <c>(status, created_at_utc)</c> index used by the
/// dispatcher's pending scan (Req 5.2).
/// </summary>
public sealed class OutboxEntryConfiguration : IEntityTypeConfiguration<OutboxEntry>
{
    /// <summary>Table name for the outbox (design data model).</summary>
    public const string TableName = "messaging_outbox";

    /// <summary>Name of the index backing the dispatcher's pending scan (Req 5.2).</summary>
    public const string PendingScanIndexName = "ix_messaging_outbox_status_created_at_utc";

    /// <inheritdoc />
    public void Configure(EntityTypeBuilder<OutboxEntry> builder)
    {
        builder.ToTable(TableName);

        builder.HasKey(e => e.Id);

        builder.Property(e => e.Id)
            .HasColumnName("id")
            .HasColumnType("uuid");

        builder.Property(e => e.CorrelationId)
            .HasColumnName("correlation_id")
            .HasColumnType("varchar(128)")
            .IsRequired();

        builder.Property(e => e.MessageType)
            .HasColumnName("message_type")
            .HasColumnType("varchar(256)")
            .IsRequired();

        builder.Property(e => e.ContractVersion)
            .HasColumnName("contract_version")
            .HasColumnType("integer")
            .IsRequired()
            .HasDefaultValue(1);

        // Headers (0..64 entries) are stored as jsonb. The value converter serializes the
        // dictionary to/from JSON; a value comparer lets EF detect header changes correctly.
        builder.Property(e => e.Headers)
            .HasColumnName("headers")
            .HasColumnType("jsonb")
            .HasConversion(
                headers => Serialize(headers),
                json => Deserialize(json),
                new ValueComparer<IReadOnlyDictionary<string, string>>(
                    (left, right) => Equals(left, right),
                    dictionary => GetHashCode(dictionary),
                    dictionary => Clone(dictionary)))
            .IsRequired();

        builder.Property(e => e.Payload)
            .HasColumnName("payload")
            .HasColumnType("bytea")
            .IsRequired();

        // The OutboxStatus enum maps to smallint with its explicit 0/1/2 values; the conversion
        // preserves the persisted contract documented on the enum (do not reorder).
        builder.Property(e => e.Status)
            .HasColumnName("status")
            .HasColumnType("smallint")
            .HasConversion<short>()
            .IsRequired();

        builder.Property(e => e.AttemptCount)
            .HasColumnName("attempt_count")
            .HasColumnType("integer")
            .IsRequired()
            .HasDefaultValue(0);

        builder.Property(e => e.CreatedAtUtc)
            .HasColumnName("created_at_utc")
            .HasColumnType("timestamptz")
            .IsRequired();

        builder.Property(e => e.DispatchedAtUtc)
            .HasColumnName("dispatched_at_utc")
            .HasColumnType("timestamptz");

        builder.Property(e => e.LastError)
            .HasColumnName("last_error")
            .HasColumnType("text");

        // Index for the dispatcher's pending scan over (status, created_at_utc) (Req 5.2).
        builder.HasIndex(e => new { e.Status, e.CreatedAtUtc })
            .HasDatabaseName(PendingScanIndexName);
    }

    private static string Serialize(IReadOnlyDictionary<string, string> headers)
        => JsonSerializer.Serialize(headers);

    private static IReadOnlyDictionary<string, string> Deserialize(string json)
        => JsonSerializer.Deserialize<Dictionary<string, string>>(json)
           ?? new Dictionary<string, string>();

    private static bool Equals(
        IReadOnlyDictionary<string, string>? left,
        IReadOnlyDictionary<string, string>? right)
    {
        if (ReferenceEquals(left, right))
        {
            return true;
        }

        if (left is null || right is null || left.Count != right.Count)
        {
            return false;
        }

        foreach (var pair in left)
        {
            if (!right.TryGetValue(pair.Key, out var otherValue) || otherValue != pair.Value)
            {
                return false;
            }
        }

        return true;
    }

    private static int GetHashCode(IReadOnlyDictionary<string, string> headers)
    {
        var hash = new HashCode();
        foreach (var pair in headers)
        {
            hash.Add(pair.Key);
            hash.Add(pair.Value);
        }

        return hash.ToHashCode();
    }

    private static IReadOnlyDictionary<string, string> Clone(
        IReadOnlyDictionary<string, string> headers)
        => new Dictionary<string, string>(headers.ToDictionary(pair => pair.Key, pair => pair.Value));
}
