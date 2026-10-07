using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace Messaging.Persistence.Idempotency;

/// <summary>
/// EF mapping for <see cref="IdempotencyRecord"/> onto the <c>messaging_idempotency</c> table
/// (Req 7). The primary key on <c>idempotency_key</c> provides the single indexed lookup for the
/// duplicate check and atomic record-on-success (Req 7.1, 7.2, 7.4); <c>expires_at_utc</c> backs
/// the retention sweep (Req 7.3).
/// </summary>
public sealed class IdempotencyRecordConfiguration : IEntityTypeConfiguration<IdempotencyRecord>
{
    /// <summary>Table name for the idempotency store (design data model).</summary>
    public const string TableName = "messaging_idempotency";

    /// <inheritdoc />
    public void Configure(EntityTypeBuilder<IdempotencyRecord> builder)
    {
        builder.ToTable(TableName);

        // PK on idempotency_key: single indexed lookup + atomic record-on-success (Req 7.1, 7.4).
        builder.HasKey(e => e.IdempotencyKey);

        builder.Property(e => e.IdempotencyKey)
            .HasColumnName("idempotency_key")
            .HasColumnType("varchar(256)")
            .IsRequired();

        builder.Property(e => e.CorrelationId)
            .HasColumnName("correlation_id")
            .HasColumnType("varchar(128)")
            .IsRequired();

        builder.Property(e => e.ProcessedAtUtc)
            .HasColumnName("processed_at_utc")
            .HasColumnType("timestamptz")
            .IsRequired();

        builder.Property(e => e.ExpiresAtUtc)
            .HasColumnName("expires_at_utc")
            .HasColumnType("timestamptz")
            .IsRequired();
    }
}
