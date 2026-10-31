package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.store.BoundedReadBudget;
import java.nio.file.Path;
import java.util.Objects;
import java.util.function.LongSupplier;

/** A finite maintenance request. Configuration grants neither writer closure nor Source activation. */
public record TargetMigrationConfiguration(
        TargetQuotaScope scope,
        byte[] migrationId,
        Path legacyCheckpoint,
        Path candidateDirectory,
        long maximumRetainedBytes,
        int maximumRecordsPerTurn,
        long maximumBytesPerTurn,
        long maximumElapsedNanos,
        long taskBytes,
        int maximumBackends) {
    public TargetMigrationConfiguration {
        Objects.requireNonNull(scope);
        Objects.requireNonNull(legacyCheckpoint);
        Objects.requireNonNull(candidateDirectory);
        Bytes.requireLength(migrationId, 32, "configured migration identity");
        migrationId = Bytes.copy(migrationId);
        legacyCheckpoint = legacyCheckpoint.toAbsolutePath().normalize();
        candidateDirectory = candidateDirectory.toAbsolutePath().normalize();
        if (scope.target() != null
                || maximumRetainedBytes < 1
                || maximumRetainedBytes == Long.MAX_VALUE
                || maximumRecordsPerTurn < 1
                || maximumRecordsPerTurn == Integer.MAX_VALUE
                || maximumBytesPerTurn < 1
                || maximumBytesPerTurn == Long.MAX_VALUE
                || maximumElapsedNanos < 1
                || maximumElapsedNanos == Long.MAX_VALUE
                || taskBytes < 1
                || taskBytes == Long.MAX_VALUE
                || taskBytes > maximumRetainedBytes
                || maximumBackends < 1
                || maximumBackends > 16
                || legacyCheckpoint.startsWith(candidateDirectory)
                || candidateDirectory.startsWith(legacyCheckpoint)) {
            throw new IllegalArgumentException(
                    "migration requires disjoint original/candidate paths and finite owned resources");
        }
    }

    @Override
    public byte[] migrationId() {
        return Bytes.copy(migrationId);
    }

    public BoundedReadBudget newReadBudget(LongSupplier nanos) {
        return new BoundedReadBudget(maximumRecordsPerTurn, maximumBytesPerTurn, maximumElapsedNanos, nanos);
    }

    public static TargetMigrationConfiguration load(Path file) throws java.io.IOException {
        final var f = TargetWorkerRuntimeConfigLoader.loadFields(file);
        final var value = new TargetMigrationConfiguration(
                TargetQuotaScope.decode(f.image("migration.scopeFile", TargetQuotaScope.MAX_CANONICAL_BYTES)),
                f.hex("migration.id", 32, 32),
                f.absolutePath("migration.legacyCheckpoint"),
                f.absolutePath("migration.candidateDirectory"),
                f.number("migration.maximumRetainedBytes"),
                f.integer("migration.maximumRecordsPerTurn"),
                f.number("migration.maximumBytesPerTurn"),
                f.number("migration.maximumElapsedNanos"),
                f.number("migration.taskBytes"),
                f.integer("migration.maximumBackends"));
        f.requireComplete();
        return value;
    }
}
