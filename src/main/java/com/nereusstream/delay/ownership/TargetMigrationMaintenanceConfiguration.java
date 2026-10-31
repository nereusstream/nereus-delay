package com.nereusstream.delay.ownership;

import java.util.Objects;

/** Explicit bounded live input/SDK request; no management, deletion or closure permission is conveyed. */
public record TargetMigrationMaintenanceConfiguration(
        TargetMigrationConfiguration migration,
        long maximumRetainedBytes,
        int maximumResourcesPerTurn,
        int maximumCleanupOwners,
        long preparationTimeoutNanos) {
    public TargetMigrationMaintenanceConfiguration {
        Objects.requireNonNull(migration);
        if (maximumRetainedBytes < migration.taskBytes()
                || maximumRetainedBytes == Long.MAX_VALUE
                || maximumResourcesPerTurn < 1
                || maximumResourcesPerTurn > 256
                || maximumCleanupOwners < migration.maximumBackends()
                || maximumCleanupOwners > 64
                || preparationTimeoutNanos < 1
                || preparationTimeoutNanos == Long.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "live migration maintenance requires finite original input/time/owner bounds");
        }
    }

    public static TargetMigrationMaintenanceConfiguration load(java.nio.file.Path file) throws java.io.IOException {
        final var fields = TargetWorkerRuntimeConfigLoader.loadFields(file);
        final var original = TargetMigrationConfiguration.load(fields.absolutePath("maintenance.migrationFile"));
        final var value = new TargetMigrationMaintenanceConfiguration(
                original,
                fields.number("maintenance.maximumRetainedBytes"),
                fields.integer("maintenance.maximumResourcesPerTurn"),
                fields.integer("maintenance.maximumCleanupOwners"),
                fields.number("maintenance.preparationTimeoutNanos"));
        fields.requireComplete();
        return value;
    }
}
