package com.nereusstream.delay.store;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RocksDbUsageLimitsTest {
    @TempDir
    Path tempDir;

    @Test
    void validatesPerDbAndWorkerPhysicalTotals() {
        final RocksDbUsageLimits limits = limits(30, 50, 4, 8, 30, 50, 4, 8, 30, 50, 4, 8, 200, 1, 30, 50, 4);
        final RocksDbUsageSnapshot first = snapshot(10, 10, 10, 40, 10, 2, 1, 1, 5, 1);
        final RocksDbUsageSnapshot second = snapshot(10, 10, 10, 40, 10, 2, 1, 1, 5, 1);
        assertDoesNotThrow(() -> limits.validate(List.of(first, second), tempDir));

        assertThrows(
                IllegalArgumentException.class,
                () -> limits.validate(List.of(snapshot(31, 1, 1, 1, 1, 1, 1, 1, 1, 1)), tempDir));
        assertThrows(
                IllegalArgumentException.class,
                () -> limits.validate(
                        List.of(snapshot(30, 30, 1, 1, 1, 2, 1, 1, 1, 1), snapshot(30, 30, 1, 1, 1, 2, 1, 1, 1, 1)),
                        tempDir));
    }

    @Test
    void rejectsDuplicateShardObservations() {
        final RocksDbUsageSnapshot value = snapshot(1, 1, 1, 1, 1, 1, 1, 1, 1, 1);
        final RocksDbUsageSnapshot sameShardWithDifferentCounters =
                new RocksDbUsageSnapshot(value.shardId(), 2, 1, 1, 1, 1, 1, 1, 1, 1, 1);
        final RocksDbUsageLimits limits = limits(10, 20, 2, 4, 10, 20, 2, 4, 10, 20, 2, 4, 20, 1, 10, 20, 2);
        assertThrows(
                IllegalArgumentException.class,
                () -> limits.validate(List.of(value, sameShardWithDifferentCounters), tempDir));
    }

    private static RocksDbUsageSnapshot snapshot(
            final long walBytes,
            final long manifestBytes,
            final long sstBytes,
            final long localBytes,
            final long pendingBytes,
            final int walFiles,
            final int manifestFiles,
            final int sstFiles,
            final int localFiles,
            final int l0Files) {
        return new RocksDbUsageSnapshot(
                new ShardId(RouteIncarnation.random(), localFiles),
                sstBytes,
                walBytes,
                manifestBytes,
                localBytes,
                pendingBytes,
                sstFiles,
                walFiles,
                manifestFiles,
                localFiles,
                l0Files);
    }

    private static RocksDbUsageLimits limits(
            final long maxWalPerDb,
            final long maxWalTotal,
            final int maxWalFilesPerDb,
            final int maxWalFilesTotal,
            final long maxManifestPerDb,
            final long maxManifestTotal,
            final int maxManifestFilesPerDb,
            final int maxManifestFilesTotal,
            final long maxSstPerDb,
            final long maxSstTotal,
            final int maxSstFilesPerDb,
            final int maxSstFilesTotal,
            final long maxLocal,
            final long minFree,
            final long maxPendingPerDb,
            final long maxPendingTotal,
            final int maxL0) {
        return new RocksDbUsageLimits(
                maxWalPerDb,
                maxWalTotal,
                maxWalFilesPerDb,
                maxWalFilesTotal,
                maxManifestPerDb,
                maxManifestTotal,
                maxManifestFilesPerDb,
                maxManifestFilesTotal,
                maxSstPerDb,
                maxSstTotal,
                maxSstFilesPerDb,
                maxSstFilesTotal,
                maxLocal,
                minFree,
                maxPendingPerDb,
                maxPendingTotal,
                maxL0);
    }
}
