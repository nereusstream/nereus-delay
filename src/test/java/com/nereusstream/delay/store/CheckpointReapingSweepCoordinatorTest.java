package com.nereusstream.delay.store;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.ownership.OwnerLease;
import com.nereusstream.delay.ownership.OwnerLeaseContext;
import com.nereusstream.delay.ownership.ShardLifecycleState;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CheckpointUploadIntent;
import com.nereusstream.delay.protocol.CheckpointUploadState;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardSubject;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class CheckpointReapingSweepCoordinatorTest {
    @Test
    void winsReapingCasBeforeSweepingTheExactPrefix() {
        final CheckpointUploadIntentStore store = new CheckpointUploadIntentStore();
        final CheckpointUploadIntent pending = pending();
        final CheckpointReapingOwnerProof ownerProof = ownerProof(pending);
        store.create(pending);
        final AtomicReference<CheckpointPrefixSweepRequest> observed = new AtomicReference<>();
        final CheckpointPrefixSweepAdapter adapter = request -> {
            assertEquals(
                    CheckpointUploadState.REAPING, store.current().orElseThrow().state());
            observed.set(request);
            return result(3, 3);
        };

        final CheckpointReapingSweepResult result = new CheckpointReapingSweepCoordinator(store, adapter)
                .reap(
                        pending,
                        new RecoveryCatalog(),
                        ownerProof,
                        quiescence(pending, ownerProof, evidence(5_000)),
                        100);

        assertEquals(CheckpointUploadState.REAPING, result.reapingIntent().state());
        assertEquals(3, result.prefixSweep().listedVersionCount());
        assertEquals(3, result.prefixSweep().deletedVersionCount());
        assertTrue(result.prefixSweep().emptyAfterSweep());
        assertEquals(pending.objectStoreProfile(), observed.get().objectStoreProfile());
        assertArrayEquals(pending.recoveryLineageId(), observed.get().recoveryLineageId());
        assertArrayEquals(pending.checkpointId(), observed.get().checkpointId());
        assertEquals(100, observed.get().maxVersions());
    }

    @Test
    void responseLossLeavesReapingStateAndRetryUsesTheSamePrefix() {
        final CheckpointUploadIntentStore store = new CheckpointUploadIntentStore();
        final CheckpointUploadIntent pending = pending();
        final CheckpointReapingOwnerProof ownerProof = ownerProof(pending);
        store.create(pending);
        final AtomicInteger calls = new AtomicInteger();
        final AtomicReference<CheckpointPrefixSweepRequest> first = new AtomicReference<>();
        final AtomicReference<CheckpointPrefixSweepRequest> second = new AtomicReference<>();
        final CheckpointPrefixSweepAdapter adapter = request -> {
            if (calls.incrementAndGet() == 1) {
                first.set(request);
                throw new IllegalStateException("provider response lost after sweep");
            }
            second.set(request);
            return result(0, 0);
        };
        final CheckpointReapingSweepCoordinator coordinator = new CheckpointReapingSweepCoordinator(store, adapter);

        assertThrows(
                IllegalStateException.class,
                () -> coordinator.reap(
                        pending,
                        new RecoveryCatalog(),
                        ownerProof,
                        quiescence(pending, ownerProof, evidence(5_000)),
                        100));
        assertEquals(
                CheckpointUploadState.REAPING, store.current().orElseThrow().state());
        final CheckpointReapingSweepResult retried = coordinator.reap(
                pending, new RecoveryCatalog(), ownerProof, quiescence(pending, ownerProof, evidence(5_000)), 100);

        assertTrue(retried.prefixSweep().emptyAfterSweep());
        assertEquals(first.get().objectStoreProfile(), second.get().objectStoreProfile());
        assertArrayEquals(first.get().recoveryLineageId(), second.get().recoveryLineageId());
        assertArrayEquals(first.get().checkpointId(), second.get().checkpointId());
    }

    @Test
    void catalogProtectionPreventsProviderSweep() {
        final CheckpointUploadIntentStore store = new CheckpointUploadIntentStore();
        final CheckpointUploadIntent pending = pending();
        final CheckpointReapingOwnerProof ownerProof = ownerProof(pending);
        store.create(pending);
        final AtomicBoolean called = new AtomicBoolean();
        final CheckpointPrefixSweepAdapter adapter = request -> {
            called.set(true);
            return result(0, 0);
        };
        final RecoveryCatalog catalog = new RecoveryCatalog();
        catalog.publish(manifest(pending), 0);

        assertThrows(IllegalStateException.class, () -> new CheckpointReapingSweepCoordinator(store, adapter)
                .reap(pending, catalog, ownerProof, quiescence(pending, ownerProof, evidence(5_000)), 100));
        assertEquals(
                CheckpointUploadState.PENDING_UPLOAD,
                store.current().orElseThrow().state());
        assertFalse(called.get());
    }

    @Test
    void providerOwnershipHorizonBlocksSweepAfterTheRequestWindow() {
        final CheckpointUploadIntentStore store = new CheckpointUploadIntentStore();
        final CheckpointUploadIntent pending = pending();
        final CheckpointReapingOwnerProof ownerProof = ownerProof(pending);
        store.create(pending);
        final AtomicBoolean called = new AtomicBoolean();
        final CheckpointPrefixSweepAdapter adapter = request -> {
            called.set(true);
            return result(0, 0);
        };
        final CheckpointReapingQuiescenceProof proof =
                quiescence(pending, ownerProof, evidence(5_000), evidence(10_000), evidence(7_000), evidence(11_000));

        assertThrows(IllegalStateException.class, () -> new CheckpointReapingSweepCoordinator(store, adapter)
                .reap(pending, new RecoveryCatalog(), ownerProof, proof, 100));
        assertEquals(
                CheckpointUploadState.REAPING, store.current().orElseThrow().state());
        assertFalse(called.get());
    }

    @Test
    void quiescenceReceiptMustBindTheExactOwnerProof() {
        final CheckpointUploadIntentStore store = new CheckpointUploadIntentStore();
        final CheckpointUploadIntent pending = pending();
        final CheckpointReapingOwnerProof ownerProof = ownerProof(pending);
        store.create(pending);
        final AtomicBoolean called = new AtomicBoolean();
        final CheckpointPrefixSweepAdapter adapter = request -> {
            called.set(true);
            return result(0, 0);
        };
        final CheckpointReapingQuiescenceProof mismatched = new CheckpointReapingQuiescenceProof(
                pending.intentDigest(),
                evidence(5_000),
                evidence(10_000),
                evidence(7_000),
                evidence(7_000),
                1_000,
                500,
                10,
                bytes(32, 65),
                bytes(32, 61));

        assertThrows(IllegalStateException.class, () -> new CheckpointReapingSweepCoordinator(store, adapter)
                .reap(pending, new RecoveryCatalog(), ownerProof, mismatched, 100));
        assertEquals(
                CheckpointUploadState.REAPING, store.current().orElseThrow().state());
        assertFalse(called.get());
    }

    @Test
    void horizonMustCoverProviderLifetimeAndTrustedClockWidth() {
        final CheckpointUploadIntent pending = pending();
        assertThrows(
                IllegalArgumentException.class,
                () -> new CheckpointReapingQuiescenceProof(
                        pending.intentDigest(),
                        evidence(5_000),
                        evidence(10_000),
                        evidence(7_000),
                        evidence(7_000),
                        509,
                        500,
                        10,
                        bytes(32, 60),
                        bytes(32, 61)));
    }

    private static CheckpointPrefixSweepResult result(final int listed, final int deleted) {
        return new CheckpointPrefixSweepResult(listed, deleted, bytes(32, 40), bytes(32, 41));
    }

    private static CheckpointUploadIntent pending() {
        final com.nereusstream.delay.protocol.ShardId shard =
                new com.nereusstream.delay.protocol.ShardId(new RouteIncarnation(bytes(16, 1)), 3);
        return new CheckpointUploadIntent(
                new ShardSubject(shard),
                bytes(16, 2),
                bytes(16, 3),
                new OwnerIdentity(bytes(8, 4), bytes(8, 5), 9, bytes(32, 6)),
                bytes(16, 7),
                bytes(32, 8),
                11,
                bytes(16, 9),
                bytes(32, 10),
                new ProfileRef(Bytes.utf8("store"), 1, bytes(32, 15), ProfileKind.OBJECT_STORE),
                evidence(1_000),
                5_000,
                CheckpointUploadState.PENDING_UPLOAD,
                2,
                null,
                null);
    }

    private static CheckpointManifest manifest(final CheckpointUploadIntent pending) {
        final com.nereusstream.delay.protocol.ShardId shard = pending.shard().shardId();
        final KafkaSourcePosition position = new KafkaSourcePosition(
                shard, "cluster", UUID.fromString("00000000-0000-0000-0000-000000000001"), 1, null, 1_001);
        final CheckpointManifest.CreatedBy createdBy = new CheckpointManifest.CreatedBy(
                pending.owner().deploymentId(),
                pending.owner().workerRunId(),
                pending.owner().ownerEpoch());
        final CheckpointManifest.CreatedAt createdAt = new CheckpointManifest.CreatedAt(
                1_000, 1_001, "CERTIFIED_HOST_CLOCK", bytes(8, 50), 1, 2, 3, bytes(32, 51), 0, null);
        return new CheckpointManifest(
                pending.checkpointId(),
                pending.recoveryLineageId(),
                0,
                null,
                null,
                createdBy,
                createdAt,
                shard,
                bytes(32, 52),
                UUID.randomUUID(),
                1,
                11,
                position,
                bytes(32, 53),
                bytes(32, 54),
                java.util.List.of(),
                java.util.List.of(new CheckpointManifest.FileEntry(
                        "CURRENT", 1, bytes(32, 55), bytes(16, 56), bytes(16, 57), null)));
    }

    private static TrustedUtcIntervalEvidence evidence(final long time) {
        return new TrustedUtcIntervalEvidence(
                time,
                time + 1,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                bytes(8, 16),
                1,
                2,
                3,
                bytes(32, 17),
                0,
                null);
    }

    private static CheckpointReapingQuiescenceProof quiescence(
            final CheckpointUploadIntent pending,
            final CheckpointReapingOwnerProof ownerProof,
            final TrustedUtcIntervalEvidence reapingEvidence) {
        return quiescence(pending, ownerProof, reapingEvidence, evidence(10_000), evidence(7_000), evidence(7_000));
    }

    private static CheckpointReapingQuiescenceProof quiescence(
            final CheckpointUploadIntent pending,
            final CheckpointReapingOwnerProof ownerProof,
            final TrustedUtcIntervalEvidence reapingEvidence,
            final TrustedUtcIntervalEvidence observedAt,
            final TrustedUtcIntervalEvidence oldOwnerClosedAt,
            final TrustedUtcIntervalEvidence providerClosedAt) {
        return new CheckpointReapingQuiescenceProof(
                pending.intentDigest(),
                reapingEvidence,
                observedAt,
                oldOwnerClosedAt,
                providerClosedAt,
                1_000,
                500,
                10,
                ownerProof.proofDigest(),
                bytes(32, 61));
    }

    private static CheckpointReapingOwnerProof ownerProof(final CheckpointUploadIntent pending) {
        final OwnerLease lease = new OwnerLease(
                pending.shard().shardId(),
                "owner-proof",
                pending.owner().ownerEpoch(),
                bytes(32, 62),
                20_000,
                new OwnerLeaseContext(bytes(32, 63), 1, bytes(32, 64)),
                ShardLifecycleState.ACTIVE_FOR_COMMANDS);
        return new CheckpointReapingOwnerProof(
                pending.intentDigest(),
                pending.owner(),
                pending.sourceStoreIncarnation(),
                lease,
                CheckpointReapingOwnerProof.Kind.EXACT_OWNER_EXPLICIT_ABANDON,
                null,
                evidence(7_000));
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
