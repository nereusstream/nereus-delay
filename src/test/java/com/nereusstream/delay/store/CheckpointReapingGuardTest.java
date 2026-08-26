package com.nereusstream.delay.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CheckpointUploadIntent;
import com.nereusstream.delay.protocol.CheckpointUploadState;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.RecoveryCandidateKind;
import com.nereusstream.delay.protocol.RecoveryCandidateRef;
import com.nereusstream.delay.protocol.RecoveryFloorRef;
import com.nereusstream.delay.protocol.RecoveryPin;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.ShardSubject;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class CheckpointReapingGuardTest {
    @Test
    void failsClosedWhenCatalogProtectionCannotBeRead() {
        final CheckpointUploadIntent pending = pending();
        assertEquals(
                CheckpointReapingGuard.Decision.CATALOG_STATE_UNAVAILABLE,
                CheckpointReapingGuard.evaluate(pending, evidence(5_000), authority(null, true)));
    }

    @Test
    void failsClosedWhenCatalogReadThrowsFatalError() {
        final CheckpointUploadIntent pending = pending();
        assertEquals(
                CheckpointReapingGuard.Decision.CATALOG_STATE_UNAVAILABLE,
                CheckpointReapingGuard.evaluate(
                        pending, evidence(5_000), authority(null, new AssertionError("catalog read failed fatally"))));
    }

    @Test
    void failsClosedWhenRecoveryPinReadThrowsFatalError() {
        final CheckpointUploadIntent pending = pending();
        assertEquals(
                CheckpointReapingGuard.Decision.RECOVERY_PIN_STATE_UNAVAILABLE,
                CheckpointReapingGuard.evaluate(
                        pending,
                        evidence(5_000),
                        authority(null, null, new AssertionError("pin read failed fatally"))));
    }

    @Test
    void activeRecoveryPinBlocksReapingAndUnpinnedCatalogAllowsIt() {
        final CheckpointUploadIntent pending = pending();
        final RecoveryPin pin = pin(pending);
        assertEquals(
                CheckpointReapingGuard.Decision.RECOVERY_PIN_PROTECTION,
                CheckpointReapingGuard.evaluate(pending, evidence(5_000), authority(pin, false)));

        final CheckpointUploadIntentStore store = new CheckpointUploadIntentStore();
        store.create(pending);
        assertThrows(
                IllegalStateException.class, () -> store.beginReaping(pending, evidence(5_000), authority(pin, false)));
        assertEquals(
                CheckpointUploadState.PENDING_UPLOAD,
                store.current().orElseThrow().state());

        final CheckpointUploadIntentStore unpinnedStore = new CheckpointUploadIntentStore();
        unpinnedStore.create(pending);
        assertEquals(
                CheckpointUploadState.REAPING,
                unpinnedStore
                        .beginReaping(pending, evidence(5_000), new RecoveryCatalog())
                        .state());
    }

    private static CheckpointUploadIntent pending() {
        return new CheckpointUploadIntent(
                new ShardSubject(new RouteIncarnation(bytes(16, 1)), 3),
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

    private static RecoveryPin pin(final CheckpointUploadIntent pending) {
        final ShardId shard = pending.shard().shardId();
        final SourcePosition position = new KafkaSourcePosition(
                shard, "cluster", UUID.fromString("00000000-0000-0000-0000-000000000001"), 1, null, 1_001);
        final byte[] manifestHash = bytes(32, 18);
        final RecoveryFloorRef floor = new RecoveryFloorRef(
                pending.recoveryLineageId(), pending.checkpointId(), manifestHash, 1, position, 1, java.util.List.of());
        final RecoveryCandidateRef candidate = new RecoveryCandidateRef(
                RecoveryCandidateKind.CATALOG_CHECKPOINT,
                pending.recoveryLineageId(),
                pending.checkpointId(),
                manifestHash,
                null);
        return new RecoveryPin(bytes(16, 19), pending.shard(), pending.owner(), candidate, floor, 1, bytes(32, 20));
    }

    private static RecoveryCatalogAuthority authority(final RecoveryPin pin, final boolean failCatalog) {
        return authority(pin, failCatalog ? new IllegalStateException("catalog read unavailable") : null, null);
    }

    private static RecoveryCatalogAuthority authority(final RecoveryPin pin, final Throwable catalogFailure) {
        return authority(pin, catalogFailure, null);
    }

    private static RecoveryCatalogAuthority authority(
            final RecoveryPin pin, final Throwable catalogFailure, final Throwable pinFailure) {
        return new RecoveryCatalogAuthority() {
            @Override
            public RecoveryCatalog.Publication publish(
                    final CheckpointManifest manifest, final long expectedCatalogGeneration) {
                throw new AssertionError("publish must not be called by reaping guard");
            }

            @Override
            public RecoveryFloor advanceFloor(
                    final byte[] checkpointId,
                    final long expectedCatalogGeneration,
                    final byte[] evidenceCursorDigest) {
                throw new AssertionError("advanceFloor must not be called by reaping guard");
            }

            @Override
            public Optional<CheckpointManifest> manifest(final byte[] checkpointId) {
                if (catalogFailure != null) {
                    throwUnchecked(catalogFailure);
                }
                return Optional.empty();
            }

            @Override
            public Optional<RecoveryFloor> currentFloor() {
                return Optional.empty();
            }

            @Override
            public void validatePublishedRestoreCandidate(final CheckpointManifest candidate) {
                throw new AssertionError("restore validation must not be called by reaping guard");
            }

            @Override
            public Optional<RecoveryCatalog.FloorCoverage> proveFloorCoverage(
                    final byte[] candidateCheckpointId,
                    final long requiredMutationSequence,
                    final SourcePosition... requiredPositions) {
                return Optional.empty();
            }

            @Override
            public Optional<RecoveryPin> activeRecoveryPin() {
                if (pinFailure != null) {
                    throwUnchecked(pinFailure);
                }
                return Optional.ofNullable(pin);
            }
        };
    }

    private static void throwUnchecked(final Throwable failure) {
        if (failure instanceof RuntimeException runtimeFailure) {
            throw runtimeFailure;
        }
        if (failure instanceof Error errorFailure) {
            throw errorFailure;
        }
        throw new AssertionError("unexpected test failure type", failure);
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

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
