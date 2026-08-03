package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CheckpointUploadIntentV1;
import io.nereusstream.delay.protocol.CheckpointUploadStateV1;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.OwnerIdentityV1;
import io.nereusstream.delay.protocol.ProfileKindV1;
import io.nereusstream.delay.protocol.ProfileRefV1;
import io.nereusstream.delay.protocol.RecoveryCandidateKindV1;
import io.nereusstream.delay.protocol.RecoveryCandidateRefV1;
import io.nereusstream.delay.protocol.RecoveryFloorRefV1;
import io.nereusstream.delay.protocol.RecoveryPinV1;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.ShardSubjectV1;
import io.nereusstream.delay.protocol.SourcePosition;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CheckpointReapingGuardTest {
    @Test
    void failsClosedWhenCatalogProtectionCannotBeRead() {
        final CheckpointUploadIntentV1 pending = pending();
        assertEquals(CheckpointReapingGuard.Decision.CATALOG_STATE_UNAVAILABLE,
                CheckpointReapingGuard.evaluate(pending, evidence(5_000), authority(null, true)));
    }

    @Test
    void activeRecoveryPinBlocksReapingAndUnpinnedCatalogAllowsIt() {
        final CheckpointUploadIntentV1 pending = pending();
        final RecoveryPinV1 pin = pin(pending);
        assertEquals(CheckpointReapingGuard.Decision.RECOVERY_PIN_PROTECTION,
                CheckpointReapingGuard.evaluate(pending, evidence(5_000), authority(pin, false)));

        final CheckpointUploadIntentStore store = new CheckpointUploadIntentStore();
        store.create(pending);
        assertThrows(IllegalStateException.class,
                () -> store.beginReaping(pending, evidence(5_000), authority(pin, false)));
        assertEquals(CheckpointUploadStateV1.PENDING_UPLOAD, store.current().orElseThrow().state());

        final CheckpointUploadIntentStore unpinnedStore = new CheckpointUploadIntentStore();
        unpinnedStore.create(pending);
        assertEquals(CheckpointUploadStateV1.REAPING,
                unpinnedStore.beginReaping(pending, evidence(5_000), new RecoveryCatalog()).state());
    }

    private static CheckpointUploadIntentV1 pending() {
        return new CheckpointUploadIntentV1(
                new ShardSubjectV1(new RouteIncarnation(bytes(16, 1)), 3),
                bytes(16, 2), bytes(16, 3),
                new OwnerIdentityV1(bytes(8, 4), bytes(8, 5), 9, bytes(32, 6)),
                bytes(16, 7), bytes(32, 8), 11, bytes(16, 9), bytes(32, 10),
                new ProfileRefV1(Bytes.utf8("store"), 1, bytes(32, 15), ProfileKindV1.OBJECT_STORE),
                evidence(1_000), 5_000, CheckpointUploadStateV1.PENDING_UPLOAD, 2, null, null);
    }

    private static RecoveryPinV1 pin(final CheckpointUploadIntentV1 pending) {
        final ShardId shard = pending.shard().shardId();
        final SourcePosition position = new KafkaSourcePosition(shard, "cluster",
                UUID.fromString("00000000-0000-0000-0000-000000000001"), 1, null, 1_001);
        final byte[] manifestHash = bytes(32, 18);
        final RecoveryFloorRefV1 floor = new RecoveryFloorRefV1(pending.recoveryLineageId(),
                pending.checkpointId(), manifestHash, 1, position, 1, java.util.List.of());
        final RecoveryCandidateRefV1 candidate = new RecoveryCandidateRefV1(
                RecoveryCandidateKindV1.CATALOG_CHECKPOINT, pending.recoveryLineageId(), pending.checkpointId(),
                manifestHash, null);
        return new RecoveryPinV1(bytes(16, 19), pending.shard(), pending.owner(), candidate, floor, 1,
                bytes(32, 20));
    }

    private static RecoveryCatalogAuthority authority(final RecoveryPinV1 pin, final boolean failCatalog) {
        return new RecoveryCatalogAuthority() {
            @Override
            public RecoveryCatalog.Publication publish(final CheckpointManifest manifest,
                                                       final long expectedCatalogGeneration) {
                throw new AssertionError("publish must not be called by reaping guard");
            }

            @Override
            public RecoveryFloor advanceFloor(final byte[] checkpointId, final long expectedCatalogGeneration,
                                              final byte[] evidenceCursorDigest) {
                throw new AssertionError("advanceFloor must not be called by reaping guard");
            }

            @Override
            public Optional<CheckpointManifest> manifest(final byte[] checkpointId) {
                if (failCatalog) {
                    throw new IllegalStateException("catalog read unavailable");
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
                    final byte[] candidateCheckpointId, final long requiredMutationSequence,
                    final SourcePosition... requiredPositions) {
                return Optional.empty();
            }

            @Override
            public Optional<RecoveryPinV1> activeRecoveryPin() {
                return Optional.ofNullable(pin);
            }
        };
    }

    private static TrustedUtcIntervalEvidence evidence(final long time) {
        return new TrustedUtcIntervalEvidence(time, time + 1,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, bytes(8, 16), 1, 2, 3,
                bytes(32, 17), 0, null);
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
