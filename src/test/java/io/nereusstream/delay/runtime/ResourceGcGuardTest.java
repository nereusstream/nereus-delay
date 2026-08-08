package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CanonicalProtobuf;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.OwnerIdentityV1;
import io.nereusstream.delay.protocol.ProfileKindV1;
import io.nereusstream.delay.protocol.ProfileRefV1;
import io.nereusstream.delay.protocol.RecoveryCandidateKindV1;
import io.nereusstream.delay.protocol.RecoveryCandidateRefV1;
import io.nereusstream.delay.protocol.RecoveryFloorRefV1;
import io.nereusstream.delay.protocol.RecoveryPinV1;
import io.nereusstream.delay.protocol.ResourceKind;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.ShardSubjectV1;
import io.nereusstream.delay.protocol.SourcePosition;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import io.nereusstream.delay.store.CheckpointManifest;
import io.nereusstream.delay.store.RecoveryCatalog;
import io.nereusstream.delay.store.RecoveryCatalogAuthority;
import io.nereusstream.delay.store.RecoveryFloor;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ResourceGcGuardTest {
    @Test
    void checkpointGcFailsClosedWhenRecoveryPinStateCannotBeRead() {
        final Fixture fixture = fixture();
        final RecoveryCatalogAuthority catalog = authority(fixture.floor(), null,
                new UnsupportedOperationException("pin read unavailable"));

        assertEquals(ResourceGcGuard.Decision.RECOVERY_PIN_STATE_UNAVAILABLE,
                ResourceGcGuard.evaluate(fixture.intent(), fixture.confirmation(), catalog,
                        fixture.floor().checkpointId()));
    }

    @Test
    void durableGcRecordsPreserveUnsignedResourceStateVersionBits() {
        final Fixture fixture = fixture();

        assertEquals(Long.MIN_VALUE,
                ResourceRetireIntentRecord.decode(fixture.intent().encode()).expectedResourceStateVersion());
        assertEquals(Long.MIN_VALUE,
                ResourceDeleteConfirmedRecord.decode(fixture.confirmation().encode())
                        .retireIntent().expectedResourceStateVersion());
    }

    @Test
    void deleteConfirmationMustCarryByteIdenticalRetireIntent() {
        final Fixture fixture = fixture();
        final ResourceRetireIntentRecord altered = new ResourceRetireIntentRecord(
                fixture.intent().mutationId(), fixture.intent().mutationHash(), fixture.intent().resourceKind(),
                fixture.intent().resourceIdentity(), fixture.intent().resourceIdentityHash(),
                fixture.intent().expectedResourceStateVersion(), fixture.intent().appliedMutationSequence(),
                Bytes.utf8("different-protection-set"), fixture.intent().appliedSourcePosition());
        final ResourceDeleteConfirmedRecord confirmation = new ResourceDeleteConfirmedRecord(
                fixture.confirmation().confirmationMutationId(), fixture.confirmation().confirmationMutationHash(),
                altered, fixture.confirmation().outcome(), fixture.confirmation().appliedMutationSequence(),
                fixture.confirmation().providerRequestIdHash(), fixture.confirmation().observedImmutableVersion(),
                fixture.confirmation().observedEtag(), fixture.confirmation().responseHash(),
                fixture.confirmation().observedAt(), fixture.confirmation().confirmedAt(),
                fixture.confirmation().appliedSourcePosition());

        assertEquals(ResourceGcGuard.Decision.INTENT_REFERENCE_MISMATCH,
                ResourceGcGuard.evaluate(fixture.intent(), confirmation, fixture.floor()));
    }

    @Test
    void checkpointGcRetainsCheckpointPinnedAsCandidateOrObservedFloor() {
        final Fixture fixture = fixture();
        final RecoveryPinV1 candidatePin = pin(fixture, fixture.floor().checkpointId(),
                fixture.floor().manifestSha256());
        assertEquals(ResourceGcGuard.Decision.RECOVERY_PIN_PROTECTS_RESOURCE,
                ResourceGcGuard.evaluate(fixture.intent(), fixture.confirmation(),
                        authority(fixture.floor(), candidatePin, null), fixture.floor().checkpointId()));

        final byte[] otherCheckpoint = id16(51);
        final byte[] otherManifest = id32(52);
        final RecoveryPinV1 floorPin = pin(fixture, otherCheckpoint, otherManifest);
        assertEquals(ResourceGcGuard.Decision.RECOVERY_PIN_PROTECTS_RESOURCE,
                ResourceGcGuard.evaluate(fixture.intent(), fixture.confirmation(),
                        authority(fixture.floor(), floorPin, null), fixture.floor().checkpointId()));
    }

    @Test
    void checkpointGcFailsClosedWhenRecoveryFloorCannotBeRead() {
        final Fixture fixture = fixture();
        assertEquals(ResourceGcGuard.Decision.FLOOR_SOURCE_OR_SEQUENCE_NOT_COVERING,
                ResourceGcGuard.evaluate(fixture.intent(), fixture.confirmation(),
                        authority(fixture.floor(), null, null,
                                new IllegalStateException("floor read unavailable"), null),
                        fixture.floor().checkpointId()));
    }

    @Test
    void checkpointGcFailsClosedWhenFloorCoverageCannotBeProved() {
        final Fixture fixture = fixture();
        assertEquals(ResourceGcGuard.Decision.FLOOR_SOURCE_OR_SEQUENCE_NOT_COVERING,
                ResourceGcGuard.evaluate(fixture.intent(), fixture.confirmation(),
                        authority(fixture.floor(), null, null, null,
                                new IllegalStateException("coverage proof unavailable")),
                        fixture.floor().checkpointId()));
    }

    private static Fixture fixture() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 3);
        final SourcePosition intentPosition = position(shard, 1, 1_001);
        final SourcePosition confirmationPosition = position(shard, 2, 1_002);
        final byte[] checkpointId = id16(1);
        final byte[] manifestHash = id32(2);
        final byte[] resourceIdentity = checkpointIdentity(checkpointId, manifestHash);
        final byte[] resourceIdentityHash = Bytes.sha256(Bytes.utf8("nereus-delay-resource-identity-v1\0"),
                resourceIdentity);
        final ResourceRetireIntentRecord intent = new ResourceRetireIntentRecord(
                id32(3), id32(4), ResourceKind.CHECKPOINT, resourceIdentity, resourceIdentityHash,
                Long.MIN_VALUE, 1, Bytes.utf8("floor-protection"), intentPosition.canonicalBytes());
        final TrustedUtcIntervalEvidence evidence = evidence();
        final ResourceDeleteConfirmedRecord confirmation = new ResourceDeleteConfirmedRecord(
                id32(5), id32(6), intent,
                io.nereusstream.delay.protocol.ResourceDeleteConfirmedBody.DeleteOutcome.ALREADY_ABSENT,
                2, id32(7), new byte[0], new byte[0], id32(8), evidence.canonicalBytes(),
                evidence.canonicalBytes(), confirmationPosition.canonicalBytes());
        final RecoveryFloor floor = RecoveryFloor.create(id16(9), checkpointId, manifestHash, 4,
                confirmationPosition, 2, id32(10));
        return new Fixture(shard, intent, confirmation, floor);
    }

    private static RecoveryPinV1 pin(final Fixture fixture, final byte[] candidateCheckpoint,
                                     final byte[] candidateManifest) {
        final RecoveryFloor floor = fixture.floor();
        final RecoveryFloorRefV1 floorRef = new RecoveryFloorRefV1(floor.recoveryLineageId(), floor.checkpointId(),
                floor.manifestSha256(), floor.catalogGeneration(), floor.appliedSourcePosition(),
                floor.includedMutationSequence(), List.of());
        final RecoveryCandidateRefV1 candidate = new RecoveryCandidateRefV1(
                RecoveryCandidateKindV1.CATALOG_CHECKPOINT, floor.recoveryLineageId(), candidateCheckpoint,
                candidateManifest, null);
        return new RecoveryPinV1(id16(11), new ShardSubjectV1(fixture.shard()),
                new OwnerIdentityV1(Bytes.utf8("deployment"), Bytes.utf8("worker"), 1, id32(12)),
                candidate, floorRef, floor.catalogGeneration(), id32(13));
    }

    private static RecoveryCatalogAuthority authority(final RecoveryFloor floor, final RecoveryPinV1 pin,
                                                      final RuntimeException pinFailure) {
        return authority(floor, pin, pinFailure, null, null);
    }

    private static RecoveryCatalogAuthority authority(final RecoveryFloor floor, final RecoveryPinV1 pin,
                                                      final RuntimeException pinFailure,
                                                      final RuntimeException floorFailure,
                                                      final RuntimeException proofFailure) {
        return new RecoveryCatalogAuthority() {
            @Override
            public RecoveryCatalog.Publication publish(final CheckpointManifest manifest,
                                                       final long expectedCatalogGeneration) {
                throw new AssertionError("publish must not be called by GC guard");
            }

            @Override
            public RecoveryFloor advanceFloor(final byte[] checkpointId, final long expectedCatalogGeneration,
                                              final byte[] evidenceCursorDigest) {
                throw new AssertionError("advanceFloor must not be called by GC guard");
            }

            @Override
            public Optional<CheckpointManifest> manifest(final byte[] checkpointId) {
                return Optional.empty();
            }

            @Override
            public Optional<RecoveryFloor> currentFloor() {
                if (floorFailure != null) {
                    throw floorFailure;
                }
                return Optional.of(floor);
            }

            @Override
            public void validatePublishedRestoreCandidate(final CheckpointManifest candidate) {
                throw new AssertionError("restore validation must not be called by GC guard");
            }

            @Override
            public Optional<RecoveryCatalog.FloorCoverage> proveFloorCoverage(
                    final byte[] candidateCheckpointId, final long requiredMutationSequence,
                    final SourcePosition... requiredPositions) {
                if (proofFailure != null) {
                    throw proofFailure;
                }
                throw new AssertionError("Floor proof must not be reached while a pin blocks deletion");
            }

            @Override
            public Optional<RecoveryPinV1> activeRecoveryPin() {
                if (pinFailure != null) {
                    throw pinFailure;
                }
                return Optional.ofNullable(pin);
            }
        };
    }

    private static byte[] checkpointIdentity(final byte[] checkpointId, final byte[] manifestHash) {
        final byte[] branch = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, id16(14));
            CanonicalProtobuf.bytes(output, 2, checkpointId);
            CanonicalProtobuf.bytes(output, 3, new ProfileRefV1(Bytes.utf8("checkpoint-profile"), 1,
                    id32(15), ProfileKindV1.OBJECT_STORE).canonicalBytes());
            CanonicalProtobuf.bytes(output, 4, Bytes.utf8("bucket"));
            CanonicalProtobuf.bytes(output, 5, Bytes.utf8("checkpoint/1"));
            CanonicalProtobuf.bytes(output, 6, Bytes.utf8("object-version"));
            CanonicalProtobuf.uint64(output, 7, 1);
            CanonicalProtobuf.bytes(output, 8, manifestHash);
        });
        return CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output,
                ResourceKind.CHECKPOINT.wireValue(), branch));
    }

    private static TrustedUtcIntervalEvidence evidence() {
        return new TrustedUtcIntervalEvidence(1_000, 1_001,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("clock"), 1, 1, 1,
                id32(16), 0, null);
    }

    private static KafkaSourcePosition position(final ShardId shard, final long offset, final long appendTime) {
        return new KafkaSourcePosition(shard, "cluster", UUID.fromString("00000000-0000-0000-0000-000000000001"),
                offset, null, appendTime);
    }

    private static byte[] id16(final int seed) {
        return java.util.Arrays.copyOf(id32(seed), 16);
    }

    private static byte[] id32(final int seed) {
        return Bytes.sha256(Bytes.utf8("resource-gc-" + seed));
    }

    private record Fixture(ShardId shard, ResourceRetireIntentRecord intent,
                           ResourceDeleteConfirmedRecord confirmation, RecoveryFloor floor) {
    }
}
