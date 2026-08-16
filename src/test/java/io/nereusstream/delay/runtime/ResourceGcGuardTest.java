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
import static org.junit.jupiter.api.Assertions.assertThrows;

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
    void checkpointGcFailsClosedWhenRecoveryPinReadThrowsFatalError() {
        final Fixture fixture = fixture();
        final RecoveryCatalogAuthority catalog = authority(fixture.floor(), null,
                new AssertionError("pin read failed fatally"));

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
    void retireIntentRecordRequiresCanonicalProtectionSet() {
        final Fixture fixture = fixture();
        assertThrows(IllegalArgumentException.class, () -> new ResourceRetireIntentRecord(
                fixture.intent().mutationId(), fixture.intent().mutationHash(), fixture.intent().resourceKind(),
                fixture.intent().resourceIdentity(), fixture.intent().resourceIdentityHash(),
                fixture.intent().expectedResourceStateVersion(), fixture.intent().appliedMutationSequence(),
                Bytes.utf8("not-a-protection-set"), fixture.intent().appliedSourcePosition()));
    }

    @Test
    void retireIntentRecordRejectsProtectionSourceFromAnotherShard() {
        final Fixture fixture = fixture();
        final ShardId foreignShard = new ShardId(RouteIncarnation.random(), fixture.shard().partition());
        assertThrows(IllegalArgumentException.class, () -> new ResourceRetireIntentRecord(
                fixture.intent().mutationId(), fixture.intent().mutationHash(), fixture.intent().resourceKind(),
                fixture.intent().resourceIdentity(), fixture.intent().resourceIdentityHash(),
                fixture.intent().expectedResourceStateVersion(), fixture.intent().appliedMutationSequence(),
                protectionSetWithSource(foreignShard), fixture.intent().appliedSourcePosition()));
    }

    @Test
    void durableDeletedCheckpointEvidenceCannotOmitPinnedVersion() {
        final Fixture fixture = fixture();
        assertThrows(IllegalArgumentException.class, () -> new ResourceDeleteConfirmedRecord(
                fixture.confirmation().confirmationMutationId(), fixture.confirmation().confirmationMutationHash(),
                fixture.intent(), io.nereusstream.delay.protocol.ResourceDeleteConfirmedBody.DeleteOutcome.DELETED,
                fixture.confirmation().appliedMutationSequence(), fixture.confirmation().providerRequestIdHash(),
                new byte[0], new byte[0], fixture.confirmation().responseHash(), fixture.confirmation().observedAt(),
                fixture.confirmation().confirmedAt(), fixture.confirmation().appliedSourcePosition()));
    }

    @Test
    void durableDeleteConfirmationRequiresConfirmationAfterObservation() {
        final Fixture fixture = fixture();
        final TrustedUtcIntervalEvidence observedAt = new TrustedUtcIntervalEvidence(1_000, 1_010,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("observed-order"), 1, 1, 1,
                id32(18), 0, null);
        final TrustedUtcIntervalEvidence confirmedAt = new TrustedUtcIntervalEvidence(1_005, 1_006,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("confirmed-order"), 1, 2, 1,
                id32(19), 0, null);

        assertThrows(IllegalArgumentException.class, () -> new ResourceDeleteConfirmedRecord(
                fixture.confirmation().confirmationMutationId(), fixture.confirmation().confirmationMutationHash(),
                fixture.intent(), fixture.confirmation().outcome(), fixture.confirmation().appliedMutationSequence(),
                fixture.confirmation().providerRequestIdHash(), fixture.confirmation().observedImmutableVersion(),
                fixture.confirmation().observedEtag(), fixture.confirmation().responseHash(),
                observedAt.canonicalBytes(), confirmedAt.canonicalBytes(),
                fixture.confirmation().appliedSourcePosition()));
    }

    @Test
    void deleteConfirmationMustCarryByteIdenticalRetireIntent() {
        final Fixture fixture = fixture();
        final ResourceRetireIntentRecord altered = new ResourceRetireIntentRecord(
                fixture.intent().mutationId(), fixture.intent().mutationHash(), fixture.intent().resourceKind(),
                fixture.intent().resourceIdentity(), fixture.intent().resourceIdentityHash(),
                fixture.intent().expectedResourceStateVersion(), fixture.intent().appliedMutationSequence(),
                protectionSet(id32(18)), fixture.intent().appliedSourcePosition());
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
    void deleteConfirmationSourcePositionMustMatchRetireIntentSource() {
        final Fixture fixture = fixture();
        final KafkaSourcePosition foreignSource = new KafkaSourcePosition(fixture.shard(), "cluster",
                UUID.fromString("00000000-0000-0000-0000-000000000002"), 2, null, 1_002);
        assertThrows(IllegalArgumentException.class, () -> new ResourceDeleteConfirmedRecord(
                fixture.confirmation().confirmationMutationId(), fixture.confirmation().confirmationMutationHash(),
                fixture.intent(), fixture.confirmation().outcome(), fixture.confirmation().appliedMutationSequence(),
                fixture.confirmation().providerRequestIdHash(), fixture.confirmation().observedImmutableVersion(),
                fixture.confirmation().observedEtag(), fixture.confirmation().responseHash(),
                fixture.confirmation().observedAt(), fixture.confirmation().confirmedAt(),
                foreignSource.canonicalBytes()));

        final ShardId foreignShard = new ShardId(RouteIncarnation.random(), fixture.shard().partition());
        final KafkaSourcePosition foreignShardSource = new KafkaSourcePosition(foreignShard, "cluster",
                UUID.fromString("00000000-0000-0000-0000-000000000001"), 2, null, 1_002);
        assertThrows(IllegalArgumentException.class, () -> new ResourceDeleteConfirmedRecord(
                fixture.confirmation().confirmationMutationId(), fixture.confirmation().confirmationMutationHash(),
                fixture.intent(), fixture.confirmation().outcome(), fixture.confirmation().appliedMutationSequence(),
                fixture.confirmation().providerRequestIdHash(), fixture.confirmation().observedImmutableVersion(),
                fixture.confirmation().observedEtag(), fixture.confirmation().responseHash(),
                fixture.confirmation().observedAt(), fixture.confirmation().confirmedAt(),
                foreignShardSource.canonicalBytes()));
    }

    @Test
    void deleteConfirmationSourcePositionMustFollowRetireIntent() {
        final Fixture fixture = fixture();
        final KafkaSourcePosition samePosition = position(fixture.shard(), 1, 1_002);
        final KafkaSourcePosition earlierPosition = position(fixture.shard(), 0, 1_002);
        for (KafkaSourcePosition invalid : List.of(samePosition, earlierPosition)) {
            assertThrows(IllegalArgumentException.class, () -> new ResourceDeleteConfirmedRecord(
                    fixture.confirmation().confirmationMutationId(),
                    fixture.confirmation().confirmationMutationHash(), fixture.intent(),
                    fixture.confirmation().outcome(), fixture.confirmation().appliedMutationSequence(),
                    fixture.confirmation().providerRequestIdHash(),
                    fixture.confirmation().observedImmutableVersion(), fixture.confirmation().observedEtag(),
                    fixture.confirmation().responseHash(), fixture.confirmation().observedAt(),
                    fixture.confirmation().confirmedAt(), invalid.canonicalBytes()));
        }
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
    void checkpointGcFailsClosedWhenRecoveryFloorReadThrowsFatalError() {
        final Fixture fixture = fixture();
        assertEquals(ResourceGcGuard.Decision.FLOOR_SOURCE_OR_SEQUENCE_NOT_COVERING,
                ResourceGcGuard.evaluate(fixture.intent(), fixture.confirmation(),
                        authority(fixture.floor(), null, null,
                                new AssertionError("floor read failed fatally"), null),
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

    @Test
    void checkpointGcFailsClosedWhenFloorCoverageThrowsFatalError() {
        final Fixture fixture = fixture();
        assertEquals(ResourceGcGuard.Decision.FLOOR_SOURCE_OR_SEQUENCE_NOT_COVERING,
                ResourceGcGuard.evaluate(fixture.intent(), fixture.confirmation(),
                        authority(fixture.floor(), null, null, null,
                                new AssertionError("floor proof failed fatally")),
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
                Long.MIN_VALUE, Long.MIN_VALUE, protectionSet(id32(17)), intentPosition.canonicalBytes());
        final TrustedUtcIntervalEvidence evidence = evidence();
        final TrustedUtcIntervalEvidence confirmationEvidence = new TrustedUtcIntervalEvidence(1_002, 1_003,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("confirmation-clock"), 1, 2, 1,
                id32(20), 0, null);
        final ResourceDeleteConfirmedRecord confirmation = new ResourceDeleteConfirmedRecord(
                id32(5), id32(6), intent,
                io.nereusstream.delay.protocol.ResourceDeleteConfirmedBody.DeleteOutcome.ALREADY_ABSENT,
                -1L, id32(7), new byte[0], new byte[0], id32(8), evidence.canonicalBytes(),
                confirmationEvidence.canonicalBytes(), confirmationPosition.canonicalBytes());
        final RecoveryFloor floor = RecoveryFloor.create(id16(9), checkpointId, manifestHash, 4,
                confirmationPosition, -1L, id32(10));
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
                                                      final Throwable pinFailure) {
        return authority(floor, pin, pinFailure, null, null);
    }

    private static RecoveryCatalogAuthority authority(final RecoveryFloor floor, final RecoveryPinV1 pin,
                                                      final Throwable pinFailure,
                                                      final Throwable floorFailure,
                                                      final Throwable proofFailure) {
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
                    throwUnchecked(floorFailure);
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
                    throwUnchecked(proofFailure);
                }
                throw new AssertionError("Floor proof must not be reached while a pin blocks deletion");
            }

            @Override
            public Optional<RecoveryPinV1> activeRecoveryPin() {
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

    private static byte[] protectionSet(final byte[] protectedResourceId) {
        final byte[] reference = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, 3);
            CanonicalProtobuf.bytes(output, 2, protectedResourceId);
            CanonicalProtobuf.uint32(output, 3, 1);
        });
        final byte[] references = CanonicalProtobuf.message(output ->
                CanonicalProtobuf.bytes(output, 1, reference));
        final byte[] digest = Bytes.sha256(Bytes.utf8("nereus-delay-protection-set-v1\0"), references);
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, reference);
            CanonicalProtobuf.bytes(output, 2, digest);
        });
    }

    private static byte[] protectionSetWithSource(final ShardId sourceShard) {
        final byte[] reference = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, 2);
            CanonicalProtobuf.bytes(output, 2, id32(18));
            CanonicalProtobuf.uint64(output, 3, 1);
            CanonicalProtobuf.bytes(output, 4, position(sourceShard, 1, 1_001).canonicalBytes());
        });
        final byte[] references = CanonicalProtobuf.message(output ->
                CanonicalProtobuf.bytes(output, 1, reference));
        final byte[] digest = Bytes.sha256(Bytes.utf8("nereus-delay-protection-set-v1\0"), references);
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, reference);
            CanonicalProtobuf.bytes(output, 2, digest);
        });
    }

    private record Fixture(ShardId shard, ResourceRetireIntentRecord intent,
                           ResourceDeleteConfirmedRecord confirmation, RecoveryFloor floor) {
    }
}
