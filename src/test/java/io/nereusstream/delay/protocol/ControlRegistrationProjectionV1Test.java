package io.nereusstream.delay.protocol;

import org.junit.jupiter.api.Test;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ControlRegistrationProjectionV1Test {
    @Test
    void initialProjectionBindsReceiptAndAllPreparedTargets() throws Exception {
        final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final ControlOperationRequestV1 request = ControlOperationRequestV1.forceCheckpoint(
                new ForceCheckpointRequestV1(new ControlReasonV1(ControlReasonKindV1.MAINTENANCE, null, null)));
        final ControlTargetRefV1 target = new ControlTargetRefV1(0, ControlTargetKindV1.SHARD,
                new ShardSubjectV1(new ShardId(new RouteIncarnation(bytes(16, 1)), 3)), null, null);
        final PreparedControlOperationV1 prepared = PreparedControlOperationV1.prepare(bytes(32, 2),
                request.kind(), new ControlAuthorV1(bytes(32, 3), bytes(32, 4), bytes(32, 5)), request,
                List.of(target), 1, 2, 1, keyPair.getPrivate());
        final TrustedUtcIntervalEvidence time = new TrustedUtcIntervalEvidence(100, 110,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("clock"), 1, 2, 3,
                bytes(32, 6), 0, null);
        final ControlRegistrationProjectionV1 projection = ControlRegistrationProjectionV1.initial(prepared,
                time, 200);
        assertEquals(1, projection.receipt().operationRevision());
        assertEquals(ControlOperationStateV1.PENDING, projection.current().state());
        assertEquals(1, projection.current().targetStates().size());
        assertArrayEquals(prepared.targetSnapshotHash(), projection.receipt().targetSnapshotHash());
    }

    @Test
    void projectionRejectsReceiptCurrentIdentityDrift() throws Exception {
        final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final ControlOperationRequestV1 request = ControlOperationRequestV1.forceCheckpoint(
                new ForceCheckpointRequestV1(new ControlReasonV1(ControlReasonKindV1.MAINTENANCE, null, null)));
        final ControlTargetRefV1 target = new ControlTargetRefV1(0, ControlTargetKindV1.SHARD,
                new ShardSubjectV1(new ShardId(new RouteIncarnation(bytes(16, 7)), 0)), null, null);
        final PreparedControlOperationV1 prepared = PreparedControlOperationV1.prepare(bytes(32, 8),
                request.kind(), new ControlAuthorV1(bytes(32, 9), bytes(32, 10), bytes(32, 11)), request,
                List.of(target), 1, 2, 1, keyPair.getPrivate());
        final TrustedUtcIntervalEvidence time = new TrustedUtcIntervalEvidence(100, 110,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("clock"), 1, 2, 3,
                bytes(32, 12), 0, null);
        final ControlOperationReceiptV1 receipt = ControlOperationReceiptV1.create(prepared.operationId(),
                prepared.requestHash(), prepared.author().tenantResourceScopeHash(), prepared.targetSnapshotHash(),
                1, time, 200);
        assertThrows(IllegalArgumentException.class, () -> new ControlRegistrationProjectionV1(receipt,
                new CurrentControlOperationV1(bytes(32, 99), prepared.requestHash(),
                        prepared.author().tenantResourceScopeHash(), ControlOperationStateV1.PENDING, 1,
                        List.of(), null)));
    }

    @Test
    void queryWindowUsesTrustedLatestAndFailsClosedOnOverflow() throws Exception {
        final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final ControlOperationRequestV1 request = ControlOperationRequestV1.forceCheckpoint(
                new ForceCheckpointRequestV1(new ControlReasonV1(ControlReasonKindV1.MAINTENANCE, null, null)));
        final ControlTargetRefV1 target = new ControlTargetRefV1(0, ControlTargetKindV1.SHARD,
                new ShardSubjectV1(new ShardId(new RouteIncarnation(bytes(16, 24)), 0)), null, null);
        final PreparedControlOperationV1 prepared = PreparedControlOperationV1.prepare(bytes(32, 25),
                request.kind(), new ControlAuthorV1(bytes(32, 26), bytes(32, 27), bytes(32, 28)), request,
                List.of(target), 1, 2, 1, keyPair.getPrivate());
        final TrustedUtcIntervalEvidence time = new TrustedUtcIntervalEvidence(Long.MAX_VALUE - 10,
                Long.MAX_VALUE - 1, TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("clock"), 1, 2, 3, bytes(32, 29), 0, null);
        final ControlRegistrationProjectionV1 projection = ControlRegistrationProjectionV1.initialWithQueryWindow(
                prepared, time, 1);
        assertEquals(Long.MAX_VALUE, projection.receipt().queryUntilEpochMs());
        assertThrows(IllegalArgumentException.class, () -> ControlOperationReceiptV1.createWithQueryWindow(
                prepared.operationId(), prepared.requestHash(), prepared.author().tenantResourceScopeHash(),
                prepared.targetSnapshotHash(), 1, time, 2));
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
