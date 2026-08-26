package com.nereusstream.delay.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.ownership.InMemoryOwnerLeaseStore;
import com.nereusstream.delay.ownership.OwnerLease;
import com.nereusstream.delay.ownership.OxiaOwnerLeaseStore;
import com.nereusstream.delay.ownership.SourceAssignment;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CheckpointUploadIntent;
import com.nereusstream.delay.protocol.CheckpointUploadState;
import com.nereusstream.delay.protocol.KafkaActivationBarrier;
import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardSubject;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class CheckpointReapingOwnerProofIssuerTest {
    @Test
    void explicitOwnerAbandonmentReleasesExactSessionBoundLease() {
        final CheckpointUploadIntent pending = pending();
        final OxiaOwnerLeaseStore authority = new OxiaOwnerLeaseStore(new InMemoryOwnerLeaseStore());
        final OwnerLease lease = acquire(authority, pending, "old-owner", 1);

        final CheckpointReapingOwnerProof proof =
                CheckpointReapingOwnerProofIssuer.explicitOwnerAbandon(pending, authority, lease, evidence(5_000));

        assertEquals(CheckpointReapingOwnerProof.Kind.EXACT_OWNER_EXPLICIT_ABANDON, proof.kind());
        assertTrue(authority.current(pending.shard().shardId()).isEmpty());
        assertEquals(
                CheckpointReapingOwnerProofGuard.Decision.OWNER_PROOF_ACCEPTED,
                CheckpointReapingOwnerProofGuard.evaluate(pending, proof));
    }

    @Test
    void anotherActorCanProveTheRecordedLeaseWasReplaced() {
        final CheckpointUploadIntent pending = pending();
        final OxiaOwnerLeaseStore authority = new OxiaOwnerLeaseStore(new InMemoryOwnerLeaseStore());
        final OwnerLease oldLease = acquire(authority, pending, "old-owner", 1);
        assertTrue(authority.release(oldLease));
        final OwnerLease replacement = acquire(authority, pending, "new-owner", 2);

        final CheckpointReapingOwnerProof proof = CheckpointReapingOwnerProofIssuer.proveRecordedOwnerNotCurrent(
                pending, authority, oldLease, evidence(5_000));

        assertEquals(CheckpointReapingOwnerProof.Kind.RECORDED_OWNER_NOT_CURRENT, proof.kind());
        assertTrue(proof.observedCurrentLease().sameIdentity(replacement));
        assertEquals(
                CheckpointReapingOwnerProofGuard.Decision.OWNER_PROOF_ACCEPTED,
                CheckpointReapingOwnerProofGuard.evaluate(pending, proof));
    }

    @Test
    void currentRecordedLeaseAndUnclosedDeadlineFailClosed() {
        final CheckpointUploadIntent pending = pending();
        final OxiaOwnerLeaseStore authority = new OxiaOwnerLeaseStore(new InMemoryOwnerLeaseStore());
        final OwnerLease lease = acquire(authority, pending, "current-owner", 1);

        assertThrows(
                IllegalStateException.class,
                () -> CheckpointReapingOwnerProofIssuer.proveRecordedOwnerNotCurrent(
                        pending, authority, lease, evidence(5_000)));
        assertThrows(
                IllegalArgumentException.class,
                () -> CheckpointReapingOwnerProofIssuer.explicitOwnerAbandon(
                        pending, authority, lease, evidence(4_999)));
        assertFalse(authority.current(pending.shard().shardId()).isEmpty());
    }

    private static OwnerLease acquire(
            final OxiaOwnerLeaseStore authority,
            final CheckpointUploadIntent pending,
            final String ownerId,
            final int seed) {
        final SourceAssignment assignment = new SourceAssignment(
                pending.shard().shardId(),
                bytes(32, 40 + seed),
                1,
                new KafkaActivationBarrier(pending.shard().shardId(), "cluster", UUID.randomUUID(), 0));
        return authority
                .acquire(assignment, ownerId, bytes(32, 80 + seed), 0, 20_000)
                .orElseThrow();
    }

    private static CheckpointUploadIntent pending() {
        final com.nereusstream.delay.protocol.ShardId shard =
                new com.nereusstream.delay.protocol.ShardId(new RouteIncarnation(bytes(16, 1)), 3);
        return new CheckpointUploadIntent(
                new ShardSubject(shard),
                bytes(16, 2),
                bytes(16, 3),
                new OwnerIdentity(bytes(8, 4), bytes(8, 5), 1, bytes(32, 6)),
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
