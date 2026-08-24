package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CheckpointUploadIntentV1;
import com.nereusstream.delay.protocol.CheckpointUploadStateV1;
import java.util.Objects;

/** Pure fail-closed identity and deadline gate for a checkpoint Owner proof. */
public final class CheckpointReapingOwnerProofGuard {
    private CheckpointReapingOwnerProofGuard() {}

    public enum Decision {
        PENDING_INTENT_NOT_PENDING,
        PENDING_INTENT_MISMATCH,
        OWNER_IDENTITY_MISMATCH,
        STORE_INCARNATION_MISMATCH,
        LEASE_SHARD_MISMATCH,
        LEASE_EPOCH_MISMATCH,
        LEASE_SESSION_NOT_BOUND,
        OWNER_PROOF_DEADLINE_NOT_CLOSED,
        OWNER_PROOF_OBSERVATION_INVALID,
        OWNER_PROOF_ACCEPTED
    }

    public static Decision evaluate(final CheckpointUploadIntentV1 pending, final CheckpointReapingOwnerProof proof) {
        Objects.requireNonNull(pending, "pending");
        Objects.requireNonNull(proof, "proof");
        if (pending.state() != CheckpointUploadStateV1.PENDING_UPLOAD) {
            return Decision.PENDING_INTENT_NOT_PENDING;
        }
        if (!Bytes.constantTimeEquals(pending.intentDigest(), proof.pendingIntentDigest())) {
            return Decision.PENDING_INTENT_MISMATCH;
        }
        if (!pending.owner().equals(proof.owner())) {
            return Decision.OWNER_IDENTITY_MISMATCH;
        }
        if (!Bytes.constantTimeEquals(pending.sourceStoreIncarnation(), proof.sourceStoreIncarnation())) {
            return Decision.STORE_INCARNATION_MISMATCH;
        }
        if (!pending.shard().shardId().equals(proof.recordedLease().shardId())) {
            return Decision.LEASE_SHARD_MISMATCH;
        }
        if (pending.owner().ownerEpoch() != proof.recordedLease().ownerEpoch()) {
            return Decision.LEASE_EPOCH_MISMATCH;
        }
        if (proof.recordedLease().context() == null) {
            return Decision.LEASE_SESSION_NOT_BOUND;
        }
        try {
            proof.observedAt().requireEarliestAtLeast(pending.uploadDeadlineEpochMs());
        } catch (IllegalArgumentException boundaryFailure) {
            return Decision.OWNER_PROOF_DEADLINE_NOT_CLOSED;
        }
        if (proof.kind() == CheckpointReapingOwnerProof.Kind.EXACT_OWNER_EXPLICIT_ABANDON
                && proof.observedCurrentLease() != null) {
            return Decision.OWNER_PROOF_OBSERVATION_INVALID;
        }
        if (proof.observedCurrentLease() != null && proof.recordedLease().sameIdentity(proof.observedCurrentLease())) {
            return Decision.OWNER_PROOF_OBSERVATION_INVALID;
        }
        return Decision.OWNER_PROOF_ACCEPTED;
    }

    public static void require(final CheckpointUploadIntentV1 pending, final CheckpointReapingOwnerProof proof) {
        final Decision decision = evaluate(pending, proof);
        if (decision != Decision.OWNER_PROOF_ACCEPTED) {
            throw new IllegalStateException("checkpoint reaping Owner proof rejected: " + decision);
        }
    }
}
