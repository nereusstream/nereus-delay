package com.nereusstream.delay.store;

import com.nereusstream.delay.ownership.OwnerLease;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import java.util.Objects;

/**
 * Typed evidence that the Owner recorded in a pending checkpoint intent has
 * stopped being an authority for that intent.
 *
 * <p>This value is a receipt, not an authority. The
 * {@link CheckpointReapingOwnerProofIssuer} is the composition boundary that
 * must obtain the result from the Owner Lease/session authority. The receipt
 * nevertheless carries the complete recorded lease identity so a reaper
 * cannot replace it with an epoch-only or deadline-only claim.</p>
 */
public record CheckpointReapingOwnerProof(
        byte[] pendingIntentDigest,
        OwnerIdentity owner,
        byte[] sourceStoreIncarnation,
        OwnerLease recordedLease,
        Kind kind,
        OwnerLease observedCurrentLease,
        TrustedUtcIntervalEvidence observedAt) {
    private static final int DIGEST_LENGTH = 32;
    private static final int STORE_INCARNATION_LENGTH = 16;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-checkpoint-reaping-owner-proof\0");

    public enum Kind {
        EXACT_OWNER_EXPLICIT_ABANDON(1),
        RECORDED_OWNER_NOT_CURRENT(2);

        private final int wireValue;

        Kind(final int wireValue) {
            this.wireValue = wireValue;
        }

        int wireValue() {
            return wireValue;
        }
    }

    public CheckpointReapingOwnerProof {
        Bytes.requireLength(pendingIntentDigest, DIGEST_LENGTH, "pendingIntentDigest");
        owner = Objects.requireNonNull(owner, "owner");
        requireNonZero(sourceStoreIncarnation, STORE_INCARNATION_LENGTH, "sourceStoreIncarnation");
        recordedLease = Objects.requireNonNull(recordedLease, "recordedLease");
        if (recordedLease.context() == null) {
            throw new IllegalArgumentException("reaping owner proof requires a session-bound lease");
        }
        kind = Objects.requireNonNull(kind, "kind");
        observedAt = Objects.requireNonNull(observedAt, "observedAt");
        if (kind == Kind.EXACT_OWNER_EXPLICIT_ABANDON && observedCurrentLease != null) {
            throw new IllegalArgumentException("explicit Owner abandonment cannot carry a current lease");
        }
        if (observedCurrentLease != null) {
            if (!recordedLease.shardId().equals(observedCurrentLease.shardId())) {
                throw new IllegalArgumentException("observed current lease belongs to another shard");
            }
            if (recordedLease.sameIdentity(observedCurrentLease)) {
                throw new IllegalArgumentException("Owner proof observed the recorded lease as current");
            }
        }
        pendingIntentDigest = Bytes.copy(pendingIntentDigest);
        sourceStoreIncarnation = Bytes.copy(sourceStoreIncarnation);
    }

    @Override
    public byte[] pendingIntentDigest() {
        return Bytes.copy(pendingIntentDigest);
    }

    @Override
    public byte[] sourceStoreIncarnation() {
        return Bytes.copy(sourceStoreIncarnation);
    }

    /** Canonical evidence bytes used to bind the proof to one observed lease transition. */
    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, pendingIntentDigest);
            CanonicalProtobuf.bytes(output, 2, owner.canonicalBytes());
            CanonicalProtobuf.bytes(output, 3, sourceStoreIncarnation);
            CanonicalProtobuf.bytes(output, 4, canonicalLease(recordedLease));
            CanonicalProtobuf.uint32(output, 5, kind.wireValue());
            if (observedCurrentLease != null) {
                CanonicalProtobuf.bytes(output, 6, canonicalLease(observedCurrentLease));
            }
            CanonicalProtobuf.bytes(output, 7, observedAt.canonicalBytes());
        });
    }

    /** Stable identifier that a quiescence proof can bind to this exact Owner observation. */
    public byte[] proofDigest() {
        return Bytes.sha256(DIGEST_DOMAIN, canonicalBytes());
    }

    private static byte[] canonicalLease(final OwnerLease lease) {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(
                    output, 1, lease.shardId().routeIncarnation().bytes());
            CanonicalProtobuf.uint32Bits(output, 2, lease.shardId().partition());
            CanonicalProtobuf.bytes(output, 3, Bytes.utf8(lease.ownerId()));
            CanonicalProtobuf.uint64Bits(output, 4, lease.ownerEpoch());
            CanonicalProtobuf.bytes(output, 5, lease.leaseToken());
            CanonicalProtobuf.int64(output, 6, lease.expiresAtEpochMs());
            CanonicalProtobuf.uint32(output, 7, lease.state().wireValue());
            CanonicalProtobuf.bytes(output, 8, CanonicalProtobuf.message(context -> {
                CanonicalProtobuf.bytes(context, 1, lease.context().sourceAssignmentId());
                CanonicalProtobuf.uint64Bits(context, 2, lease.context().assignmentEpoch());
                CanonicalProtobuf.bytes(context, 3, lease.context().sessionIdentity());
            }));
        });
    }

    private static void requireNonZero(final byte[] value, final int length, final String name) {
        Bytes.requireLength(value, length, name);
        for (byte element : value) {
            if (element != 0) {
                return;
            }
        }
        throw new IllegalArgumentException(name + " must be non-zero");
    }
}
