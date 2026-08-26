package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.ActiveLaneState;
import com.nereusstream.delay.protocol.DestinationLaneId;
import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.runtime.LaneRecord;
import java.util.Objects;

/**
 * Strict Lane-scoped activation boundary for an owned Shard.
 *
 * <p>The coordinator asks an injected authority for typed prerequisites only
 * after the owner has entered source catch-up. It then rereads the exact
 * Oxia lease and commits the certificate/channel-backed READY projection in
 * the shard Store. No implementation of this class can turn a boolean into
 * READY or infer live Broker/Profile/credential evidence locally.</p>
 */
public final class LaneActivationCoordinator {
    private final OwnedDelayShard ownedShard;
    private final OxiaOwnerLeaseStore authority;

    public LaneActivationCoordinator(final OwnedDelayShard ownedShard, final OxiaOwnerLeaseStore authority) {
        this.ownedShard = Objects.requireNonNull(ownedShard, "ownedShard");
        this.authority = Objects.requireNonNull(authority, "authority");
    }

    /**
     * Resolves and commits one Lane activation. The authority callback is the
     * integration point for Profile/credential/channel fencing and evidence
     * catch-up; it must return the exact certificate-backed proof.
     */
    public LaneRecord activate(
            final DestinationLaneId laneId, final long nowEpochMs, final PrerequisiteAuthority prerequisiteAuthority) {
        final ActivationRequest request = ownedShard.requireLaneActivationRequest(laneId, nowEpochMs);
        final LaneActivationPrerequisites prerequisites = Objects.requireNonNull(
                Objects.requireNonNull(prerequisiteAuthority, "prerequisiteAuthority")
                        .prepare(request),
                "activation prerequisites");
        prerequisites.requireCurrentAt(nowEpochMs);
        return ownedShard.activateLaneAuthoritatively(authority, laneId, prerequisites, nowEpochMs);
    }

    @FunctionalInterface
    public interface PrerequisiteAuthority {
        LaneActivationPrerequisites prepare(ActivationRequest request);
    }

    /** Exact local identity supplied to the external prerequisite authority. */
    public record ActivationRequest(
            DestinationLaneId laneId,
            byte[] laneIncarnation,
            OwnerIdentity owner,
            byte[] storeIncarnation,
            ActiveLaneState laneState,
            long nowEpochMs) {
        public ActivationRequest {
            Objects.requireNonNull(laneId, "laneId");
            laneIncarnation = com.nereusstream.delay.protocol.Bytes.copy(
                    Objects.requireNonNull(laneIncarnation, "laneIncarnation"));
            owner = Objects.requireNonNull(owner, "owner");
            storeIncarnation = com.nereusstream.delay.protocol.Bytes.copy(
                    Objects.requireNonNull(storeIncarnation, "storeIncarnation"));
            laneState = Objects.requireNonNull(laneState, "laneState");
            if (!laneId.equals(laneState.laneId())
                    || !java.util.Arrays.equals(laneIncarnation, laneState.laneIncarnation())) {
                throw new IllegalArgumentException("Lane activation request identity mismatch");
            }
            if (nowEpochMs < 0) {
                throw new IllegalArgumentException("nowEpochMs must be non-negative");
            }
        }

        @Override
        public byte[] laneIncarnation() {
            return com.nereusstream.delay.protocol.Bytes.copy(laneIncarnation);
        }

        @Override
        public byte[] storeIncarnation() {
            return com.nereusstream.delay.protocol.Bytes.copy(storeIncarnation);
        }
    }
}
