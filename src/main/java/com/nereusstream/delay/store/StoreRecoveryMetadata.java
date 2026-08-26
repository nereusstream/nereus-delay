package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.RecoveryCandidateRef;
import com.nereusstream.delay.protocol.RecoveryFloorRef;
import com.nereusstream.delay.protocol.RecoveryInstallState;
import java.util.Arrays;

/**
 * Local meta/RECOVERY projections for one physical shard DB.
 *
 * <p>Missing projections are intentional for a fresh store. They mean that
 * the local DB has no recovery-reuse proof; callers must not synthesize a
 * candidate from a directory name or an ACTIVE pointer.</p>
 */
public record StoreRecoveryMetadata(
        RecoveryCandidateRef lineageBase,
        RecoveryFloorRef lastObservedFloor,
        long catalogGeneration,
        RecoveryInstallState installState) {
    public StoreRecoveryMetadata {
        if (lastObservedFloor == null && catalogGeneration != 0) {
            throw new IllegalArgumentException("catalog generation requires an observed Recovery Floor");
        }
        if (lastObservedFloor != null && catalogGeneration != lastObservedFloor.catalogGeneration()) {
            throw new IllegalArgumentException("catalog generation does not match observed Recovery Floor");
        }
        if (lineageBase != null
                && lastObservedFloor != null
                && !Arrays.equals(lineageBase.recoveryLineageId(), lastObservedFloor.recoveryLineageId())) {
            throw new IllegalArgumentException("recovery candidate and Floor lineage differ");
        }
    }

    public static StoreRecoveryMetadata empty() {
        return new StoreRecoveryMetadata(null, null, 0, null);
    }

    public StoreRecoveryMetadata withInstallState(final RecoveryInstallState next) {
        return new StoreRecoveryMetadata(lineageBase, lastObservedFloor, catalogGeneration, next);
    }

    public boolean hasReusableProof() {
        return lineageBase != null
                && lastObservedFloor != null
                && installState != null
                && installState.phase() != com.nereusstream.delay.protocol.RecoveryInstallPhase.STAGED
                && installState.phase() != com.nereusstream.delay.protocol.RecoveryInstallPhase.INSTALLED
                && Arrays.equals(lineageBase.recoveryLineageId(), lastObservedFloor.recoveryLineageId());
    }

    public boolean observesFloor(final RecoveryFloorRef floor) {
        if (floor == null || lastObservedFloor == null) {
            return false;
        }
        return Arrays.equals(lastObservedFloor.canonicalBytes(), floor.canonicalBytes());
    }

    public byte[] lineageId() {
        return lineageBase == null ? null : lineageBase.recoveryLineageId();
    }

    public byte[] checkpointId() {
        return lineageBase == null ? null : lineageBase.checkpointId();
    }

    public byte[] manifestSha256() {
        return lineageBase == null ? null : lineageBase.manifestSha256();
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof StoreRecoveryMetadata that
                && same(lineageBase, that.lineageBase)
                && same(lastObservedFloor, that.lastObservedFloor)
                && catalogGeneration == that.catalogGeneration
                && same(installState, that.installState);
    }

    @Override
    public int hashCode() {
        return 31 * (31 * (31 * hash(lineageBase) + hash(lastObservedFloor)) + Long.hashCode(catalogGeneration))
                + hash(installState);
    }

    private static boolean same(final Object left, final Object right) {
        if (left == right) {
            return true;
        }
        if (left == null || right == null) {
            return false;
        }
        if (left instanceof RecoveryCandidateRef candidate && right instanceof RecoveryCandidateRef otherCandidate) {
            return Arrays.equals(candidate.canonicalBytes(), otherCandidate.canonicalBytes());
        }
        if (left instanceof RecoveryFloorRef floor && right instanceof RecoveryFloorRef otherFloor) {
            return Arrays.equals(floor.canonicalBytes(), otherFloor.canonicalBytes());
        }
        if (left instanceof RecoveryInstallState state && right instanceof RecoveryInstallState otherState) {
            return Arrays.equals(state.canonicalBytes(), otherState.canonicalBytes());
        }
        return left.equals(right);
    }

    private static int hash(final Object value) {
        if (value == null) {
            return 0;
        }
        if (value instanceof RecoveryCandidateRef candidate) {
            return Arrays.hashCode(candidate.canonicalBytes());
        }
        if (value instanceof RecoveryFloorRef floor) {
            return Arrays.hashCode(floor.canonicalBytes());
        }
        if (value instanceof RecoveryInstallState state) {
            return Arrays.hashCode(state.canonicalBytes());
        }
        return value.hashCode();
    }
}
