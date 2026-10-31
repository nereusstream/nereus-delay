package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.ControlTargetRef;
import com.nereusstream.delay.protocol.PreparedControlOperation;
import com.nereusstream.delay.protocol.SystemMutation;
import java.util.Optional;

/**
 * Authority seam for immutable per-operation Control target registration.
 * Production implementations are expected to back it with one Oxia
 * transaction; the local implementation is only a deterministic test model.
 */
public interface ControlTargetRegistrationAuthority {
    RegistrationResult register(PreparedControlOperation prepared);

    Optional<PreparedControlOperation> find(byte[] operationId);

    void validateMutation(PreparedControlOperation prepared, ControlTargetRef target, SystemMutation mutation);

    default long scopedMutationReservationBytes() {
        return 0;
    }

    default TargetSloControlScopeAuthority.MemberHold admitScopedMutation(
            PreparedControlOperation prepared,
            ControlTargetRef target,
            SystemMutation mutation,
            long maximumRetainedBytes,
            Runnable guard) {
        throw new IllegalStateException("original E4 Scope admission is not configured");
    }
    /** Acquire outside Store locks; partial failures must retain their original Cleanup handle. */
    default TargetSloControlScopeAuthority.MemberHold captureScopedMutation(
            PreparedControlOperation prepared,
            ControlTargetRef target,
            SystemMutation mutation,
            long maximumRetainedBytes,
            Runnable guard) {
        throw new IllegalStateException("original E4 Scope membership is not available");
    }
    /** Local frozen MEMBER proof only. An ordinary point lookup never satisfies this method. */
    default void validateScopedMutation(
            PreparedControlOperation prepared, ControlTargetRef target, SystemMutation mutation) {
        throw new IllegalStateException("original E4 Scope membership was not frozen for Source use");
    }

    enum RegistrationResult {
        RECORDED,
        ALREADY_RECORDED
    }
}
