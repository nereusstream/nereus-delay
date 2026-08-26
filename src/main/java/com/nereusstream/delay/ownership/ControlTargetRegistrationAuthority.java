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

    enum RegistrationResult {
        RECORDED,
        ALREADY_RECORDED
    }
}
