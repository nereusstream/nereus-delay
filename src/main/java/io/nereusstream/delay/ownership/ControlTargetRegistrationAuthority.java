package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.ControlTargetRefV1;
import io.nereusstream.delay.protocol.PreparedControlOperationV1;
import io.nereusstream.delay.protocol.SystemMutation;

import java.util.Optional;

/**
 * Authority seam for immutable per-operation Control target registration.
 * Production implementations are expected to back it with one Oxia
 * transaction; the local implementation is only a deterministic test model.
 */
public interface ControlTargetRegistrationAuthority {
    RegistrationResult register(PreparedControlOperationV1 prepared);

    Optional<PreparedControlOperationV1> find(byte[] operationId);

    void validateMutation(PreparedControlOperationV1 prepared, ControlTargetRefV1 target,
                          SystemMutation mutation);

    enum RegistrationResult {
        RECORDED,
        ALREADY_RECORDED
    }
}
