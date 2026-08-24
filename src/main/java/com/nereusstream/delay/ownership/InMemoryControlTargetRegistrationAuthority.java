package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.ControlTargetMutationBindingV1;
import com.nereusstream.delay.protocol.ControlTargetRefV1;
import com.nereusstream.delay.protocol.PreparedControlOperationV1;
import com.nereusstream.delay.protocol.SystemMutation;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Deterministic local model for immutable Control target registration. */
public final class InMemoryControlTargetRegistrationAuthority implements ControlTargetRegistrationAuthority {
    private final Map<String, PreparedControlOperationV1> operations = new HashMap<>();

    @Override
    public synchronized RegistrationResult register(final PreparedControlOperationV1 prepared) {
        Objects.requireNonNull(prepared, "prepared");
        final String key = key(prepared.operationId());
        final PreparedControlOperationV1 existing = operations.get(key);
        if (existing == null) {
            operations.put(key, prepared);
            return RegistrationResult.RECORDED;
        }
        if (!Bytes.constantTimeEquals(existing.canonicalBytes(), prepared.canonicalBytes())) {
            throw new IllegalArgumentException("Control operation ID is already registered with different bytes");
        }
        return RegistrationResult.ALREADY_RECORDED;
    }

    @Override
    public synchronized Optional<PreparedControlOperationV1> find(final byte[] operationId) {
        Objects.requireNonNull(operationId, "operationId");
        return Optional.ofNullable(operations.get(key(operationId)));
    }

    @Override
    public synchronized void validateMutation(
            final PreparedControlOperationV1 prepared, final ControlTargetRefV1 target, final SystemMutation mutation) {
        Objects.requireNonNull(prepared, "prepared");
        final PreparedControlOperationV1 registered = operations.get(key(prepared.operationId()));
        if (registered == null || !Bytes.constantTimeEquals(registered.canonicalBytes(), prepared.canonicalBytes())) {
            throw new IllegalArgumentException("Control operation has not been registered exactly");
        }
        ControlTargetMutationBindingV1.validate(registered, target, mutation);
    }

    private static String key(final byte[] operationId) {
        Bytes.requireLength(operationId, 32, "operationId");
        boolean nonZero = false;
        for (byte value : operationId) {
            nonZero |= value != 0;
        }
        if (!nonZero) {
            throw new IllegalArgumentException("operationId must be non-zero");
        }
        return Bytes.hex(operationId);
    }
}
