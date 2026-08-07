package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.ControlTargetMutationBindingV1;
import io.nereusstream.delay.protocol.ControlTargetRefV1;
import io.nereusstream.delay.protocol.PreparedControlOperationV1;
import io.nereusstream.delay.protocol.SystemMutation;

import java.util.Objects;
import java.util.Optional;

/**
 * Validation adapter for the Oxia-backed immutable Control target registry.
 *
 * <p>The backend owns the actual Oxia transaction, conditional response
 * classification and target lookup. This adapter requires an exact reread
 * after registration and never turns a missing or differently encoded value
 * into a successful registration.</p>
 */
public final class OxiaControlTargetRegistrationAuthority implements ControlTargetRegistrationAuthority {
    private final CasBackend backend;

    public OxiaControlTargetRegistrationAuthority(final CasBackend backend) {
        this.backend = Objects.requireNonNull(backend, "backend");
    }

    /** Uses the deterministic in-memory authority as an embedded adapter. */
    public OxiaControlTargetRegistrationAuthority(final ControlTargetRegistrationAuthority backend) {
        this(new DelegatingBackend(backend));
    }

    @Override
    public RegistrationResult register(final PreparedControlOperationV1 prepared) {
        Objects.requireNonNull(prepared, "prepared");
        final RegistrationResult result = Objects.requireNonNull(backend.register(prepared),
                "Oxia target registration result");
        final PreparedControlOperationV1 observed = find(prepared.operationId()).orElseThrow(
                () -> new IllegalStateException("Oxia target registration disappeared after CAS"));
        if (!Bytes.constantTimeEquals(prepared.canonicalBytes(), observed.canonicalBytes())) {
            throw new IllegalStateException("Oxia target registration changed Prepared bytes");
        }
        return result;
    }

    @Override
    public Optional<PreparedControlOperationV1> find(final byte[] operationId) {
        validateOperationId(operationId);
        final byte[] requestedOperationId = Bytes.copy(operationId);
        final byte[] backendOperationId = Bytes.copy(requestedOperationId);
        final Optional<PreparedControlOperationV1> result = Objects.requireNonNull(backend.find(backendOperationId),
                "Oxia target lookup result");
        if (result.isPresent()
                && !Bytes.constantTimeEquals(requestedOperationId, result.orElseThrow().operationId())) {
            throw new IllegalStateException("Oxia target lookup returned another operation");
        }
        return result;
    }

    @Override
    public void validateMutation(final PreparedControlOperationV1 prepared, final ControlTargetRefV1 target,
                                 final SystemMutation mutation) {
        Objects.requireNonNull(prepared, "prepared");
        final PreparedControlOperationV1 registered = find(prepared.operationId()).orElseThrow(
                () -> new IllegalArgumentException("Control operation is not registered in Oxia"));
        if (!Bytes.constantTimeEquals(prepared.canonicalBytes(), registered.canonicalBytes())) {
            throw new IllegalArgumentException("Control operation bytes changed after Oxia registration");
        }
        backend.validateMutation(registered, target, mutation);
        ControlTargetMutationBindingV1.validate(registered, target, mutation);
    }

    private static void validateOperationId(final byte[] operationId) {
        Bytes.requireLength(operationId, 32, "operationId");
        boolean nonZero = false;
        for (byte value : operationId) {
            nonZero |= value != 0;
        }
        if (!nonZero) {
            throw new IllegalArgumentException("operationId must be non-zero");
        }
    }

    /** Minimal immutable-registration/CAS surface implemented by the Oxia client. */
    public interface CasBackend {
        RegistrationResult register(PreparedControlOperationV1 prepared);

        Optional<PreparedControlOperationV1> find(byte[] operationId);

        void validateMutation(PreparedControlOperationV1 prepared, ControlTargetRefV1 target,
                              SystemMutation mutation);
    }

    private static final class DelegatingBackend implements CasBackend {
        private final ControlTargetRegistrationAuthority delegate;

        private DelegatingBackend(final ControlTargetRegistrationAuthority delegate) {
            this.delegate = Objects.requireNonNull(delegate, "backend");
        }

        @Override
        public RegistrationResult register(final PreparedControlOperationV1 prepared) {
            return delegate.register(prepared);
        }

        @Override
        public Optional<PreparedControlOperationV1> find(final byte[] operationId) {
            return delegate.find(operationId);
        }

        @Override
        public void validateMutation(final PreparedControlOperationV1 prepared, final ControlTargetRefV1 target,
                                     final SystemMutation mutation) {
            delegate.validateMutation(prepared, target, mutation);
        }
    }
}
