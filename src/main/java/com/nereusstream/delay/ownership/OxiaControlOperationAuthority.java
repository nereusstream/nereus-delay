package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.ControlOperationQueryResponse;
import com.nereusstream.delay.protocol.ControlOperationReceipt;
import com.nereusstream.delay.protocol.CurrentControlOperation;
import java.util.Objects;

/**
 * Validation adapter for the production Oxia Control Operation authority.
 * The backend owns durable routing, authorization-safe lookup and CAS; this
 * class rejects a successful response whose projection is not bound to the
 * requested operation identity.
 */
public final class OxiaControlOperationAuthority implements ControlOperationAuthority {
    private final CasBackend backend;

    public OxiaControlOperationAuthority(final CasBackend backend) {
        this.backend = Objects.requireNonNull(backend, "backend");
    }

    /** Uses the deterministic in-memory authority as an embedded adapter. */
    public OxiaControlOperationAuthority(final ControlOperationAuthority backend) {
        this(new DelegatingBackend(backend));
    }

    @Override
    public ControlOperationQueryResponse register(
            final ControlOperationReceipt receipt, final CurrentControlOperation initial) {
        Objects.requireNonNull(receipt, "receipt");
        Objects.requireNonNull(initial, "initial");
        validateIdentityAndRevision(receipt, initial, receipt.operationRevision(), "initial");
        final ControlOperationQueryResponse response =
                Objects.requireNonNull(backend.register(receipt, initial), "Oxia register response");
        validateCurrent(response, receipt, initial.operationRevision(), null);
        return response;
    }

    @Override
    public ControlOperationQueryResponse advance(
            final ControlOperationReceipt receipt, final long expectedRevision, final CurrentControlOperation next) {
        if (expectedRevision <= 0) {
            throw new IllegalArgumentException("expectedRevision must be positive");
        }
        Objects.requireNonNull(receipt, "receipt");
        Objects.requireNonNull(next, "next");
        if (expectedRevision == Long.MAX_VALUE) {
            throw new IllegalArgumentException("expectedRevision cannot advance past Long.MAX_VALUE");
        }
        validateIdentityAndRevision(receipt, next, Math.addExact(expectedRevision, 1), "next");
        final ControlOperationQueryResponse response =
                Objects.requireNonNull(backend.advance(receipt, expectedRevision, next), "Oxia advance response");
        validateCurrent(response, receipt, next.operationRevision(), next);
        return response;
    }

    @Override
    public ControlOperationQueryResponse query(final ControlOperationReceipt receipt, final long nowEpochMs) {
        if (receipt == null || nowEpochMs < 0) {
            return ControlOperationQueryResponse.invalidReceipt();
        }
        final ControlOperationQueryResponse response =
                Objects.requireNonNull(backend.query(receipt, nowEpochMs), "Oxia query response");
        validateCurrent(response, receipt, -1, null);
        return response;
    }

    private static void validateCurrent(
            final ControlOperationQueryResponse response,
            final ControlOperationReceipt receipt,
            final long expectedRevision,
            final CurrentControlOperation expectedCurrent) {
        if (response.resultKind() != com.nereusstream.delay.protocol.ControlOperationQueryResult.CURRENT) {
            return;
        }
        final CurrentControlOperation current = response.current();
        if (!Bytes.constantTimeEquals(receipt.operationId(), current.operationId())
                || !Bytes.constantTimeEquals(receipt.requestHash(), current.requestHash())
                || !Bytes.constantTimeEquals(receipt.authenticatedScopeHash(), current.authenticatedScopeHash())) {
            throw new IllegalStateException("Oxia response is not bound to the requested operation");
        }
        if (expectedCurrent != null
                && (current.operationRevision() != expectedRevision || !current.equals(expectedCurrent))) {
            throw new IllegalStateException("Oxia response did not prove the requested operation revision");
        }
        if (expectedCurrent == null && expectedRevision > 0 && current.operationRevision() < expectedRevision) {
            throw new IllegalStateException("Oxia response regressed operation revision");
        }
    }

    private static void validateIdentityAndRevision(
            final ControlOperationReceipt receipt,
            final CurrentControlOperation current,
            final long expectedRevision,
            final String name) {
        if (!Bytes.constantTimeEquals(receipt.operationId(), current.operationId())
                || !Bytes.constantTimeEquals(receipt.requestHash(), current.requestHash())
                || !Bytes.constantTimeEquals(receipt.authenticatedScopeHash(), current.authenticatedScopeHash())) {
            throw new IllegalArgumentException(name + " is not bound to the operation receipt");
        }
        if (current.operationRevision() != expectedRevision) {
            throw new IllegalArgumentException(name + " revision must be exactly " + expectedRevision);
        }
    }

    /** Minimal response/CAS surface implemented by the real Oxia client. */
    public interface CasBackend {
        ControlOperationQueryResponse register(ControlOperationReceipt receipt, CurrentControlOperation initial);

        ControlOperationQueryResponse advance(
                ControlOperationReceipt receipt, long expectedRevision, CurrentControlOperation next);

        ControlOperationQueryResponse query(ControlOperationReceipt receipt, long nowEpochMs);
    }

    private static final class DelegatingBackend implements CasBackend {
        private final ControlOperationAuthority delegate;

        private DelegatingBackend(final ControlOperationAuthority delegate) {
            this.delegate = Objects.requireNonNull(delegate, "backend");
        }

        @Override
        public ControlOperationQueryResponse register(
                final ControlOperationReceipt receipt, final CurrentControlOperation initial) {
            return delegate.register(receipt, initial);
        }

        @Override
        public ControlOperationQueryResponse advance(
                final ControlOperationReceipt receipt,
                final long expectedRevision,
                final CurrentControlOperation next) {
            return delegate.advance(receipt, expectedRevision, next);
        }

        @Override
        public ControlOperationQueryResponse query(final ControlOperationReceipt receipt, final long nowEpochMs) {
            return delegate.query(receipt, nowEpochMs);
        }
    }
}
