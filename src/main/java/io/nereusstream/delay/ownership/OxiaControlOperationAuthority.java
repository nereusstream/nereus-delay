package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.ControlOperationQueryResponseV1;
import io.nereusstream.delay.protocol.ControlOperationReceiptV1;
import io.nereusstream.delay.protocol.CurrentControlOperationV1;

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
    public ControlOperationQueryResponseV1 register(final ControlOperationReceiptV1 receipt,
                                                     final CurrentControlOperationV1 initial) {
        final ControlOperationQueryResponseV1 response = Objects.requireNonNull(backend.register(
                Objects.requireNonNull(receipt, "receipt"), Objects.requireNonNull(initial, "initial")),
                "Oxia register response");
        validateCurrent(response, receipt, initial.operationRevision());
        return response;
    }

    @Override
    public ControlOperationQueryResponseV1 advance(final ControlOperationReceiptV1 receipt,
                                                    final long expectedRevision,
                                                    final CurrentControlOperationV1 next) {
        if (expectedRevision <= 0) {
            throw new IllegalArgumentException("expectedRevision must be positive");
        }
        final ControlOperationQueryResponseV1 response = Objects.requireNonNull(backend.advance(
                Objects.requireNonNull(receipt, "receipt"), expectedRevision,
                Objects.requireNonNull(next, "next")), "Oxia advance response");
        validateCurrent(response, receipt, next.operationRevision());
        return response;
    }

    @Override
    public ControlOperationQueryResponseV1 query(final ControlOperationReceiptV1 receipt, final long nowEpochMs) {
        if (nowEpochMs < 0) {
            throw new IllegalArgumentException("nowEpochMs must be non-negative");
        }
        final ControlOperationQueryResponseV1 response = Objects.requireNonNull(backend.query(
                Objects.requireNonNull(receipt, "receipt"), nowEpochMs), "Oxia query response");
        validateCurrent(response, receipt, -1);
        return response;
    }

    private static void validateCurrent(final ControlOperationQueryResponseV1 response,
                                        final ControlOperationReceiptV1 receipt,
                                        final long expectedRevision) {
        if (response.resultKind() != io.nereusstream.delay.protocol.ControlOperationQueryResultV1.CURRENT) {
            return;
        }
        final CurrentControlOperationV1 current = response.current();
        if (!Bytes.constantTimeEquals(receipt.operationId(), current.operationId())
                || !Bytes.constantTimeEquals(receipt.requestHash(), current.requestHash())
                || !Bytes.constantTimeEquals(receipt.authenticatedScopeHash(), current.authenticatedScopeHash())) {
            throw new IllegalStateException("Oxia response is not bound to the requested operation");
        }
        if (expectedRevision > 0 && current.operationRevision() < expectedRevision) {
            throw new IllegalStateException("Oxia response regressed operation revision");
        }
    }

    /** Minimal response/CAS surface implemented by the real Oxia client. */
    public interface CasBackend {
        ControlOperationQueryResponseV1 register(ControlOperationReceiptV1 receipt,
                                                  CurrentControlOperationV1 initial);

        ControlOperationQueryResponseV1 advance(ControlOperationReceiptV1 receipt, long expectedRevision,
                                                 CurrentControlOperationV1 next);

        ControlOperationQueryResponseV1 query(ControlOperationReceiptV1 receipt, long nowEpochMs);
    }

    private static final class DelegatingBackend implements CasBackend {
        private final ControlOperationAuthority delegate;

        private DelegatingBackend(final ControlOperationAuthority delegate) {
            this.delegate = Objects.requireNonNull(delegate, "backend");
        }

        @Override
        public ControlOperationQueryResponseV1 register(final ControlOperationReceiptV1 receipt,
                                                         final CurrentControlOperationV1 initial) {
            return delegate.register(receipt, initial);
        }

        @Override
        public ControlOperationQueryResponseV1 advance(final ControlOperationReceiptV1 receipt,
                                                        final long expectedRevision,
                                                        final CurrentControlOperationV1 next) {
            return delegate.advance(receipt, expectedRevision, next);
        }

        @Override
        public ControlOperationQueryResponseV1 query(final ControlOperationReceiptV1 receipt,
                                                    final long nowEpochMs) {
            return delegate.query(receipt, nowEpochMs);
        }
    }
}
