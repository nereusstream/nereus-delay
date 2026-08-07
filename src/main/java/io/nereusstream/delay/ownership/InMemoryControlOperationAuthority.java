package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.ControlOperationQueryResponseV1;
import io.nereusstream.delay.protocol.ControlOperationReceiptV1;
import io.nereusstream.delay.protocol.ControlOperationStateTransitionV1;
import io.nereusstream.delay.protocol.CurrentControlOperationV1;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Deterministic local CAS model for Control Operation state.
 *
 * <p>This is a test/embedded authority only.  It intentionally keeps the
 * complete receipt alongside the current projection so a later query cannot
 * accept a reused operation ID with different request, scope, target or
 * retention bytes.</p>
 */
public final class InMemoryControlOperationAuthority implements ControlOperationAuthority {
    private final Map<String, Entry> operations = new HashMap<>();

    @Override
    public synchronized ControlOperationQueryResponseV1 register(final ControlOperationReceiptV1 receipt,
                                                                  final CurrentControlOperationV1 initial) {
        Objects.requireNonNull(receipt, "receipt");
        Objects.requireNonNull(initial, "initial");
        if (!matchesIdentity(receipt, initial)) {
            return ControlOperationQueryResponseV1.integrityError();
        }
        if (initial.operationRevision() != receipt.operationRevision()) {
            return ControlOperationQueryResponseV1.integrityError();
        }
        final String key = key(receipt.operationId());
        final Entry existing = operations.get(key);
        if (existing == null) {
            operations.put(key, new Entry(receipt, initial));
            return ControlOperationQueryResponseV1.current(initial);
        }
        if (!existing.receipt().equals(receipt)) {
            return ControlOperationQueryResponseV1.notFoundOrNotAuthorized();
        }
        if (existing.current().equals(initial)) {
            return ControlOperationQueryResponseV1.current(existing.current());
        }
        return ControlOperationQueryResponseV1.integrityError();
    }

    @Override
    public synchronized ControlOperationQueryResponseV1 advance(final ControlOperationReceiptV1 receipt,
                                                                 final long expectedRevision,
                                                                 final CurrentControlOperationV1 next) {
        Objects.requireNonNull(receipt, "receipt");
        Objects.requireNonNull(next, "next");
        if (expectedRevision <= 0) {
            return ControlOperationQueryResponseV1.invalidReceipt();
        }
        final Entry existing = operations.get(key(receipt.operationId()));
        if (existing == null || !existing.receipt().equals(receipt)) {
            return ControlOperationQueryResponseV1.notFoundOrNotAuthorized();
        }
        if (!matchesIdentity(receipt, next)) {
            return ControlOperationQueryResponseV1.integrityError();
        }
        try {
            ControlOperationStateTransitionV1.validate(existing.current().state(), next.state());
            ControlOperationStateTransitionV1.validateTargets(existing.current().targetStates(), next.targetStates());
        } catch (IllegalArgumentException invalidTransition) {
            return ControlOperationQueryResponseV1.integrityError();
        }
        if (existing.current().equals(next)) {
            if (!isExactSuccessor(expectedRevision, next.operationRevision())) {
                return ControlOperationQueryResponseV1.integrityError();
            }
            // The original CAS may have committed before its response was
            // lost. An exact CURRENT reread is the only success that this
            // bounded current-only authority can prove idempotently.
            return ControlOperationQueryResponseV1.current(existing.current());
        }
        if (expectedRevision != existing.current().operationRevision()) {
            return ControlOperationQueryResponseV1.integrityError();
        }
        if (!isExactSuccessor(expectedRevision, next.operationRevision())) {
            return ControlOperationQueryResponseV1.integrityError();
        }
        operations.put(key(receipt.operationId()), new Entry(receipt, next));
        return ControlOperationQueryResponseV1.current(next);
    }

    @Override
    public synchronized ControlOperationQueryResponseV1 query(final ControlOperationReceiptV1 receipt,
                                                               final long nowEpochMs) {
        Objects.requireNonNull(receipt, "receipt");
        if (nowEpochMs < 0) {
            return ControlOperationQueryResponseV1.invalidReceipt();
        }
        final Entry entry = operations.get(key(receipt.operationId()));
        if (entry == null || !entry.receipt().equals(receipt)) {
            return ControlOperationQueryResponseV1.notFoundOrNotAuthorized();
        }
        if (nowEpochMs > receipt.queryUntilEpochMs()) {
            return ControlOperationQueryResponseV1.notFoundOrNotAuthorized();
        }
        return ControlOperationQueryResponseV1.current(entry.current());
    }

    private static boolean matchesIdentity(final ControlOperationReceiptV1 receipt,
                                           final CurrentControlOperationV1 current) {
        return Bytes.constantTimeEquals(receipt.operationId(), current.operationId())
                && Bytes.constantTimeEquals(receipt.requestHash(), current.requestHash())
                && Bytes.constantTimeEquals(receipt.authenticatedScopeHash(), current.authenticatedScopeHash());
    }

    private static String key(final byte[] operationId) {
        Objects.requireNonNull(operationId, "operationId");
        return Bytes.hex(operationId);
    }

    /**
     * Checks a revision successor without ever evaluating a wrapping
     * {@code expectedRevision + 1}.  Control revisions are positive signed
     * Java values representing the V1 unsigned range that this local model
     * can safely materialize; {@link Long#MAX_VALUE} therefore has no valid
     * successor and must fail closed.
     */
    private static boolean isExactSuccessor(final long expectedRevision, final long nextRevision) {
        return expectedRevision > 0
                && expectedRevision < Long.MAX_VALUE
                && nextRevision == expectedRevision + 1;
    }

    private record Entry(ControlOperationReceiptV1 receipt, CurrentControlOperationV1 current) {
        private Entry {
            Objects.requireNonNull(receipt, "receipt");
            Objects.requireNonNull(current, "current");
        }
    }
}
