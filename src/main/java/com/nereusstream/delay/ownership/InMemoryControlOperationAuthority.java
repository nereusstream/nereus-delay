package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.ControlOperationQueryResponse;
import com.nereusstream.delay.protocol.ControlOperationReceipt;
import com.nereusstream.delay.protocol.ControlOperationStateTransition;
import com.nereusstream.delay.protocol.CurrentControlOperation;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Deterministic local CAS model for Control Operation state.
 *
 * <p>This is a test/embedded authority only. It intentionally keeps the
 * complete receipt alongside the current projection so a later query cannot
 * accept a reused operation ID with different request, scope, target or
 * retention bytes.</p>
 */
public final class InMemoryControlOperationAuthority implements ControlOperationAuthority {
    private final Map<String, Entry> operations = new HashMap<>();

    @Override
    public synchronized ControlOperationQueryResponse register(
            final ControlOperationReceipt receipt, final CurrentControlOperation initial) {
        Objects.requireNonNull(receipt, "receipt");
        Objects.requireNonNull(initial, "initial");
        if (!matchesIdentity(receipt, initial)) {
            return ControlOperationQueryResponse.integrityError();
        }
        if (initial.operationRevision() != receipt.operationRevision()) {
            return ControlOperationQueryResponse.integrityError();
        }
        final String key = key(receipt.operationId());
        final Entry existing = operations.get(key);
        if (existing == null) {
            operations.put(key, new Entry(receipt, initial));
            return ControlOperationQueryResponse.current(initial);
        }
        if (!existing.receipt().equals(receipt)) {
            return ControlOperationQueryResponse.notFoundOrNotAuthorized();
        }
        if (existing.current().equals(initial)) {
            return ControlOperationQueryResponse.current(existing.current());
        }
        return ControlOperationQueryResponse.integrityError();
    }

    @Override
    public synchronized ControlOperationQueryResponse advance(
            final ControlOperationReceipt receipt, final long expectedRevision, final CurrentControlOperation next) {
        Objects.requireNonNull(receipt, "receipt");
        Objects.requireNonNull(next, "next");
        if (expectedRevision <= 0) {
            return ControlOperationQueryResponse.invalidReceipt();
        }
        final Entry existing = operations.get(key(receipt.operationId()));
        if (existing == null || !existing.receipt().equals(receipt)) {
            return ControlOperationQueryResponse.notFoundOrNotAuthorized();
        }
        if (!matchesIdentity(receipt, next)) {
            return ControlOperationQueryResponse.integrityError();
        }
        try {
            ControlOperationStateTransition.validate(existing.current().state(), next.state());
            ControlOperationStateTransition.validateTargets(existing.current().targetStates(), next.targetStates());
        } catch (IllegalArgumentException invalidTransition) {
            return ControlOperationQueryResponse.integrityError();
        }
        if (existing.current().equals(next)) {
            if (!isExactSuccessor(expectedRevision, next.operationRevision())) {
                return ControlOperationQueryResponse.integrityError();
            }
            // The original CAS may have committed before its response was
            // lost. An exact CURRENT reread is the only success that this
            // bounded current-only authority can prove idempotently.
            return ControlOperationQueryResponse.current(existing.current());
        }
        if (expectedRevision != existing.current().operationRevision()) {
            return ControlOperationQueryResponse.integrityError();
        }
        if (!isExactSuccessor(expectedRevision, next.operationRevision())) {
            return ControlOperationQueryResponse.integrityError();
        }
        operations.put(key(receipt.operationId()), new Entry(receipt, next));
        return ControlOperationQueryResponse.current(next);
    }

    @Override
    public synchronized ControlOperationQueryResponse query(
            final ControlOperationReceipt receipt, final long nowEpochMs) {
        if (receipt == null || nowEpochMs < 0) {
            return ControlOperationQueryResponse.invalidReceipt();
        }
        final Entry entry = operations.get(key(receipt.operationId()));
        if (entry == null || !entry.receipt().equals(receipt)) {
            return ControlOperationQueryResponse.notFoundOrNotAuthorized();
        }
        if (nowEpochMs > receipt.queryUntilEpochMs()) {
            return ControlOperationQueryResponse.notFoundOrNotAuthorized();
        }
        return ControlOperationQueryResponse.current(entry.current());
    }

    private static boolean matchesIdentity(
            final ControlOperationReceipt receipt, final CurrentControlOperation current) {
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
     * {@code expectedRevision + 1}. Control revisions are positive signed
     * Java values representing the unsigned range that this local model
     * can safely materialize; {@link Long#MAX_VALUE} therefore has no valid
     * successor and must fail closed.
     */
    private static boolean isExactSuccessor(final long expectedRevision, final long nextRevision) {
        return expectedRevision > 0 && expectedRevision < Long.MAX_VALUE && nextRevision == expectedRevision + 1;
    }

    private record Entry(ControlOperationReceipt receipt, CurrentControlOperation current) {
        private Entry {
            Objects.requireNonNull(receipt, "receipt");
            Objects.requireNonNull(current, "current");
        }
    }
}
