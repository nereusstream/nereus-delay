package com.nereusstream.delay.ownership;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.ControlOperationQueryResponse;
import com.nereusstream.delay.protocol.ControlOperationQueryResult;
import com.nereusstream.delay.protocol.ControlOperationReceipt;
import com.nereusstream.delay.protocol.ControlOperationState;
import com.nereusstream.delay.protocol.CurrentControlOperation;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import java.util.List;
import org.junit.jupiter.api.Test;

class ControlOperationAuthorityTest {
    @Test
    void registerIsIdempotentAndAdvanceRequiresExactRevision() {
        final ControlOperationReceipt receipt = receipt(1, 4_000);
        final CurrentControlOperation initial = current(receipt, 1, ControlOperationState.PENDING);
        final InMemoryControlOperationAuthority authority = new InMemoryControlOperationAuthority();

        assertEquals(
                ControlOperationQueryResult.CURRENT,
                authority.register(receipt, initial).resultKind());
        assertEquals(initial, authority.register(receipt, initial).current());

        final CurrentControlOperation dispatching = current(receipt, 2, ControlOperationState.DISPATCHING);
        assertEquals(
                ControlOperationQueryResult.CURRENT,
                authority.advance(receipt, 1, dispatching).resultKind());
        assertEquals(
                ControlOperationQueryResult.CURRENT,
                authority.advance(receipt, 1, dispatching).resultKind());
        final CurrentControlOperation next = current(receipt, 3, ControlOperationState.IN_PROGRESS);
        assertEquals(
                ControlOperationQueryResult.CURRENT,
                authority.advance(receipt, 2, next).resultKind());
        assertEquals(
                ControlOperationQueryResult.INTEGRITY_ERROR,
                authority
                        .advance(receipt, 3, current(receipt, 5, ControlOperationState.IN_PROGRESS))
                        .resultKind());
        assertEquals(
                ControlOperationQueryResult.INTEGRITY_ERROR,
                authority
                        .advance(receipt, 2, current(receipt, 4, ControlOperationState.IN_PROGRESS))
                        .resultKind());
        assertEquals(next, authority.query(receipt, 2_000).current());
    }

    @Test
    void operationStateCannotRollbackAfterDispatchOrEffect() {
        final ControlOperationReceipt receipt = receipt(9, 4_000);
        final InMemoryControlOperationAuthority authority = new InMemoryControlOperationAuthority();
        authority.register(receipt, current(receipt, 1, ControlOperationState.PENDING));
        assertEquals(
                ControlOperationQueryResult.CURRENT,
                authority
                        .advance(receipt, 1, current(receipt, 2, ControlOperationState.DISPATCHING))
                        .resultKind());
        assertEquals(
                ControlOperationQueryResult.CURRENT,
                authority
                        .advance(receipt, 2, current(receipt, 3, ControlOperationState.IN_PROGRESS))
                        .resultKind());
        assertEquals(
                ControlOperationQueryResult.INTEGRITY_ERROR,
                authority
                        .advance(receipt, 3, current(receipt, 4, ControlOperationState.FAILED_BEFORE_EFFECT))
                        .resultKind());
    }

    @Test
    void queryUsesCompleteReceiptAndFixedRetentionBoundary() {
        final ControlOperationReceipt receipt = receipt(2, 4_000);
        final CurrentControlOperation initial = current(receipt, 1, ControlOperationState.PENDING);
        final InMemoryControlOperationAuthority authority = new InMemoryControlOperationAuthority();
        authority.register(receipt, initial);

        final byte[] alteredScope = receipt.authenticatedScopeHash();
        alteredScope[0]++;
        final ControlOperationReceipt wrongScope = ControlOperationReceipt.create(
                receipt.operationId(),
                receipt.requestHash(),
                alteredScope,
                receipt.targetSnapshotHash(),
                receipt.operationRevision(),
                receipt.registeredAt(),
                receipt.queryUntilEpochMs());
        assertEquals(
                ControlOperationQueryResult.NOT_FOUND_OR_NOT_AUTHORIZED,
                authority.query(wrongScope, 2_000).resultKind());
        assertEquals(
                ControlOperationQueryResult.NOT_FOUND_OR_NOT_AUTHORIZED,
                authority.query(receipt, 4_001).resultKind());
        assertEquals(
                ControlOperationQueryResult.INVALID_RECEIPT,
                authority.query(receipt, -1).resultKind());
        assertEquals(
                ControlOperationQueryResult.INVALID_RECEIPT,
                authority.query(null, 2_000).resultKind());
    }

    @Test
    void projectionIdentityMismatchIsAClosedIntegrityError() {
        final ControlOperationReceipt receipt = receipt(5, 4_000);
        final InMemoryControlOperationAuthority authority = new InMemoryControlOperationAuthority();
        final byte[] otherOperation = receipt.operationId();
        otherOperation[0]++;
        final CurrentControlOperation wrong = new CurrentControlOperation(
                otherOperation,
                receipt.requestHash(),
                receipt.authenticatedScopeHash(),
                ControlOperationState.PENDING,
                1,
                List.of(),
                null);
        assertEquals(
                ControlOperationQueryResult.INTEGRITY_ERROR,
                authority.register(receipt, wrong).resultKind());
    }

    @Test
    void revisionSuccessorFailsClosedBeforeLongWraparound() {
        final ControlOperationReceipt receipt = receipt(10, Long.MAX_VALUE, 4_000);
        final InMemoryControlOperationAuthority authority = new InMemoryControlOperationAuthority();
        final CurrentControlOperation terminal = current(receipt, Long.MAX_VALUE, ControlOperationState.REJECTED);
        authority.register(receipt, terminal);

        assertEquals(
                ControlOperationQueryResult.INTEGRITY_ERROR,
                authority.advance(receipt, Long.MAX_VALUE, terminal).resultKind());
    }

    @Test
    void oxiaAdapterRejectsAResponseBoundToAnotherOperation() {
        final ControlOperationReceipt receipt = receipt(3, 4_000);
        final CurrentControlOperation initial = current(receipt, 1, ControlOperationState.PENDING);
        final ControlOperationReceipt other = receipt(4, 4_000);
        final CurrentControlOperation otherCurrent = current(other, 1, ControlOperationState.PENDING);
        final OxiaControlOperationAuthority authority =
                new OxiaControlOperationAuthority(new OxiaControlOperationAuthority.CasBackend() {
                    @Override
                    public ControlOperationQueryResponse register(
                            final ControlOperationReceipt ignored, final CurrentControlOperation ignoredInitial) {
                        return ControlOperationQueryResponse.current(otherCurrent);
                    }

                    @Override
                    public ControlOperationQueryResponse advance(
                            final ControlOperationReceipt ignored,
                            final long ignoredRevision,
                            final CurrentControlOperation ignoredNext) {
                        return ControlOperationQueryResponse.current(otherCurrent);
                    }

                    @Override
                    public ControlOperationQueryResponse query(
                            final ControlOperationReceipt ignored, final long ignoredNow) {
                        return ControlOperationQueryResponse.current(otherCurrent);
                    }
                });
        assertThrows(IllegalStateException.class, () -> authority.register(receipt, initial));
    }

    @Test
    void oxiaAdvanceRequiresTheExactRequestedCurrentAfterResponseLoss() {
        final ControlOperationReceipt receipt = receipt(6, 4_000);
        final CurrentControlOperation initial = current(receipt, 1, ControlOperationState.PENDING);
        final CurrentControlOperation requested = current(receipt, 2, ControlOperationState.IN_PROGRESS);
        final CurrentControlOperation later = current(receipt, 3, ControlOperationState.REJECTED);
        final OxiaControlOperationAuthority authority =
                new OxiaControlOperationAuthority(new OxiaControlOperationAuthority.CasBackend() {
                    @Override
                    public ControlOperationQueryResponse register(
                            final ControlOperationReceipt ignored, final CurrentControlOperation ignoredInitial) {
                        return ControlOperationQueryResponse.current(initial);
                    }

                    @Override
                    public ControlOperationQueryResponse advance(
                            final ControlOperationReceipt ignored,
                            final long ignoredRevision,
                            final CurrentControlOperation ignoredNext) {
                        return ControlOperationQueryResponse.current(later);
                    }

                    @Override
                    public ControlOperationQueryResponse query(
                            final ControlOperationReceipt ignored, final long ignoredNow) {
                        return ControlOperationQueryResponse.current(later);
                    }
                });
        assertThrows(IllegalStateException.class, () -> authority.advance(receipt, 1, requested));
    }

    @Test
    void oxiaAdapterRejectsMalformedRegisterAndRevisionRequestsBeforeBackendCall() {
        final ControlOperationReceipt receipt = receipt(7, 4_000);
        final CurrentControlOperation wrongIdentity = current(receipt(8, 4_000), 1, ControlOperationState.PENDING);
        final CurrentControlOperation wrongRevision = current(receipt, 3, ControlOperationState.IN_PROGRESS);
        final boolean[] called = {false};
        final OxiaControlOperationAuthority.CasBackend backend = new OxiaControlOperationAuthority.CasBackend() {
            @Override
            public ControlOperationQueryResponse register(
                    final ControlOperationReceipt ignored, final CurrentControlOperation ignoredInitial) {
                called[0] = true;
                return ControlOperationQueryResponse.current(ignoredInitial);
            }

            @Override
            public ControlOperationQueryResponse advance(
                    final ControlOperationReceipt ignored,
                    final long ignoredRevision,
                    final CurrentControlOperation ignoredNext) {
                called[0] = true;
                return ControlOperationQueryResponse.current(ignoredNext);
            }

            @Override
            public ControlOperationQueryResponse query(final ControlOperationReceipt ignored, final long ignoredNow) {
                called[0] = true;
                return ControlOperationQueryResponse.notFoundOrNotAuthorized();
            }
        };
        final OxiaControlOperationAuthority authority = new OxiaControlOperationAuthority(backend);
        assertThrows(IllegalArgumentException.class, () -> authority.register(receipt, wrongIdentity));
        assertThrows(IllegalArgumentException.class, () -> authority.advance(receipt, 1, wrongRevision));
        assertEquals(
                ControlOperationQueryResult.INVALID_RECEIPT,
                authority.query(null, 2_000).resultKind());
        assertEquals(
                ControlOperationQueryResult.INVALID_RECEIPT,
                authority.query(receipt, -1).resultKind());
        assertFalse(called[0]);
    }

    private static ControlOperationReceipt receipt(final int seed, final long queryUntil) {
        return receipt(seed, 1, queryUntil);
    }

    private static ControlOperationReceipt receipt(final int seed, final long revision, final long queryUntil) {
        final byte[] operation = bytes(32, seed);
        final TrustedUtcIntervalEvidence registered = new TrustedUtcIntervalEvidence(
                1_000,
                1_100,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("control-clock" + seed),
                1,
                1,
                1,
                bytes(32, seed + 10),
                0,
                null);
        return ControlOperationReceipt.create(
                operation,
                bytes(32, seed + 1),
                bytes(32, seed + 2),
                bytes(32, seed + 3),
                revision,
                registered,
                queryUntil);
    }

    private static CurrentControlOperation current(
            final ControlOperationReceipt receipt, final long revision, final ControlOperationState state) {
        return new CurrentControlOperation(
                receipt.operationId(),
                receipt.requestHash(),
                receipt.authenticatedScopeHash(),
                state,
                revision,
                List.of(),
                null);
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < value.length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
