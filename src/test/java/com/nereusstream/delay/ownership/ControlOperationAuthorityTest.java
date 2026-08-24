package com.nereusstream.delay.ownership;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.ControlOperationQueryResponseV1;
import com.nereusstream.delay.protocol.ControlOperationQueryResultV1;
import com.nereusstream.delay.protocol.ControlOperationReceiptV1;
import com.nereusstream.delay.protocol.ControlOperationStateV1;
import com.nereusstream.delay.protocol.CurrentControlOperationV1;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import java.util.List;
import org.junit.jupiter.api.Test;

class ControlOperationAuthorityTest {
    @Test
    void registerIsIdempotentAndAdvanceRequiresExactRevision() {
        final ControlOperationReceiptV1 receipt = receipt(1, 4_000);
        final CurrentControlOperationV1 initial = current(receipt, 1, ControlOperationStateV1.PENDING);
        final InMemoryControlOperationAuthority authority = new InMemoryControlOperationAuthority();

        assertEquals(
                ControlOperationQueryResultV1.CURRENT,
                authority.register(receipt, initial).resultKind());
        assertEquals(initial, authority.register(receipt, initial).current());

        final CurrentControlOperationV1 dispatching = current(receipt, 2, ControlOperationStateV1.DISPATCHING);
        assertEquals(
                ControlOperationQueryResultV1.CURRENT,
                authority.advance(receipt, 1, dispatching).resultKind());
        assertEquals(
                ControlOperationQueryResultV1.CURRENT,
                authority.advance(receipt, 1, dispatching).resultKind());
        final CurrentControlOperationV1 next = current(receipt, 3, ControlOperationStateV1.IN_PROGRESS);
        assertEquals(
                ControlOperationQueryResultV1.CURRENT,
                authority.advance(receipt, 2, next).resultKind());
        assertEquals(
                ControlOperationQueryResultV1.INTEGRITY_ERROR,
                authority
                        .advance(receipt, 3, current(receipt, 5, ControlOperationStateV1.IN_PROGRESS))
                        .resultKind());
        assertEquals(
                ControlOperationQueryResultV1.INTEGRITY_ERROR,
                authority
                        .advance(receipt, 2, current(receipt, 4, ControlOperationStateV1.IN_PROGRESS))
                        .resultKind());
        assertEquals(next, authority.query(receipt, 2_000).current());
    }

    @Test
    void operationStateCannotRollbackAfterDispatchOrEffect() {
        final ControlOperationReceiptV1 receipt = receipt(9, 4_000);
        final InMemoryControlOperationAuthority authority = new InMemoryControlOperationAuthority();
        authority.register(receipt, current(receipt, 1, ControlOperationStateV1.PENDING));
        assertEquals(
                ControlOperationQueryResultV1.CURRENT,
                authority
                        .advance(receipt, 1, current(receipt, 2, ControlOperationStateV1.DISPATCHING))
                        .resultKind());
        assertEquals(
                ControlOperationQueryResultV1.CURRENT,
                authority
                        .advance(receipt, 2, current(receipt, 3, ControlOperationStateV1.IN_PROGRESS))
                        .resultKind());
        assertEquals(
                ControlOperationQueryResultV1.INTEGRITY_ERROR,
                authority
                        .advance(receipt, 3, current(receipt, 4, ControlOperationStateV1.FAILED_BEFORE_EFFECT))
                        .resultKind());
    }

    @Test
    void queryUsesCompleteReceiptAndFixedRetentionBoundary() {
        final ControlOperationReceiptV1 receipt = receipt(2, 4_000);
        final CurrentControlOperationV1 initial = current(receipt, 1, ControlOperationStateV1.PENDING);
        final InMemoryControlOperationAuthority authority = new InMemoryControlOperationAuthority();
        authority.register(receipt, initial);

        final byte[] alteredScope = receipt.authenticatedScopeHash();
        alteredScope[0]++;
        final ControlOperationReceiptV1 wrongScope = ControlOperationReceiptV1.create(
                receipt.operationId(),
                receipt.requestHash(),
                alteredScope,
                receipt.targetSnapshotHash(),
                receipt.operationRevision(),
                receipt.registeredAt(),
                receipt.queryUntilEpochMs());
        assertEquals(
                ControlOperationQueryResultV1.NOT_FOUND_OR_NOT_AUTHORIZED,
                authority.query(wrongScope, 2_000).resultKind());
        assertEquals(
                ControlOperationQueryResultV1.NOT_FOUND_OR_NOT_AUTHORIZED,
                authority.query(receipt, 4_001).resultKind());
        assertEquals(
                ControlOperationQueryResultV1.INVALID_RECEIPT,
                authority.query(receipt, -1).resultKind());
        assertEquals(
                ControlOperationQueryResultV1.INVALID_RECEIPT,
                authority.query(null, 2_000).resultKind());
    }

    @Test
    void projectionIdentityMismatchIsAClosedIntegrityError() {
        final ControlOperationReceiptV1 receipt = receipt(5, 4_000);
        final InMemoryControlOperationAuthority authority = new InMemoryControlOperationAuthority();
        final byte[] otherOperation = receipt.operationId();
        otherOperation[0]++;
        final CurrentControlOperationV1 wrong = new CurrentControlOperationV1(
                otherOperation,
                receipt.requestHash(),
                receipt.authenticatedScopeHash(),
                ControlOperationStateV1.PENDING,
                1,
                List.of(),
                null);
        assertEquals(
                ControlOperationQueryResultV1.INTEGRITY_ERROR,
                authority.register(receipt, wrong).resultKind());
    }

    @Test
    void revisionSuccessorFailsClosedBeforeLongWraparound() {
        final ControlOperationReceiptV1 receipt = receipt(10, Long.MAX_VALUE, 4_000);
        final InMemoryControlOperationAuthority authority = new InMemoryControlOperationAuthority();
        final CurrentControlOperationV1 terminal = current(receipt, Long.MAX_VALUE, ControlOperationStateV1.REJECTED);
        authority.register(receipt, terminal);

        assertEquals(
                ControlOperationQueryResultV1.INTEGRITY_ERROR,
                authority.advance(receipt, Long.MAX_VALUE, terminal).resultKind());
    }

    @Test
    void oxiaAdapterRejectsAResponseBoundToAnotherOperation() {
        final ControlOperationReceiptV1 receipt = receipt(3, 4_000);
        final CurrentControlOperationV1 initial = current(receipt, 1, ControlOperationStateV1.PENDING);
        final ControlOperationReceiptV1 other = receipt(4, 4_000);
        final CurrentControlOperationV1 otherCurrent = current(other, 1, ControlOperationStateV1.PENDING);
        final OxiaControlOperationAuthority authority =
                new OxiaControlOperationAuthority(new OxiaControlOperationAuthority.CasBackend() {
                    @Override
                    public ControlOperationQueryResponseV1 register(
                            final ControlOperationReceiptV1 ignored, final CurrentControlOperationV1 ignoredInitial) {
                        return ControlOperationQueryResponseV1.current(otherCurrent);
                    }

                    @Override
                    public ControlOperationQueryResponseV1 advance(
                            final ControlOperationReceiptV1 ignored,
                            final long ignoredRevision,
                            final CurrentControlOperationV1 ignoredNext) {
                        return ControlOperationQueryResponseV1.current(otherCurrent);
                    }

                    @Override
                    public ControlOperationQueryResponseV1 query(
                            final ControlOperationReceiptV1 ignored, final long ignoredNow) {
                        return ControlOperationQueryResponseV1.current(otherCurrent);
                    }
                });
        assertThrows(IllegalStateException.class, () -> authority.register(receipt, initial));
    }

    @Test
    void oxiaAdvanceRequiresTheExactRequestedCurrentAfterResponseLoss() {
        final ControlOperationReceiptV1 receipt = receipt(6, 4_000);
        final CurrentControlOperationV1 initial = current(receipt, 1, ControlOperationStateV1.PENDING);
        final CurrentControlOperationV1 requested = current(receipt, 2, ControlOperationStateV1.IN_PROGRESS);
        final CurrentControlOperationV1 later = current(receipt, 3, ControlOperationStateV1.REJECTED);
        final OxiaControlOperationAuthority authority =
                new OxiaControlOperationAuthority(new OxiaControlOperationAuthority.CasBackend() {
                    @Override
                    public ControlOperationQueryResponseV1 register(
                            final ControlOperationReceiptV1 ignored, final CurrentControlOperationV1 ignoredInitial) {
                        return ControlOperationQueryResponseV1.current(initial);
                    }

                    @Override
                    public ControlOperationQueryResponseV1 advance(
                            final ControlOperationReceiptV1 ignored,
                            final long ignoredRevision,
                            final CurrentControlOperationV1 ignoredNext) {
                        return ControlOperationQueryResponseV1.current(later);
                    }

                    @Override
                    public ControlOperationQueryResponseV1 query(
                            final ControlOperationReceiptV1 ignored, final long ignoredNow) {
                        return ControlOperationQueryResponseV1.current(later);
                    }
                });
        assertThrows(IllegalStateException.class, () -> authority.advance(receipt, 1, requested));
    }

    @Test
    void oxiaAdapterRejectsMalformedRegisterAndRevisionRequestsBeforeBackendCall() {
        final ControlOperationReceiptV1 receipt = receipt(7, 4_000);
        final CurrentControlOperationV1 wrongIdentity = current(receipt(8, 4_000), 1, ControlOperationStateV1.PENDING);
        final CurrentControlOperationV1 wrongRevision = current(receipt, 3, ControlOperationStateV1.IN_PROGRESS);
        final boolean[] called = {false};
        final OxiaControlOperationAuthority.CasBackend backend = new OxiaControlOperationAuthority.CasBackend() {
            @Override
            public ControlOperationQueryResponseV1 register(
                    final ControlOperationReceiptV1 ignored, final CurrentControlOperationV1 ignoredInitial) {
                called[0] = true;
                return ControlOperationQueryResponseV1.current(ignoredInitial);
            }

            @Override
            public ControlOperationQueryResponseV1 advance(
                    final ControlOperationReceiptV1 ignored,
                    final long ignoredRevision,
                    final CurrentControlOperationV1 ignoredNext) {
                called[0] = true;
                return ControlOperationQueryResponseV1.current(ignoredNext);
            }

            @Override
            public ControlOperationQueryResponseV1 query(
                    final ControlOperationReceiptV1 ignored, final long ignoredNow) {
                called[0] = true;
                return ControlOperationQueryResponseV1.notFoundOrNotAuthorized();
            }
        };
        final OxiaControlOperationAuthority authority = new OxiaControlOperationAuthority(backend);
        assertThrows(IllegalArgumentException.class, () -> authority.register(receipt, wrongIdentity));
        assertThrows(IllegalArgumentException.class, () -> authority.advance(receipt, 1, wrongRevision));
        assertEquals(
                ControlOperationQueryResultV1.INVALID_RECEIPT,
                authority.query(null, 2_000).resultKind());
        assertEquals(
                ControlOperationQueryResultV1.INVALID_RECEIPT,
                authority.query(receipt, -1).resultKind());
        assertFalse(called[0]);
    }

    private static ControlOperationReceiptV1 receipt(final int seed, final long queryUntil) {
        return receipt(seed, 1, queryUntil);
    }

    private static ControlOperationReceiptV1 receipt(final int seed, final long revision, final long queryUntil) {
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
        return ControlOperationReceiptV1.create(
                operation,
                bytes(32, seed + 1),
                bytes(32, seed + 2),
                bytes(32, seed + 3),
                revision,
                registered,
                queryUntil);
    }

    private static CurrentControlOperationV1 current(
            final ControlOperationReceiptV1 receipt, final long revision, final ControlOperationStateV1 state) {
        return new CurrentControlOperationV1(
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
