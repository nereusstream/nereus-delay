package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.ControlOperationQueryResponseV1;
import io.nereusstream.delay.protocol.ControlOperationQueryResultV1;
import io.nereusstream.delay.protocol.ControlOperationReceiptV1;
import io.nereusstream.delay.protocol.ControlOperationStateV1;
import io.nereusstream.delay.protocol.CurrentControlOperationV1;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ControlOperationAuthorityTest {
    @Test
    void registerIsIdempotentAndAdvanceRequiresExactRevision() {
        final ControlOperationReceiptV1 receipt = receipt(1, 4_000);
        final CurrentControlOperationV1 initial = current(receipt, 1, ControlOperationStateV1.PENDING);
        final InMemoryControlOperationAuthority authority = new InMemoryControlOperationAuthority();

        assertEquals(ControlOperationQueryResultV1.CURRENT,
                authority.register(receipt, initial).resultKind());
        assertEquals(initial, authority.register(receipt, initial).current());

        final CurrentControlOperationV1 next = current(receipt, 2, ControlOperationStateV1.IN_PROGRESS);
        assertEquals(ControlOperationQueryResultV1.CURRENT,
                authority.advance(receipt, 1, next).resultKind());
        assertEquals(ControlOperationQueryResultV1.INTEGRITY_ERROR,
                authority.advance(receipt, 1, current(receipt, 3, ControlOperationStateV1.IN_PROGRESS))
                        .resultKind());
        assertEquals(next, authority.query(receipt, 2_000).current());
    }

    @Test
    void queryUsesCompleteReceiptAndFixedRetentionBoundary() {
        final ControlOperationReceiptV1 receipt = receipt(2, 4_000);
        final CurrentControlOperationV1 initial = current(receipt, 1, ControlOperationStateV1.PENDING);
        final InMemoryControlOperationAuthority authority = new InMemoryControlOperationAuthority();
        authority.register(receipt, initial);

        final byte[] alteredScope = receipt.authenticatedScopeHash();
        alteredScope[0]++;
        final ControlOperationReceiptV1 wrongScope = ControlOperationReceiptV1.create(receipt.operationId(),
                receipt.requestHash(), alteredScope, receipt.targetSnapshotHash(), receipt.operationRevision(),
                receipt.registeredAt(), receipt.queryUntilEpochMs());
        assertEquals(ControlOperationQueryResultV1.NOT_FOUND_OR_NOT_AUTHORIZED,
                authority.query(wrongScope, 2_000).resultKind());
        assertEquals(ControlOperationQueryResultV1.NOT_FOUND_OR_NOT_AUTHORIZED,
                authority.query(receipt, 4_001).resultKind());
        assertEquals(ControlOperationQueryResultV1.INVALID_RECEIPT,
                authority.query(receipt, -1).resultKind());
    }

    @Test
    void projectionIdentityMismatchIsAClosedIntegrityError() {
        final ControlOperationReceiptV1 receipt = receipt(5, 4_000);
        final InMemoryControlOperationAuthority authority = new InMemoryControlOperationAuthority();
        final byte[] otherOperation = receipt.operationId();
        otherOperation[0]++;
        final CurrentControlOperationV1 wrong = new CurrentControlOperationV1(otherOperation,
                receipt.requestHash(), receipt.authenticatedScopeHash(), ControlOperationStateV1.PENDING, 1,
                List.of(), null);
        assertEquals(ControlOperationQueryResultV1.INTEGRITY_ERROR,
                authority.register(receipt, wrong).resultKind());
    }

    @Test
    void oxiaAdapterRejectsAResponseBoundToAnotherOperation() {
        final ControlOperationReceiptV1 receipt = receipt(3, 4_000);
        final CurrentControlOperationV1 initial = current(receipt, 1, ControlOperationStateV1.PENDING);
        final ControlOperationReceiptV1 other = receipt(4, 4_000);
        final CurrentControlOperationV1 otherCurrent = current(other, 1, ControlOperationStateV1.PENDING);
        final OxiaControlOperationAuthority authority = new OxiaControlOperationAuthority(
                new OxiaControlOperationAuthority.CasBackend() {
                    @Override
                    public ControlOperationQueryResponseV1 register(final ControlOperationReceiptV1 ignored,
                                                                     final CurrentControlOperationV1 ignoredInitial) {
                        return ControlOperationQueryResponseV1.current(otherCurrent);
                    }

                    @Override
                    public ControlOperationQueryResponseV1 advance(final ControlOperationReceiptV1 ignored,
                                                                    final long ignoredRevision,
                                                                    final CurrentControlOperationV1 ignoredNext) {
                        return ControlOperationQueryResponseV1.current(otherCurrent);
                    }

                    @Override
                    public ControlOperationQueryResponseV1 query(final ControlOperationReceiptV1 ignored,
                                                                 final long ignoredNow) {
                        return ControlOperationQueryResponseV1.current(otherCurrent);
                    }
                });
        assertThrows(IllegalStateException.class, () -> authority.register(receipt, initial));
    }

    private static ControlOperationReceiptV1 receipt(final int seed, final long queryUntil) {
        final byte[] operation = bytes(32, seed);
        final TrustedUtcIntervalEvidence registered = new TrustedUtcIntervalEvidence(1_000, 1_100,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("control-clock" + seed),
                1, 1, 1, bytes(32, seed + 10), 0, null);
        return ControlOperationReceiptV1.create(operation, bytes(32, seed + 1), bytes(32, seed + 2),
                bytes(32, seed + 3), 1, registered, queryUntil);
    }

    private static CurrentControlOperationV1 current(final ControlOperationReceiptV1 receipt, final long revision,
                                                      final ControlOperationStateV1 state) {
        return new CurrentControlOperationV1(receipt.operationId(), receipt.requestHash(),
                receipt.authenticatedScopeHash(), state, revision, List.of(), null);
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < value.length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
