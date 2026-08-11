package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.ControlOperationQueryResultV1;
import io.nereusstream.delay.protocol.ControlOperationReceiptV1;
import io.nereusstream.delay.protocol.ControlOperationStateV1;
import io.nereusstream.delay.protocol.CurrentControlOperationV1;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PersistentControlOperationAuthorityTest {
    @TempDir
    Path tempDir;

    @Test
    void stateSurvivesReopenAndExactAdvanceRetryIsIdempotent() {
        final ControlOperationReceiptV1 receipt = receipt(1, 4_000);
        final CurrentControlOperationV1 initial = current(receipt, 1, ControlOperationStateV1.PENDING);
        final CurrentControlOperationV1 next = current(receipt, 2, ControlOperationStateV1.DISPATCHING);

        try (AuthorityHolder ignored = new AuthorityHolder(tempDir)) {
            final PersistentControlOperationAuthority authority = ignored.authority();
            assertEquals(ControlOperationQueryResultV1.CURRENT, authority.register(receipt, initial).resultKind());
            assertEquals(next, authority.advance(receipt, 1, next).current());
            // This is the response-loss retry of the same expected revision
            // and exact successor bytes, not a second logical transition.
            assertEquals(next, authority.advance(receipt, 1, next).current());
        }

        final PersistentControlOperationAuthority reopened = new PersistentControlOperationAuthority(tempDir);
        assertEquals(next, reopened.query(receipt, 2_000).current());
        assertEquals(ControlOperationQueryResultV1.NOT_FOUND_OR_NOT_AUTHORIZED,
                reopened.query(receipt, 4_001).resultKind());
    }

    @Test
    void separateAuthorityInstancesShareTheOnDiskCasBoundary() {
        final ControlOperationReceiptV1 receipt = receipt(2, 4_000);
        final CurrentControlOperationV1 initial = current(receipt, 1, ControlOperationStateV1.PENDING);
        final CurrentControlOperationV1 next = current(receipt, 2, ControlOperationStateV1.DISPATCHING);
        final PersistentControlOperationAuthority first = new PersistentControlOperationAuthority(tempDir);
        final PersistentControlOperationAuthority second = new PersistentControlOperationAuthority(tempDir);

        assertEquals(ControlOperationQueryResultV1.CURRENT, first.register(receipt, initial).resultKind());
        assertEquals(next, second.advance(receipt, 1, next).current());
        assertEquals(next, first.query(receipt, 2_000).current());
    }

    @Test
    void malformedStateFailsClosedBeforeReturningAQueryResult() throws Exception {
        final ControlOperationReceiptV1 receipt = receipt(3, 4_000);
        final CurrentControlOperationV1 initial = current(receipt, 1, ControlOperationStateV1.PENDING);
        final PersistentControlOperationAuthority authority = new PersistentControlOperationAuthority(tempDir);
        authority.register(receipt, initial);
        final Path state = tempDir.resolve(Bytes.hex(receipt.operationId()) + ".state");
        final byte[] bytes = Files.readAllBytes(state);
        bytes[bytes.length - 1]++;
        Files.write(state, bytes);

        assertThrows(IllegalStateException.class, () -> authority.query(receipt, 2_000));
    }

    @Test
    void identityAndRevisionFencesMatchTheInMemoryAuthority() {
        final ControlOperationReceiptV1 receipt = receipt(4, 4_000);
        final PersistentControlOperationAuthority authority = new PersistentControlOperationAuthority(tempDir);
        authority.register(receipt, current(receipt, 1, ControlOperationStateV1.PENDING));
        assertEquals(ControlOperationQueryResultV1.INTEGRITY_ERROR,
                authority.advance(receipt, 1, current(receipt, 3, ControlOperationStateV1.IN_PROGRESS))
                        .resultKind());
        assertEquals(ControlOperationQueryResultV1.INTEGRITY_ERROR,
                authority.advance(receipt, Long.MAX_VALUE,
                        current(receipt, 1, ControlOperationStateV1.IN_PROGRESS)).resultKind());
        final byte[] alteredScope = receipt.authenticatedScopeHash();
        alteredScope[0]++;
        final ControlOperationReceiptV1 wrong = ControlOperationReceiptV1.create(receipt.operationId(),
                receipt.requestHash(), alteredScope, receipt.targetSnapshotHash(), receipt.operationRevision(),
                receipt.registeredAt(), receipt.queryUntilEpochMs());
        assertEquals(ControlOperationQueryResultV1.NOT_FOUND_OR_NOT_AUTHORIZED,
                authority.query(wrong, 2_000).resultKind());
        assertEquals(ControlOperationQueryResultV1.INVALID_RECEIPT,
                authority.query(null, 2_000).resultKind());
        assertEquals(ControlOperationQueryResultV1.INVALID_RECEIPT,
                authority.query(receipt, -1).resultKind());
    }

    private static ControlOperationReceiptV1 receipt(final int seed, final long queryUntil) {
        final TrustedUtcIntervalEvidence registered = new TrustedUtcIntervalEvidence(1_000, 1_100,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("persistent-control-clock" + seed),
                1, 1, 1, bytes(32, seed + 10), 0, null);
        return ControlOperationReceiptV1.create(bytes(32, seed), bytes(32, seed + 1), bytes(32, seed + 2),
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

    private record AuthorityHolder(PersistentControlOperationAuthority authority) implements AutoCloseable {
        private AuthorityHolder(final Path root) {
            this(new PersistentControlOperationAuthority(root));
        }

        @Override
        public void close() {
            // The authority acquires locks per operation and owns no open handle.
        }
    }
}
