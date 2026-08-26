package com.nereusstream.delay.ownership;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.ControlOperationQueryResult;
import com.nereusstream.delay.protocol.ControlOperationReceipt;
import com.nereusstream.delay.protocol.ControlOperationState;
import com.nereusstream.delay.protocol.CurrentControlOperation;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PersistentControlOperationAuthorityTest {
    @TempDir
    Path tempDir;

    @Test
    void stateSurvivesReopenAndExactAdvanceRetryIsIdempotent() {
        final ControlOperationReceipt receipt = receipt(1, 4_000);
        final CurrentControlOperation initial = current(receipt, 1, ControlOperationState.PENDING);
        final CurrentControlOperation next = current(receipt, 2, ControlOperationState.DISPATCHING);

        try (AuthorityHolder ignored = new AuthorityHolder(tempDir)) {
            final PersistentControlOperationAuthority authority = ignored.authority();
            assertEquals(
                    ControlOperationQueryResult.CURRENT,
                    authority.register(receipt, initial).resultKind());
            assertEquals(next, authority.advance(receipt, 1, next).current());
            // This is the response-loss retry of the same expected revision
            // and exact successor bytes, not a second logical transition.
            assertEquals(next, authority.advance(receipt, 1, next).current());
        }

        final PersistentControlOperationAuthority reopened = new PersistentControlOperationAuthority(tempDir);
        assertEquals(next, reopened.query(receipt, 2_000).current());
        assertEquals(
                ControlOperationQueryResult.NOT_FOUND_OR_NOT_AUTHORIZED,
                reopened.query(receipt, 4_001).resultKind());
    }

    @Test
    void separateAuthorityInstancesShareTheOnDiskCasBoundary() {
        final ControlOperationReceipt receipt = receipt(2, 4_000);
        final CurrentControlOperation initial = current(receipt, 1, ControlOperationState.PENDING);
        final CurrentControlOperation next = current(receipt, 2, ControlOperationState.DISPATCHING);
        final PersistentControlOperationAuthority first = new PersistentControlOperationAuthority(tempDir);
        final PersistentControlOperationAuthority second = new PersistentControlOperationAuthority(tempDir);

        assertEquals(
                ControlOperationQueryResult.CURRENT,
                first.register(receipt, initial).resultKind());
        assertEquals(next, second.advance(receipt, 1, next).current());
        assertEquals(next, first.query(receipt, 2_000).current());
    }

    @Test
    void malformedStateFailsClosedBeforeReturningAQueryResult() throws Exception {
        final ControlOperationReceipt receipt = receipt(3, 4_000);
        final CurrentControlOperation initial = current(receipt, 1, ControlOperationState.PENDING);
        final PersistentControlOperationAuthority authority = new PersistentControlOperationAuthority(tempDir);
        authority.register(receipt, initial);
        final Path state = tempDir.resolve(Bytes.hex(receipt.operationId()) + ".state");
        final byte[] bytes = Files.readAllBytes(state);
        bytes[bytes.length - 1]++;
        Files.write(state, bytes);

        assertThrows(IllegalStateException.class, () -> authority.query(receipt, 2_000));
    }

    @Test
    void rejectsSymbolicParentComponentBeforeCreatingControlStateOutsideBoundary() throws Exception {
        final Path parentRoot = tempDir.resolve("control-parent");
        final Path outside = tempDir.resolve("control-outside");
        Files.createDirectories(parentRoot);
        Files.createDirectories(outside);
        Files.createSymbolicLink(parentRoot.resolve("nested"), outside);

        assertThrows(
                IllegalStateException.class,
                () -> new PersistentControlOperationAuthority(parentRoot.resolve("nested/state")));
        assertFalse(Files.exists(outside.resolve("state"), java.nio.file.LinkOption.NOFOLLOW_LINKS));
    }

    @Test
    void identityAndRevisionFencesMatchTheInMemoryAuthority() {
        final ControlOperationReceipt receipt = receipt(4, 4_000);
        final PersistentControlOperationAuthority authority = new PersistentControlOperationAuthority(tempDir);
        authority.register(receipt, current(receipt, 1, ControlOperationState.PENDING));
        assertEquals(
                ControlOperationQueryResult.INTEGRITY_ERROR,
                authority
                        .advance(receipt, 1, current(receipt, 3, ControlOperationState.IN_PROGRESS))
                        .resultKind());
        assertEquals(
                ControlOperationQueryResult.INTEGRITY_ERROR,
                authority
                        .advance(receipt, Long.MAX_VALUE, current(receipt, 1, ControlOperationState.IN_PROGRESS))
                        .resultKind());
        final byte[] alteredScope = receipt.authenticatedScopeHash();
        alteredScope[0]++;
        final ControlOperationReceipt wrong = ControlOperationReceipt.create(
                receipt.operationId(),
                receipt.requestHash(),
                alteredScope,
                receipt.targetSnapshotHash(),
                receipt.operationRevision(),
                receipt.registeredAt(),
                receipt.queryUntilEpochMs());
        assertEquals(
                ControlOperationQueryResult.NOT_FOUND_OR_NOT_AUTHORIZED,
                authority.query(wrong, 2_000).resultKind());
        assertEquals(
                ControlOperationQueryResult.INVALID_RECEIPT,
                authority.query(null, 2_000).resultKind());
        assertEquals(
                ControlOperationQueryResult.INVALID_RECEIPT,
                authority.query(receipt, -1).resultKind());
    }

    private static ControlOperationReceipt receipt(final int seed, final long queryUntil) {
        final TrustedUtcIntervalEvidence registered = new TrustedUtcIntervalEvidence(
                1_000,
                1_100,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("persistent-control-clock" + seed),
                1,
                1,
                1,
                bytes(32, seed + 10),
                0,
                null);
        return ControlOperationReceipt.create(
                bytes(32, seed),
                bytes(32, seed + 1),
                bytes(32, seed + 2),
                bytes(32, seed + 3),
                1,
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
