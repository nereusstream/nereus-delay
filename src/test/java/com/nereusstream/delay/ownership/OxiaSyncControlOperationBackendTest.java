package com.nereusstream.delay.ownership;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.ControlOperationQueryResult;
import com.nereusstream.delay.protocol.ControlOperationReceipt;
import com.nereusstream.delay.protocol.ControlOperationState;
import com.nereusstream.delay.protocol.CurrentControlOperation;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.Version;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.PutOption;
import io.oxia.client.api.options.defs.OptionVersionId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class OxiaSyncControlOperationBackendTest {
    @Test
    void registerAdvanceQueryAndReopenUseOneVersionCasRecord() {
        final FakeRecordClient records = new FakeRecordClient();
        final OxiaSyncControlOperationBackend backend = new OxiaSyncControlOperationBackend(records, "delay/control");
        final ControlOperationReceipt receipt = receipt(1, 4_000);
        final CurrentControlOperation initial = current(receipt, 1, ControlOperationState.PENDING);

        assertEquals(
                ControlOperationQueryResult.CURRENT,
                backend.register(receipt, initial).resultKind());
        assertEquals(1, records.putCount);
        assertEquals(
                ControlOperationQueryResult.CURRENT,
                backend.register(receipt, initial).resultKind());

        final CurrentControlOperation next = current(receipt, 2, ControlOperationState.DISPATCHING);
        assertEquals(
                ControlOperationQueryResult.CURRENT,
                backend.advance(receipt, 1, next).resultKind());
        assertEquals(
                ControlOperationQueryResult.CURRENT,
                backend.advance(receipt, 1, next).resultKind());
        assertEquals(
                ControlOperationQueryResult.CURRENT,
                backend.query(receipt, 2_000).resultKind());

        final OxiaSyncControlOperationBackend reopened = new OxiaSyncControlOperationBackend(records, "delay/control");
        assertEquals(next, reopened.query(receipt, 2_000).current());
    }

    @Test
    void responseLossAcceptsOnlyExactSuccessorReread() {
        final FakeRecordClient records = new FakeRecordClient();
        final OxiaSyncControlOperationBackend backend = new OxiaSyncControlOperationBackend(records, "delay/lost");
        final ControlOperationReceipt receipt = receipt(2, 4_000);
        final CurrentControlOperation initial = current(receipt, 1, ControlOperationState.PENDING);
        final CurrentControlOperation next = current(receipt, 2, ControlOperationState.DISPATCHING);
        backend.register(receipt, initial);

        records.failNextPutAfterCommit = true;
        assertEquals(
                ControlOperationQueryResult.CURRENT,
                backend.advance(receipt, 1, next).resultKind());
        assertEquals(next, backend.query(receipt, 2_000).current());
    }

    @Test
    void sessionFenceRejectsACommittedWriteAfterTheMarkerChanges() {
        final FakeRecordClient records = new FakeRecordClient();
        final AtomicBoolean sessionAlive = new AtomicBoolean(true);
        final OxiaSyncControlOperationBackend backend =
                new OxiaSyncControlOperationBackend(records, "delay/fenced", () -> {
                    if (!sessionAlive.get()) {
                        throw new IllegalStateException("simulated Oxia session fence");
                    }
                });
        final ControlOperationReceipt receipt = receipt(6, 4_000);
        final CurrentControlOperation initial = current(receipt, 1, ControlOperationState.PENDING);
        records.afterPut = () -> sessionAlive.set(false);

        assertThrows(IllegalStateException.class, () -> backend.register(receipt, initial));
        assertEquals(1, records.putCount);

        final OxiaSyncControlOperationBackend reopened = new OxiaSyncControlOperationBackend(records, "delay/fenced");
        assertEquals(initial, reopened.query(receipt, 2_000).current());
    }

    @Test
    void invalidTransitionAndReceiptIdentityFailClosed() {
        final FakeRecordClient records = new FakeRecordClient();
        final OxiaSyncControlOperationBackend backend = new OxiaSyncControlOperationBackend(records, "delay/strict");
        final ControlOperationReceipt receipt = receipt(3, 4_000);
        final CurrentControlOperation initial = current(receipt, 1, ControlOperationState.PENDING);
        backend.register(receipt, initial);

        assertEquals(
                ControlOperationQueryResult.INTEGRITY_ERROR,
                backend.advance(receipt, 1, current(receipt, 2, ControlOperationState.IN_PROGRESS))
                        .resultKind());
        final ControlOperationReceipt wrong = receipt(4, 4_000);
        assertEquals(
                ControlOperationQueryResult.NOT_FOUND_OR_NOT_AUTHORIZED,
                backend.query(wrong, 2_000).resultKind());
        assertEquals(
                ControlOperationQueryResult.NOT_FOUND_OR_NOT_AUTHORIZED,
                backend.query(receipt, 4_001).resultKind());
    }

    @Test
    void corruptRecordFailsClosedBeforeReturningCurrent() {
        final FakeRecordClient records = new FakeRecordClient();
        final OxiaSyncControlOperationBackend backend = new OxiaSyncControlOperationBackend(records, "delay/bad");
        records.putRaw("delay/bad/operation/" + hex(bytes(32, 5)), new byte[] {0x08, 0x02});
        assertThrows(IllegalStateException.class, () -> backend.query(receipt(5, 4_000), 2_000));
    }

    private static ControlOperationReceipt receipt(final int seed, final long queryUntil) {
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

    private static String hex(final byte[] value) {
        return Bytes.hex(value);
    }

    private static final class FakeRecordClient implements OxiaSyncControlOperationBackend.RecordClient {
        private final Map<String, GetResult> records = new HashMap<>();
        private long nextVersion = 1;
        private int putCount;
        private boolean failNextPutAfterCommit;
        private Runnable afterPut = () -> {};

        @Override
        public GetResult get(final String key) {
            return records.get(key);
        }

        @Override
        public PutResult put(final String key, final byte[] value, final Set<PutOption> options)
                throws UnexpectedVersionIdException, KeyAlreadyExistsException {
            final GetResult current = records.get(key);
            final OptionVersionId condition = options.stream()
                    .filter(OptionVersionId.class::isInstance)
                    .map(OptionVersionId.class::cast)
                    .findFirst()
                    .orElse(null);
            if (condition != null) {
                if (condition.versionId() == OptionVersionId.KEY_NOT_EXISTS && current != null) {
                    throw new KeyAlreadyExistsException(key);
                }
                if (condition.versionId() != OptionVersionId.KEY_NOT_EXISTS
                        && (current == null || current.version().versionId() != condition.versionId())) {
                    throw new UnexpectedVersionIdException(
                            key,
                            current == null
                                    ? OptionVersionId.KEY_NOT_EXISTS
                                    : current.version().versionId());
                }
            }
            final Version version = new Version(nextVersion++, 0, 0, 1, Optional.empty(), Optional.empty());
            records.put(key, new GetResult(key, Bytes.copy(value), version));
            putCount++;
            afterPut.run();
            if (failNextPutAfterCommit) {
                failNextPutAfterCommit = false;
                throw new IllegalStateException("simulated response loss");
            }
            return new PutResult(key, version);
        }

        private void putRaw(final String key, final byte[] value) {
            final Version version = new Version(nextVersion++, 0, 0, 1, Optional.empty(), Optional.empty());
            records.put(key, new GetResult(key, Bytes.copy(value), version));
        }
    }
}
