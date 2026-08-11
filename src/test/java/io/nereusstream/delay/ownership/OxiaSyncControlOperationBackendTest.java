package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.ControlOperationQueryResultV1;
import io.nereusstream.delay.protocol.ControlOperationReceiptV1;
import io.nereusstream.delay.protocol.ControlOperationStateV1;
import io.nereusstream.delay.protocol.ControlOperationQueryResponseV1;
import io.nereusstream.delay.protocol.CurrentControlOperationV1;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.Version;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.PutOption;
import io.oxia.client.api.options.defs.OptionVersionId;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class OxiaSyncControlOperationBackendTest {
    @Test
    void registerAdvanceQueryAndReopenUseOneVersionCasRecord() {
        final FakeRecordClient records = new FakeRecordClient();
        final OxiaSyncControlOperationBackend backend = new OxiaSyncControlOperationBackend(records, "delay/control");
        final ControlOperationReceiptV1 receipt = receipt(1, 4_000);
        final CurrentControlOperationV1 initial = current(receipt, 1, ControlOperationStateV1.PENDING);

        assertEquals(ControlOperationQueryResultV1.CURRENT,
                backend.register(receipt, initial).resultKind());
        assertEquals(1, records.putCount);
        assertEquals(ControlOperationQueryResultV1.CURRENT,
                backend.register(receipt, initial).resultKind());

        final CurrentControlOperationV1 next = current(receipt, 2, ControlOperationStateV1.DISPATCHING);
        assertEquals(ControlOperationQueryResultV1.CURRENT,
                backend.advance(receipt, 1, next).resultKind());
        assertEquals(ControlOperationQueryResultV1.CURRENT,
                backend.advance(receipt, 1, next).resultKind());
        assertEquals(ControlOperationQueryResultV1.CURRENT,
                backend.query(receipt, 2_000).resultKind());

        final OxiaSyncControlOperationBackend reopened = new OxiaSyncControlOperationBackend(records, "delay/control");
        assertEquals(next, reopened.query(receipt, 2_000).current());
    }

    @Test
    void responseLossAcceptsOnlyExactSuccessorReread() {
        final FakeRecordClient records = new FakeRecordClient();
        final OxiaSyncControlOperationBackend backend = new OxiaSyncControlOperationBackend(records, "delay/lost");
        final ControlOperationReceiptV1 receipt = receipt(2, 4_000);
        final CurrentControlOperationV1 initial = current(receipt, 1, ControlOperationStateV1.PENDING);
        final CurrentControlOperationV1 next = current(receipt, 2, ControlOperationStateV1.DISPATCHING);
        backend.register(receipt, initial);

        records.failNextPutAfterCommit = true;
        assertEquals(ControlOperationQueryResultV1.CURRENT,
                backend.advance(receipt, 1, next).resultKind());
        assertEquals(next, backend.query(receipt, 2_000).current());
    }

    @Test
    void invalidTransitionAndReceiptIdentityFailClosed() {
        final FakeRecordClient records = new FakeRecordClient();
        final OxiaSyncControlOperationBackend backend = new OxiaSyncControlOperationBackend(records, "delay/strict");
        final ControlOperationReceiptV1 receipt = receipt(3, 4_000);
        final CurrentControlOperationV1 initial = current(receipt, 1, ControlOperationStateV1.PENDING);
        backend.register(receipt, initial);

        assertEquals(ControlOperationQueryResultV1.INTEGRITY_ERROR,
                backend.advance(receipt, 1, current(receipt, 2, ControlOperationStateV1.IN_PROGRESS)).resultKind());
        final ControlOperationReceiptV1 wrong = receipt(4, 4_000);
        assertEquals(ControlOperationQueryResultV1.NOT_FOUND_OR_NOT_AUTHORIZED,
                backend.query(wrong, 2_000).resultKind());
        assertEquals(ControlOperationQueryResultV1.NOT_FOUND_OR_NOT_AUTHORIZED,
                backend.query(receipt, 4_001).resultKind());
    }

    @Test
    void corruptRecordFailsClosedBeforeReturningCurrent() {
        final FakeRecordClient records = new FakeRecordClient();
        final OxiaSyncControlOperationBackend backend = new OxiaSyncControlOperationBackend(records, "delay/bad");
        records.putRaw("delay/bad/operation/" + hex(bytes(32, 5)), new byte[]{0x08, 0x02});
        assertThrows(IllegalStateException.class, () -> backend.query(receipt(5, 4_000), 2_000));
    }

    private static ControlOperationReceiptV1 receipt(final int seed, final long queryUntil) {
        final TrustedUtcIntervalEvidence registered = new TrustedUtcIntervalEvidence(1_000, 1_100,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("control-clock" + seed),
                1, 1, 1, bytes(32, seed + 10), 0, null);
        return ControlOperationReceiptV1.create(bytes(32, seed), bytes(32, seed + 1), bytes(32, seed + 2),
                bytes(32, seed + 3), 1, registered, queryUntil);
    }

    private static CurrentControlOperationV1 current(final ControlOperationReceiptV1 receipt,
                                                      final long revision,
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

    private static String hex(final byte[] value) {
        return Bytes.hex(value);
    }

    private static final class FakeRecordClient implements OxiaSyncControlOperationBackend.RecordClient {
        private final Map<String, GetResult> records = new HashMap<>();
        private long nextVersion = 1;
        private int putCount;
        private boolean failNextPutAfterCommit;

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
                    .findFirst().orElse(null);
            if (condition != null) {
                if (condition.versionId() == OptionVersionId.KEY_NOT_EXISTS && current != null) {
                    throw new KeyAlreadyExistsException(key);
                }
                if (condition.versionId() != OptionVersionId.KEY_NOT_EXISTS
                        && (current == null || current.version().versionId() != condition.versionId())) {
                    throw new UnexpectedVersionIdException(key,
                            current == null ? OptionVersionId.KEY_NOT_EXISTS : current.version().versionId());
                }
            }
            final Version version = new Version(nextVersion++, 0, 0, 1, Optional.empty(), Optional.empty());
            records.put(key, new GetResult(key, Bytes.copy(value), version));
            putCount++;
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
