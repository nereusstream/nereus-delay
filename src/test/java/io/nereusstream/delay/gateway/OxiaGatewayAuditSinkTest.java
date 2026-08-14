package io.nereusstream.delay.gateway;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.transport.Digest32;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.Version;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.PutOption;
import io.oxia.client.api.options.defs.OptionVersionId;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class OxiaGatewayAuditSinkTest {
    @Test
    void recordsAnImmutableEventAndDeduplicatesExactRetries() {
        final FakeRecordClient records = new FakeRecordClient();
        final OxiaGatewayAuditSink sink = new OxiaGatewayAuditSink(records, "delay/gateway");
        final GatewayAuditEventV1 event = event(10, GatewayAuditPhaseV1.RECEIVED);

        sink.record(event);
        sink.record(event);

        assertEquals(1, records.putCount);
        assertEquals(1, records.values().size());
        sink.close();
    }

    @Test
    void acceptsOnlyTheExactEventAfterAWriteResponseIsLost() {
        final FakeRecordClient records = new FakeRecordClient();
        final OxiaGatewayAuditSink sink = new OxiaGatewayAuditSink(records, "delay/lost");
        records.loseNextPutResponse = true;

        sink.record(event(20, GatewayAuditPhaseV1.COMPLETED));

        assertEquals(1, records.putCount);
        assertEquals(1, records.values().size());
    }

    @Test
    void rejectsARecordWithTheSameKeyAndDifferentBytes() {
        final FakeRecordClient records = new FakeRecordClient();
        final OxiaGatewayAuditSink sink = new OxiaGatewayAuditSink(records, "delay/conflict");
        final GatewayAuditEventV1 event = event(30, GatewayAuditPhaseV1.FAILED);
        sink.record(event);
        final String key = records.values().keySet().iterator().next();
        records.records.put(key, new FakeRecordClient.Stored(Bytes.utf8("wrong"), records.records.get(key).version()));

        assertThrows(IllegalStateException.class, () -> sink.record(event));
    }

    private static GatewayAuditEventV1 event(final long observedAt, final GatewayAuditPhaseV1 phase) {
        return new GatewayAuditEventV1(GatewayIngressOperationV1.SCHEDULE,
                new Digest32(bytes(32, 1)), new Digest32(bytes(32, 2)), phase,
                phase == GatewayAuditPhaseV1.COMPLETED ? new Digest32(bytes(32, 3)) : null, observedAt);
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }

    private static final class FakeRecordClient implements OxiaGatewayAuditSink.RecordClient {
        private final Map<String, Stored> records = new TreeMap<>();
        private long nextVersion = 1;
        private int putCount;
        private boolean loseNextPutResponse;

        @Override
        public GetResult get(final String key) {
            final Stored stored = records.get(key);
            return stored == null ? null : new GetResult(key, Bytes.copy(stored.value()), stored.version());
        }

        @Override
        public PutResult put(final String key, final byte[] value, final Set<PutOption> options)
                throws UnexpectedVersionIdException, KeyAlreadyExistsException {
            final Stored existing = records.get(key);
            final OptionVersionId expected = options.stream().filter(OptionVersionId.class::isInstance)
                    .map(OptionVersionId.class::cast).findFirst().orElse(null);
            if (expected != null && expected.versionId() == OptionVersionId.KEY_NOT_EXISTS && existing != null) {
                throw new KeyAlreadyExistsException(key);
            }
            if (expected != null && expected.versionId() != OptionVersionId.KEY_NOT_EXISTS
                    && (existing == null || existing.version().versionId() != expected.versionId())) {
                throw new UnexpectedVersionIdException(key, expected.versionId());
            }
            final Version version = new Version(nextVersion++, 0, 0, 0, Optional.empty(), Optional.empty());
            records.put(key, new Stored(Bytes.copy(value), version));
            putCount++;
            if (loseNextPutResponse) {
                loseNextPutResponse = false;
                throw new IllegalStateException("simulated response loss");
            }
            return new PutResult(key, version);
        }

        @Override
        public void close() {
        }

        private Map<String, Stored> values() {
            return records;
        }

        private record Stored(byte[] value, Version version) {
        }
    }
}
