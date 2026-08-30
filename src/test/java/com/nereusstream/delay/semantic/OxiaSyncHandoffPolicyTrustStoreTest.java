package com.nereusstream.delay.semantic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.SourcePosition;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.Version;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.PutOption;
import io.oxia.client.api.options.defs.OptionVersionId;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class OxiaSyncHandoffPolicyTrustStoreTest {
    @Test
    void persistsImmutableHistoricalIssuerAndActivationRecords() throws Exception {
        final FakeRecords records = new FakeRecords();
        final OxiaSyncHandoffPolicyTrustStore store = new OxiaSyncHandoffPolicyTrustStore(records, "delay/policy");
        final KeyPair keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final SourcePosition active = position(5);
        final SourcePosition later = position(6);
        final byte[] scope = Bytes.sha256(Bytes.utf8("scope"));

        store.installIssuerKey(3, keys.getPublic(), active);
        store.activatePolicy(scope, 7, active);
        store.installIssuerKey(3, keys.getPublic(), active);
        store.activatePolicy(scope, 7, active);

        assertTrue(store.issuerKey(3, later).isPresent());
        assertTrue(store.issuerKey(3, position(4)).isEmpty());
        assertEquals(active, store.activationPosition(scope, 7).orElseThrow());

        final KeyPair conflicting = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        assertThrows(IllegalStateException.class, () -> store.installIssuerKey(3, conflicting.getPublic(), active));
        assertThrows(IllegalStateException.class, () -> store.activatePolicy(scope, 7, later));
    }

    @Test
    void acceptsCommittedCreateAfterResponseLossAndRejectsCorruption() throws Exception {
        final FakeRecords records = new FakeRecords();
        final OxiaSyncHandoffPolicyTrustStore store = new OxiaSyncHandoffPolicyTrustStore(records, "delay/policy-loss");
        final KeyPair keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final SourcePosition active = position(5);

        records.failAfterCommit = true;
        store.installIssuerKey(1, keys.getPublic(), active);
        records.corruptOnlyRecord();
        assertThrows(IllegalStateException.class, () -> store.issuerKey(1, active));
    }

    private static SourcePosition position(final long offset) {
        return new KafkaSourcePosition(
                new ShardId(new RouteIncarnation(bytes(16, 1)), 0),
                "source-cluster",
                UUID.nameUUIDFromBytes(Bytes.utf8("source-topic")),
                offset,
                null,
                1_000 + offset);
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }

    private static final class FakeRecords implements OxiaSyncHandoffPolicyTrustStore.RecordClient {
        private final Map<String, GetResult> values = new HashMap<>();
        private long nextVersion;
        private boolean failAfterCommit;

        @Override
        public GetResult get(final String key) {
            return values.get(key);
        }

        @Override
        public PutResult put(final String key, final byte[] value, final Set<PutOption> options)
                throws UnexpectedVersionIdException, KeyAlreadyExistsException {
            final GetResult current = values.get(key);
            final OptionVersionId condition = options.stream()
                    .filter(OptionVersionId.class::isInstance)
                    .map(OptionVersionId.class::cast)
                    .findFirst()
                    .orElseThrow();
            if (condition.versionId() == OptionVersionId.KEY_NOT_EXISTS && current != null) {
                throw new KeyAlreadyExistsException(key);
            }
            final Version version = new Version(nextVersion++, 0, 0, 1, Optional.empty(), Optional.empty());
            values.put(key, new GetResult(key, Bytes.copy(value), version));
            if (failAfterCommit) {
                failAfterCommit = false;
                throw new IllegalStateException("simulated response loss");
            }
            return new PutResult(key, version);
        }

        private void corruptOnlyRecord() {
            final Map.Entry<String, GetResult> record =
                    values.entrySet().iterator().next();
            final byte[] corrupt = record.getValue().value().clone();
            corrupt[corrupt.length - 1] ^= 1;
            values.put(
                    record.getKey(),
                    new GetResult(record.getKey(), corrupt, record.getValue().version()));
        }
    }
}
