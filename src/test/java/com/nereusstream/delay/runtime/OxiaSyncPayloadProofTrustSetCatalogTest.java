package com.nereusstream.delay.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.protocol.PayloadProofTrustSetSemantic;
import com.nereusstream.delay.protocol.PayloadProofVerifierKey;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import java.security.KeyPairGenerator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class OxiaSyncPayloadProofTrustSetCatalogTest {
    @Test
    void exactImmutableBytesReopenAndRejectTransplantCorruptionAndOversize() throws Exception {
        final var records = new Records();
        final var semantic = semantic(1);
        catalog(records).publish(semantic, () -> {});
        catalog(records).publish(semantic, () -> {});
        assertEquals(semantic, catalog(records).resolve(semantic.ref()));
        final var other = semantic(2);
        assertNull(catalog(records).resolve(other.ref()));
        final String key = records.values.keySet().iterator().next();
        records.values.put(key, other.canonicalBytes());
        assertThrows(IllegalStateException.class, () -> catalog(records).resolve(semantic.ref()));
        assertThrows(IllegalStateException.class, () -> catalog(records).publish(semantic, () -> {}));
        final byte[] corrupt = semantic.canonicalBytes();
        corrupt[corrupt.length - 1] ^= 1;
        records.values.put(key, corrupt);
        assertThrows(IllegalArgumentException.class, () -> catalog(records).resolve(semantic.ref()));
        final var small = new OxiaSyncPayloadProofTrustSetCatalog(new Records(), "trust", 1, () -> {});
        assertThrows(IllegalArgumentException.class, () -> small.publish(semantic, () -> {}));
    }

    @Test
    void uncertainPublicationRequiresExactDurableReread() throws Exception {
        final var semantic = semantic(1);
        final var lostResponse = new Records();
        lostResponse.afterPut = () -> { throw new IllegalStateException("response lost"); };
        catalog(lostResponse).publish(semantic, () -> {});
        assertEquals(semantic, catalog(lostResponse).resolve(semantic.ref()));
        final var absent = new Records();
        absent.beforePut = () -> { throw new IllegalStateException("write failed"); };
        assertThrows(IllegalStateException.class, () -> catalog(absent).publish(semantic, () -> {}));
        assertNull(catalog(absent).resolve(semantic.ref()));
        final var race = new Records();
        race.insertRace = true;
        catalog(race).publish(semantic, () -> {});
        assertEquals(semantic, catalog(race).resolve(semantic.ref()));
    }

    @Test
    void lostSessionOrPublicationProtectionCannotReportSuccess() throws Exception {
        final var semantic = semantic(1);
        final var records = new Records();
        final var live = new AtomicBoolean(true);
        final Runnable guard = () -> {
            if (!live.get()) { throw new IllegalStateException("protection lost"); }
        };
        records.afterPut = () -> live.set(false);
        assertThrows(IllegalStateException.class, () -> catalog(records).publish(semantic, guard));
        assertEquals(semantic, catalog(records).resolve(semantic.ref()));
        final var session = new OxiaSyncPayloadProofTrustSetCatalog(records, "trust", 4096, guard);
        assertThrows(IllegalStateException.class, () -> session.resolve(semantic.ref()));
        final var untouched = new Records();
        assertThrows(IllegalStateException.class, () -> catalog(untouched).publish(semantic, guard));
        assertEquals(0, untouched.values.size());
    }

    static PayloadProofTrustSetSemantic semantic(long version) throws Exception {
        final var key = KeyPairGenerator.getInstance("Ed25519").generateKeyPair().getPublic();
        return new PayloadProofTrustSetSemantic(version,
                List.of(PayloadProofVerifierKey.fromPublicKey(1, key, 0, 10_000)));
    }

    private static OxiaSyncPayloadProofTrustSetCatalog catalog(Records records) {
        return new OxiaSyncPayloadProofTrustSetCatalog(records, "trust", 4096, () -> {});
    }

    private static final class Records implements OxiaSyncPayloadProofTrustSetCatalog.RecordClient {
        private final Map<String, byte[]> values = new HashMap<>();
        private Runnable beforePut = () -> {};
        private Runnable afterPut = () -> {};
        private boolean insertRace;

        @Override
        public byte[] get(String key) { return values.get(key); }

        @Override
        public void putIfAbsent(String key, byte[] value) throws KeyAlreadyExistsException {
            beforePut.run();
            if (values.putIfAbsent(key, value.clone()) != null || insertRace) {
                throw new KeyAlreadyExistsException(key);
            }
            afterPut.run();
        }
    }
}
