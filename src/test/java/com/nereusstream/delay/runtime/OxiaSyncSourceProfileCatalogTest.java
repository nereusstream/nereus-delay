package com.nereusstream.delay.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.EvidenceVerifierProfileSemantic;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.ProfileSemanticEnvelope;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class OxiaSyncSourceProfileCatalogTest {
    @Test
    void immutableVersionKeepsFirstVisibilityAcrossReopenAndOtherSources() {
        final var records = new Records();
        final var catalog = catalog(records);
        final var policy = policy(10);
        final var at = source(10, 100);
        catalog.publish(policy, at);
        catalog.publish(policy, at);
        final var reopened = catalog(records);
        assertEquals(policy, reopened.resolve(policy.ref(), at));
        assertEquals(policy, reopened.resolve(policy.ref(), source(11, 101)));
        // A version published later cannot authenticate an earlier original use.
        assertNull(reopened.resolve(policy.ref(), source(9, 99)));
        assertNull(reopened.resolve(policy.ref(), source(10, 999)));
        final var another = new KafkaSourcePosition(at.shardId(), "other-cluster", at.nativeTopicUuid(), 11, null, 101);
        assertNull(reopened.resolve(policy.ref(), another));
        assertNull(reopened.resolve(new ProfileRef(policy.profileId(), 1, new byte[32], policy.profileKind()), at));
        assertThrows(IllegalStateException.class, () -> reopened.publish(policy, source(11, 101)));
        assertThrows(IllegalStateException.class, () -> reopened.publish(policy(11), at));
        assertEquals(1, records.values.size());
    }

    @Test
    void responseLossAcceptsOnlyExactDurableRereadAndCasRace() {
        final var records = new Records();
        final var policy = policy(10);
        final var at = source(10, 100);
        records.afterPut = () -> { throw new IllegalStateException("response lost"); };
        catalog(records).publish(policy, at);
        assertEquals(policy, catalog(records).resolve(policy.ref(), at));
        final var absent = new Records();
        absent.beforePut = () -> { throw new IllegalStateException("failed before put"); };
        assertThrows(IllegalStateException.class, () -> catalog(absent).publish(policy, at));
        assertNull(catalog(absent).resolve(policy.ref(), at));
        final var racing = new Records();
        racing.insertRace = true;
        catalog(racing).publish(policy, at);
        assertEquals(policy, catalog(racing).resolve(policy.ref(), at));
    }

    @Test
    void sessionLossAfterWriteCannotBecomeSuccessfulPublicationOrRead() {
        final var records = new Records();
        final var live = new java.util.concurrent.atomic.AtomicBoolean(true);
        final var catalog = new OxiaSyncSourceProfileCatalog(records, "retry-test", () -> {
            if (!live.get()) { throw new IllegalStateException("session lost"); }
        });
        records.afterPut = () -> live.set(false);
        assertThrows(IllegalStateException.class, () -> catalog.publish(policy(10), source(10, 100)));
        assertNotNull(records.values.values().iterator().next());
        assertThrows(IllegalStateException.class, () -> catalog.resolve(policy(10).ref(), source(10, 100)));
        assertEquals(policy(10), catalog(records).resolve(policy(10).ref(), source(10, 100)));
    }

    @Test
    void malformedOrTransplantedValuesRejectInsteadOfBecomingAbsence() {
        final var records = new Records();
        final var at = source(10, 100);
        final var policy = policy(10);
        catalog(records).publish(policy, at);
        final var key = records.values.keySet().iterator().next();
        final byte[] original = records.values.get(key);
        final byte[] corrupt = original.clone();
        corrupt[corrupt.length - 1] ^= 1;
        records.values.put(key, corrupt);
        assertThrows(IllegalArgumentException.class, () -> catalog(records).resolve(policy.ref(), at));
        records.values.put(key, original);
        final var other = new ProfileSemanticEnvelope(ProfileKind.EVIDENCE_VERIFIER, Bytes.utf8("another-profile"),
                1, policy.body());
        catalog(records).publish(other, at);
        final var otherKey = records.values.keySet().stream().filter(value -> !value.equals(key))
                .findFirst().orElseThrow();
        records.values.put(key, records.values.get(otherKey));
        assertThrows(IllegalStateException.class, () -> catalog(records).resolve(policy.ref(), at));
    }

    @Test
    void asynchronousReadRetainsSourceVisibilityAndRejectsSessionLossDuringIo() throws Exception {
        final var records = new Records();
        final var live = new java.util.concurrent.atomic.AtomicBoolean(true);
        final var catalog = new OxiaSyncSourceProfileCatalog(records, "profile-test", () -> {
            if (!live.get()) { throw new IllegalStateException("session lost"); }
        });
        final var policy = policy(10);
        final var source = source(10, 100);
        catalog.publish(policy, source);
        try (var executor = new java.util.concurrent.ThreadPoolExecutor(1, 1, 0,
                java.util.concurrent.TimeUnit.SECONDS, new java.util.concurrent.ArrayBlockingQueue<>(2),
                new java.util.concurrent.ThreadPoolExecutor.AbortPolicy())) {
            assertEquals(policy, catalog.resolveAsync(policy.ref(), source, java.time.Duration.ofSeconds(5),
                    () -> {}, executor).get(5, java.util.concurrent.TimeUnit.SECONDS));
            assertNull(catalog.resolveAsync(policy.ref(), source(9, 99), java.time.Duration.ofSeconds(5),
                    () -> {}, executor).get(5, java.util.concurrent.TimeUnit.SECONDS));
            records.afterGet = () -> live.set(false);
            final var failed = catalog.resolveAsync(policy.ref(), source, java.time.Duration.ofSeconds(5),
                    () -> {}, executor);
            assertEquals("session lost", assertThrows(java.util.concurrent.ExecutionException.class,
                    () -> failed.get(5, java.util.concurrent.TimeUnit.SECONDS)).getCause().getMessage());
        }
    }

    static ProfileSemanticEnvelope policy(int seed) {
        final byte[] key = new byte[32];
        Arrays.fill(key, (byte) seed);
        return new ProfileSemanticEnvelope(ProfileKind.EVIDENCE_VERIFIER, Bytes.utf8("pinned-profile"), 1,
                new EvidenceVerifierProfileSemantic(1, 1, key, key, 0, 10_000, key));
    }

    static KafkaSourcePosition source(long offset, long time) {
        final byte[] route = new byte[16];
        Arrays.fill(route, (byte) 1);
        return new KafkaSourcePosition(new ShardId(new RouteIncarnation(route), 0), "cluster", new UUID(1, 2),
                offset, null, time);
    }

    private static OxiaSyncSourceProfileCatalog catalog(Records records) {
        return new OxiaSyncSourceProfileCatalog(records, "retry-test", () -> {});
    }

    private static final class Records implements OxiaSyncSourceProfileCatalog.RecordClient {
        private final Map<String, byte[]> values = new HashMap<>();
        private Runnable beforePut = () -> {};
        private Runnable afterPut = () -> {};
        private Runnable afterGet = () -> {};
        private boolean insertRace;

        @Override
        public byte[] get(String key) {
            final byte[] value = values.get(key);
            afterGet.run();
            return value;
        }

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
