package com.nereusstream.delay.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.DlqExportMode;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.RetryPolicyRef;
import com.nereusstream.delay.protocol.RetryPolicySemantic;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.UncertainPolicy;
import com.nereusstream.delay.store.TargetCheckpointDependencies;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class OxiaSyncRetryPolicyCatalogTest {
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
        assertEquals(policy,
                new TargetCheckpointDependencies.RetryUse(policy.ref(), source(11, 101)).resolve(reopened));
        // Later checkpoint visibility cannot repair a use that preceded publication.
        assertThrows(IllegalStateException.class,
                () -> new TargetCheckpointDependencies.RetryUse(policy.ref(), source(9, 99)).resolve(reopened));
        assertThrows(IllegalStateException.class,
                () -> new TargetCheckpointDependencies.RetryUse(policy.ref(), at)
                        .resolve((ref, original) -> policy(11)));
        assertNull(reopened.resolve(policy.ref(), source(9, 99)));
        assertNull(reopened.resolve(policy.ref(), source(10, 999)));
        final var another = new KafkaSourcePosition(at.shardId(), "other-cluster", at.nativeTopicUuid(), 11, null, 101);
        assertNull(reopened.resolve(policy.ref(), another));
        assertNull(reopened.resolve(new RetryPolicyRef(policy.policyId(), 1, new byte[32]), at));
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
        final var catalog = new OxiaSyncRetryPolicyCatalog(records, "retry-test", () -> {
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
        final var other = new RetryPolicySemantic(Bytes.utf8("another-policy"), 1, 10, 100, 3, 60_000,
                UncertainPolicy.HOLD_FOR_EVIDENCE, 0, DlqExportMode.NOT_CONFIGURED, 0, 0, 0, 0, false, new byte[32]);
        catalog(records).publish(other, at);
        final var otherKey = records.values.keySet().stream().filter(value -> !value.equals(key))
                .findFirst().orElseThrow();
        records.values.put(key, records.values.get(otherKey));
        assertThrows(IllegalStateException.class, () -> catalog(records).resolve(policy.ref(), at));
    }

    static RetryPolicySemantic policy(int backoff) {
        return new RetryPolicySemantic(Bytes.utf8("pinned-policy"), 1, backoff, 100, 3, 60_000,
                UncertainPolicy.HOLD_FOR_EVIDENCE, 0, DlqExportMode.NOT_CONFIGURED, 0, 0, 0, 0, false, new byte[32]);
    }

    static KafkaSourcePosition source(long offset, long time) {
        final byte[] route = new byte[16];
        Arrays.fill(route, (byte) 1);
        return new KafkaSourcePosition(new ShardId(new RouteIncarnation(route), 0), "cluster", new UUID(1, 2),
                offset, null, time);
    }

    private static OxiaSyncRetryPolicyCatalog catalog(Records records) {
        return new OxiaSyncRetryPolicyCatalog(records, "retry-test", () -> {});
    }

    private static final class Records implements OxiaSyncRetryPolicyCatalog.RecordClient {
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
