package com.nereusstream.delay.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.DlqExportMode;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.PayloadProofTrustSetRef;
import com.nereusstream.delay.protocol.PayloadProofTrustSetSemantic;
import com.nereusstream.delay.protocol.PayloadProofVerifierKey;
import com.nereusstream.delay.protocol.RetryPolicySemantic;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.UncertainPolicy;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class PolicyCatalogTest {
    @Test
    void retryPolicyCatalogRequiresExactSourceVisibleHistory() {
        final ShardId shard = new ShardId(RouteIncarnation.fromUuid(new UUID(11, 12)), 4);
        final UUID topic = new UUID(13, 14);
        final RetryPolicySemantic first = retryPolicy("policy", 1);
        final RetryPolicySemantic second = retryPolicy("policy", 2);
        final InMemoryRetryPolicyCatalog catalog = new InMemoryRetryPolicyCatalog();
        catalog.publish(first, position(shard, topic, 10));
        catalog.publish(second, position(shard, topic, 20));

        assertNull(catalog.resolve(first.ref(), position(shard, topic, 9)));
        assertEquals(first, catalog.resolve(first.ref(), position(shard, topic, 10)));
        assertEquals(second, catalog.resolve(second.ref(), position(shard, topic, 20)));
        assertEquals(first, catalog.resolve(first.ref(), position(shard, topic, 20)));
        final KafkaSourcePosition conflictingSameOffset =
                new KafkaSourcePosition(shard, "cluster", topic, 10, 7, 2_000);
        assertNull(catalog.resolve(first.ref(), conflictingSameOffset));
        assertThrows(IllegalArgumentException.class, () -> catalog.publish(first, conflictingSameOffset));
        assertNull(catalog.resolve(second.ref(), position(shard, new UUID(15, 16), 20)));
        assertThrows(
                IllegalArgumentException.class,
                () -> catalog.publish(retryPolicy("other", 1), position(shard, topic, 19)));
    }

    @Test
    void trustSetCatalogDoesNotResolveUnknownOrMismatchedReferences() {
        final PayloadProofTrustSetSemantic semantic =
                new PayloadProofTrustSetSemantic(3, List.of(new PayloadProofVerifierKey(1, bytes(32, 9), 100, 200)));
        final InMemoryPayloadProofTrustSetCatalog catalog = new InMemoryPayloadProofTrustSetCatalog();
        catalog.publish(semantic);
        assertEquals(semantic, catalog.resolve(semantic.ref()));
        assertNull(catalog.resolve(new PayloadProofTrustSetRef(4, bytes(32, 8))));
        catalog.publish(semantic);
        assertEquals(1, catalog.size());
    }

    private static RetryPolicySemantic retryPolicy(final String id, final long version) {
        return new RetryPolicySemantic(
                Bytes.utf8(id),
                version,
                10,
                100,
                3,
                1_000,
                UncertainPolicy.HOLD_FOR_EVIDENCE,
                0,
                DlqExportMode.NOT_CONFIGURED,
                0,
                0,
                0,
                0,
                false,
                bytes(32, (int) version));
    }

    private static KafkaSourcePosition position(final ShardId shard, final UUID topic, final long offset) {
        return new KafkaSourcePosition(shard, "cluster", topic, offset, 1, 1_000 + offset);
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] result = new byte[length];
        for (int index = 0; index < result.length; index++) {
            result[index] = (byte) (seed + index);
        }
        return result;
    }
}
