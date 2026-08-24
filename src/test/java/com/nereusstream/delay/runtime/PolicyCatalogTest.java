package com.nereusstream.delay.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.DlqExportModeV1;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.PayloadProofTrustSetRefV1;
import com.nereusstream.delay.protocol.PayloadProofTrustSetSemanticV1;
import com.nereusstream.delay.protocol.PayloadProofVerifierKeyV1;
import com.nereusstream.delay.protocol.RetryPolicySemanticV1;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.UncertainPolicyV1;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class PolicyCatalogTest {
    @Test
    void retryPolicyCatalogRequiresExactSourceVisibleHistory() {
        final ShardId shard = new ShardId(RouteIncarnation.fromUuid(new UUID(11, 12)), 4);
        final UUID topic = new UUID(13, 14);
        final RetryPolicySemanticV1 first = retryPolicy("policy", 1);
        final RetryPolicySemanticV1 second = retryPolicy("policy", 2);
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
        final PayloadProofTrustSetSemanticV1 semantic = new PayloadProofTrustSetSemanticV1(
                3, List.of(new PayloadProofVerifierKeyV1(1, bytes(32, 9), 100, 200)));
        final InMemoryPayloadProofTrustSetCatalog catalog = new InMemoryPayloadProofTrustSetCatalog();
        catalog.publish(semantic);
        assertEquals(semantic, catalog.resolve(semantic.ref()));
        assertNull(catalog.resolve(new PayloadProofTrustSetRefV1(4, bytes(32, 8))));
        catalog.publish(semantic);
        assertEquals(1, catalog.size());
    }

    private static RetryPolicySemanticV1 retryPolicy(final String id, final long version) {
        return new RetryPolicySemanticV1(
                Bytes.utf8(id),
                version,
                10,
                100,
                3,
                1_000,
                UncertainPolicyV1.HOLD_FOR_EVIDENCE,
                0,
                DlqExportModeV1.NOT_CONFIGURED,
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
