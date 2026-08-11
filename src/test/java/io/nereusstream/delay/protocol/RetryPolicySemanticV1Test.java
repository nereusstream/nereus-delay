package io.nereusstream.delay.protocol;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RetryPolicySemanticV1Test {
    @Test
    void roundTripsAndDerivesThePinnedReferenceHash() {
        final RetryPolicySemanticV1 policy = new RetryPolicySemanticV1(Bytes.utf8("policy-a"), 3,
                100, 10_000, 5, 60_000, UncertainPolicyV1.BOUNDED_RETRY_POSSIBLE_DUPLICATE, 2,
                DlqExportModeV1.BASELINE_AT_LEAST_ONCE, 100, 5_000, 3, 30_000, true, bytes(32, 7));

        assertEquals(policy, RetryPolicySemanticV1.decode(policy.canonicalBytes()));
        assertEquals(new RetryPolicyRefV1(Bytes.utf8("policy-a"), 3, policy.semanticHash()), policy.ref());
        assertTrue(policy.ref().matches(policy));
        policy.validateFor(OrderingMode.BEST_EFFORT);
        assertThrows(IllegalArgumentException.class,
                () -> policy.validateFor(OrderingMode.DELIVERY_TIME_FIFO));
    }

    @Test
    void rejectsBudgetBranchAndHashViolations() {
        assertThrows(IllegalArgumentException.class, () -> new RetryPolicySemanticV1(Bytes.utf8("p"), 1,
                1, 2, 2, 10, UncertainPolicyV1.HOLD_FOR_EVIDENCE, 1,
                DlqExportModeV1.NOT_CONFIGURED, 0, 0, 0, 0, false, bytes(32, 1)));
        assertThrows(IllegalArgumentException.class, () -> new RetryPolicySemanticV1(Bytes.utf8("p"), 1,
                1, 2, 2, 10, UncertainPolicyV1.BOUNDED_RETRY_POSSIBLE_DUPLICATE, 2,
                DlqExportModeV1.NOT_CONFIGURED, 0, 0, 0, 0, false, bytes(32, 1)));
        final RetryPolicySemanticV1 policy = new RetryPolicySemanticV1(Bytes.utf8("p"), 1,
                1, 2, 2, 10, UncertainPolicyV1.HOLD_FOR_EVIDENCE, 0,
                DlqExportModeV1.NOT_CONFIGURED, 0, 0, 0, 0, false, bytes(32, 1));
        final byte[] tampered = policy.canonicalBytes();
        tampered[tampered.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> RetryPolicySemanticV1.decode(tampered));
    }

    @Test
    void preservesCompleteUnsignedRetryPolicyVersionBits() {
        final RetryPolicySemanticV1 policy = new RetryPolicySemanticV1(Bytes.utf8("unsigned-policy"), Long.MIN_VALUE,
                1, 2, 2, 10, UncertainPolicyV1.HOLD_FOR_EVIDENCE, 0,
                DlqExportModeV1.NOT_CONFIGURED, 0, 0, 0, 0, false, bytes(32, 13));

        final RetryPolicySemanticV1 decoded = RetryPolicySemanticV1.decode(policy.canonicalBytes());

        assertEquals(Long.MIN_VALUE, decoded.version());
        assertEquals(Long.MIN_VALUE, decoded.ref().version());
        assertEquals(policy, decoded);
        assertEquals(decoded.ref(), RetryPolicyRefV1.decode(decoded.ref().canonicalBytes()));
    }

    @Test
    void computesCheckedExponentialCapAndRegistryJitterDeterministically() {
        final RetryPolicySemanticV1 policy = new RetryPolicySemanticV1(Bytes.utf8("jitter-policy"), 1,
                100, 350, 5, 10_000, UncertainPolicyV1.HOLD_FOR_EVIDENCE, 0,
                DlqExportModeV1.NOT_CONFIGURED, 0, 0, 0, 0, false, bytes(32, 19));
        final DelayMessageId messageId = DelayMessageId.random(new ShardId(RouteIncarnation.random(), 9));

        assertEquals(100, policy.retryBackoffCap(1));
        assertEquals(200, policy.retryBackoffCap(2));
        assertEquals(350, policy.retryBackoffCap(3));
        assertEquals(350, policy.retryBackoffCap(0xffff_ffffL));

        final long cap = policy.retryBackoffCap(2);
        final long sample = Bytes.readU64be(Bytes.sha256(Bytes.utf8("nereus-delay-retry-v1"), Bytes.u8(1),
                messageId.bytes(), Bytes.u32be(0), Bytes.u32be(2)), 0);
        final long expected = Math.multiplyHigh(sample, cap + 1) + (sample < 0 ? cap + 1 : 0);
        assertEquals(expected, RetryJitterV1.delayMs(RetryJitterV1.MESSAGE_PUBLISH, messageId, 0, 2, cap));
        assertEquals(RetryJitterV1.delayMs(RetryJitterV1.MESSAGE_PUBLISH, messageId, 0, 2, cap),
                RetryJitterV1.delayMs(RetryJitterV1.MESSAGE_PUBLISH, messageId, 0, 2, cap));
        assertThrows(IllegalArgumentException.class,
                () -> RetryJitterV1.delayMs(RetryJitterV1.MESSAGE_PUBLISH, messageId, 0, 0, cap));

        final RetryPolicySemanticV1 zeroBackoff = new RetryPolicySemanticV1(Bytes.utf8("zero-backoff"), 1,
                0, 1_000, 5, 10_000, UncertainPolicyV1.HOLD_FOR_EVIDENCE, 0,
                DlqExportModeV1.NOT_CONFIGURED, 0, 0, 0, 0, false, bytes(32, 20));
        assertEquals(0, zeroBackoff.retryBackoffCap(0xffff_ffffL));
    }

    @Test
    void jitterAcceptsUnsignedHighBitGenerationAndAttemptBits() {
        final DelayMessageId messageId = DelayMessageId.random(new ShardId(RouteIncarnation.random(), 9));
        final long expected = RetryJitterV1.delayMs(RetryJitterV1.MESSAGE_PUBLISH, messageId,
                0x8000_0000L, 0x8000_0000L, 10_000);
        assertEquals(expected, RetryJitterV1.delayMs(RetryJitterV1.MESSAGE_PUBLISH, messageId,
                0x8000_0000L, 0x8000_0000L, 10_000));
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
