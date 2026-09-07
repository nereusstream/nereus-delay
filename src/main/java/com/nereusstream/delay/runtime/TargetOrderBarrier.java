package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.QueryCodecSupport;
import com.nereusstream.delay.protocol.TargetMessageLocator;
import com.nereusstream.delay.store.TargetKeyCodec;
import java.util.Arrays;
import java.util.Objects;

/** Exact reference to the one strict head whose Claim or attempt obligations still block successors. */
public final class TargetOrderBarrier {
    public static final int MAX_ORDERED_KEY_BYTES = 140;
    public static final int MAX_CANONICAL_BYTES =
            3 + TargetMessageLocator.MAX_CANONICAL_BYTES + 3 + MAX_ORDERED_KEY_BYTES + 11 + 34 + 34;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-target-order-barrier\0");
    private final TargetMessageLocator locator;
    private final TargetKeyCodec.Ordered order;
    private final long runtimeRevision;
    private final byte[] runtimeDigest;
    private final byte[] digest;

    public TargetOrderBarrier(
            final TargetMessageLocator locator,
            final byte[] orderedKey,
            final long runtimeRevision,
            final byte[] runtimeDigest) {
        this.locator = Objects.requireNonNull(locator, "locator");
        order = TargetKeyCodec.decodeOrdered(orderedKey);
        if (locator.orderingMode() != OrderingMode.DELIVERY_TIME_FIFO
                || !locator.target().equals(order.target())
                || !Arrays.equals(locator.orderingDomain(), order.orderingDomain())
                || !locator.messageId().equals(order.messageId())
                || locator.generation() != order.generation()
                || runtimeRevision == 0) {
            throw new IllegalArgumentException("Target order barrier locator/order/revision mismatch");
        }
        Bytes.requireLength(runtimeDigest, 32, "runtimeDigest");
        if (Arrays.equals(runtimeDigest, new byte[32])) {
            throw new IllegalArgumentException("Target order barrier runtime digest is unassigned");
        }
        this.runtimeRevision = runtimeRevision;
        this.runtimeDigest = Bytes.copy(runtimeDigest);
        digest = Bytes.sha256(DIGEST_DOMAIN, fields());
    }

    public static TargetOrderBarrier fromMessage(final TargetMessageRecord message) {
        final TargetOrderBarrier barrier = new TargetOrderBarrier(
                message.locator(),
                orderedKey(message),
                message.runtime().runtimeRevision(),
                message.runtime().runtimeDigest());
        barrier.requireMessageProjection(message);
        return barrier;
    }

    private static byte[] orderedKey(final TargetMessageRecord message) {
        return TargetKeyCodec.ordered(
                message.locator().target(),
                message.locator().orderingDomain(),
                message.deliverAtEpochMs(),
                message.scheduleSource().sourceOrderToken(),
                message.locator().messageId(),
                message.locator().generation());
    }

    /** This verifies the exact retained projection, not source authority to create or release it. */
    public void requireMessageProjection(final TargetMessageRecord message) {
        final TargetGenerationRuntimeIndex runtime = message.runtime();
        if (!locator.equals(message.locator())
                || !Arrays.equals(order.encodedKey(), orderedKey(message))
                || runtimeRevision != runtime.runtimeRevision()
                || !Bytes.constantTimeEquals(runtimeDigest, runtime.runtimeDigest())
                || (runtime.currentWorkKind() != CurrentSendWorkKind.CLAIMED
                        && runtime.currentWorkKind() != CurrentSendWorkKind.PUBLISHING
                        && runtime.attemptObligations().isEmpty())) {
            throw new IllegalArgumentException("Target order barrier lacks its exact blocking Message runtime");
        }
    }

    public TargetMessageLocator locator() {
        return locator;
    }

    public TargetKeyCodec.Ordered order() {
        return order;
    }

    public long runtimeRevision() {
        return runtimeRevision;
    }

    public byte[] runtimeDigest() {
        return Bytes.copy(runtimeDigest);
    }

    public byte[] digest() {
        return Bytes.copy(digest);
    }

    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.bytes(out, 1, locator.canonicalBytes());
            CanonicalProtobuf.bytes(out, 2, order.encodedKey());
            CanonicalProtobuf.uint64Bits(out, 3, runtimeRevision);
            CanonicalProtobuf.bytes(out, 4, runtimeDigest);
        });
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            out.writeBytes(fields());
            CanonicalProtobuf.bytes(out, 5, digest);
        });
    }

    public static TargetOrderBarrier decode(final byte[] encoded) {
        if (encoded == null || encoded.length > MAX_CANONICAL_BYTES) {
            throw new IllegalArgumentException("Target order barrier exceeds its byte bound");
        }
        final var fields = QueryCodecSupport.read(encoded, "TargetOrderBarrier");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3, 4, 5}, "TargetOrderBarrier");
        final TargetOrderBarrier barrier = new TargetOrderBarrier(
                TargetMessageLocator.decode(QueryCodecSupport.bytes(fields.get(0), 1)),
                QueryCodecSupport.bytes(fields.get(1), 2),
                QueryCodecSupport.uint64Bits(fields.get(2), 3),
                QueryCodecSupport.fixed(fields.get(3), 4, 32));
        if (!Bytes.constantTimeEquals(barrier.digest, QueryCodecSupport.fixed(fields.get(4), 5, 32))) {
            throw new IllegalArgumentException("Target order barrier digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, barrier.canonicalBytes(), "TargetOrderBarrier");
        return barrier;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof TargetOrderBarrier that && Arrays.equals(canonicalBytes(), that.canonicalBytes());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(digest);
    }
}
