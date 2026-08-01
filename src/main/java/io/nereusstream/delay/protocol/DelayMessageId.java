package io.nereusstream.delay.protocol;

/** Stable identity of a delayed message across generations. */
public final class DelayMessageId extends FixedBytes {
    public static final int LENGTH = SelfRoutingId.LENGTH;

    public DelayMessageId(final byte[] bytes) {
        super(bytes, LENGTH, "delayMessageId");
        SelfRoutingId.decode(bytes);
    }

    public static DelayMessageId random(final ShardId shardId) {
        return new DelayMessageId(SelfRoutingId.random(shardId).bytes());
    }

    public SelfRoutingId routingId() {
        return SelfRoutingId.decode(unsafeBytes());
    }
}

