package io.nereusstream.delay.protocol;

/** Stable identity of one logical Command across physical enqueue retries. */
public final class CommandId extends FixedBytes {
    public static final int LENGTH = SelfRoutingId.LENGTH;

    public CommandId(final byte[] bytes) {
        super(bytes, LENGTH, "commandId");
        SelfRoutingId.decode(bytes);
    }

    public static CommandId random(final ShardId shardId) {
        return new CommandId(SelfRoutingId.random(shardId).bytes());
    }

    public SelfRoutingId routingId() {
        return SelfRoutingId.decode(unsafeBytes());
    }
}

