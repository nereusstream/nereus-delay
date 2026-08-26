package com.nereusstream.delay.protocol;

import java.util.List;
import java.util.Objects;

/** Canonical typed value for the Registry LaneChannelResource branch. */
public final class LaneChannelResource {
    private final ChannelResourceIdentity channel;

    public LaneChannelResource(final ChannelResourceIdentity channel) {
        this.channel = Objects.requireNonNull(channel, "channel");
    }

    public ChannelResourceIdentity channel() {
        return channel;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, 1, channel.canonicalBytes()));
    }

    public byte[] exactResourceCanonicalBytes() {
        return CanonicalProtobuf.message(
                output -> CanonicalProtobuf.bytes(output, ResourceKind.LANE_CHANNEL.wireValue(), canonicalBytes()));
    }

    public static LaneChannelResource decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "LaneChannelResource");
        QueryCodecSupport.requireNumbers(fields, new int[] {1}, "LaneChannelResource");
        final LaneChannelResource result =
                new LaneChannelResource(ChannelResourceIdentity.decode(QueryCodecSupport.nested(fields.get(0), 1)));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "LaneChannelResource");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof LaneChannelResource that && channel.equals(that.channel);
    }

    @Override
    public int hashCode() {
        return channel.hashCode();
    }
}
