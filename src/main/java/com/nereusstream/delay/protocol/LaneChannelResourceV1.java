package com.nereusstream.delay.protocol;

import java.util.List;
import java.util.Objects;

/** Canonical typed value for the Registry LaneChannelResourceV1 branch. */
public final class LaneChannelResourceV1 {
    private final ChannelResourceIdentityV1 channel;

    public LaneChannelResourceV1(final ChannelResourceIdentityV1 channel) {
        this.channel = Objects.requireNonNull(channel, "channel");
    }

    public ChannelResourceIdentityV1 channel() {
        return channel;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, 1, channel.canonicalBytes()));
    }

    public byte[] exactResourceCanonicalBytes() {
        return CanonicalProtobuf.message(
                output -> CanonicalProtobuf.bytes(output, ResourceKind.LANE_CHANNEL.wireValue(), canonicalBytes()));
    }

    public static LaneChannelResourceV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "LaneChannelResourceV1");
        QueryCodecSupport.requireNumbers(fields, new int[] {1}, "LaneChannelResourceV1");
        final LaneChannelResourceV1 result =
                new LaneChannelResourceV1(ChannelResourceIdentityV1.decode(QueryCodecSupport.nested(fields.get(0), 1)));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "LaneChannelResourceV1");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof LaneChannelResourceV1 that && channel.equals(that.channel);
    }

    @Override
    public int hashCode() {
        return channel.hashCode();
    }
}
