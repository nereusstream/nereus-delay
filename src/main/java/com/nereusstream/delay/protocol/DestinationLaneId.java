package com.nereusstream.delay.protocol;

/** Stable semantic identity of a Destination Lane. */
public final class DestinationLaneId extends FixedBytes {
    public static final int LENGTH = 32;

    public DestinationLaneId(final byte[] bytes) {
        super(bytes, LENGTH, "destinationLaneId");
    }

    public static DestinationLaneId derive(final byte[] canonicalLaneTuple) {
        return new DestinationLaneId(Bytes.sha256(
                Bytes.concat(Bytes.utf8("nereus-delay-destination-lane"), new byte[] {1}, canonicalLaneTuple)));
    }
}
