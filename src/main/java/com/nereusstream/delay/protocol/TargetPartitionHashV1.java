package com.nereusstream.delay.protocol;

import java.util.Objects;

/** Shared implementation of the Registry TARGET_PARTITION_HASH_V1 formula. */
public final class TargetPartitionHashV1 {
    private static final byte[] HASH_DOMAIN = Bytes.utf8("nereus-delay-target-partition-v1");

    private TargetPartitionHashV1() {}

    /**
     * Computes the target partition from the immutable Destination Profile
     * reference, its partition-count snapshot and the selected routing bytes.
     * The returned value is the complete unsigned uint32 domain as a long.
     */
    public static long partition(
            final ProfileRefV1 destinationProfile, final int targetPartitionCount, final byte[] routingBytes) {
        Objects.requireNonNull(destinationProfile, "destinationProfile");
        Objects.requireNonNull(routingBytes, "routingBytes");
        if (destinationProfile.profileKind() != ProfileKindV1.DESTINATION) {
            throw new IllegalArgumentException("target partition hash requires a DESTINATION profile");
        }
        final long partitionCount = Integer.toUnsignedLong(targetPartitionCount);
        if (partitionCount == 0) {
            throw new IllegalArgumentException("target partition count must be non-zero");
        }
        final byte[] digest = Bytes.sha256(
                HASH_DOMAIN,
                Bytes.lp32(destinationProfile.profileId()),
                Bytes.u64beBits(destinationProfile.version()),
                Bytes.lp32(routingBytes));
        return Long.remainderUnsigned(Bytes.readU64be(digest, 0), partitionCount);
    }
}
