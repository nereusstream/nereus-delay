package com.nereusstream.delay.protocol;

import java.util.Objects;

/** Physical destination identity, independent of Profile, tenant and execution slots. */
public final class TargetPartitionId extends FixedBytes {
    public static final int LENGTH = 32;
    private static final byte[] HASH_DOMAIN = Bytes.utf8("nereus-delay-target-partition\0");

    public TargetPartitionId(final byte[] bytes) {
        super(bytes, LENGTH, "targetPartitionId");
    }

    public static TargetPartitionId derive(final CanonicalTargetPartition target) {
        return new TargetPartitionId(Bytes.sha256(
                HASH_DOMAIN, Objects.requireNonNull(target, "target").canonicalBytes()));
    }
}
