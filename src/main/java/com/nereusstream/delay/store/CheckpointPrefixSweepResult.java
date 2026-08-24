package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.Bytes;

/** Bounded exact-version prefix sweep receipt. */
public record CheckpointPrefixSweepResult(
        int listedVersionCount, int deletedVersionCount, byte[] providerRequestIdHash, byte[] responseHash) {
    private static final int HASH_LENGTH = 32;

    public CheckpointPrefixSweepResult {
        if (listedVersionCount < 0 || deletedVersionCount < 0 || deletedVersionCount > listedVersionCount) {
            throw new IllegalArgumentException("invalid checkpoint prefix sweep counts");
        }
        Bytes.requireLength(providerRequestIdHash, HASH_LENGTH, "providerRequestIdHash");
        Bytes.requireLength(responseHash, HASH_LENGTH, "responseHash");
        providerRequestIdHash = Bytes.copy(providerRequestIdHash);
        responseHash = Bytes.copy(responseHash);
    }

    public boolean emptyAfterSweep() {
        return deletedVersionCount() == listedVersionCount();
    }

    @Override
    public byte[] providerRequestIdHash() {
        return Bytes.copy(providerRequestIdHash);
    }

    @Override
    public byte[] responseHash() {
        return Bytes.copy(responseHash);
    }
}
