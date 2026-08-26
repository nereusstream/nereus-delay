package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileRef;
import java.util.Arrays;
import java.util.Objects;

/** Exact bounded prefix identity supplied after external REAPING authorization. */
public record CheckpointPrefixSweepRequest(
        ProfileRef objectStoreProfile, byte[] recoveryLineageId, byte[] checkpointId, int maxVersions) {
    private static final int ID_LENGTH = 16;
    private static final int MAX_SINGLE_PAGE_VERSIONS = 1_000;

    public CheckpointPrefixSweepRequest {
        objectStoreProfile = Objects.requireNonNull(objectStoreProfile, "objectStoreProfile");
        if (objectStoreProfile.profileKind() != ProfileKind.OBJECT_STORE) {
            throw new IllegalArgumentException("checkpoint prefix sweep requires an OBJECT_STORE profile");
        }
        requireNonZeroFixed(recoveryLineageId, "recoveryLineageId");
        requireNonZeroFixed(checkpointId, "checkpointId");
        if (maxVersions <= 0 || maxVersions > MAX_SINGLE_PAGE_VERSIONS) {
            throw new IllegalArgumentException("maxVersions must fit one bounded ListObjectVersions page");
        }
        recoveryLineageId = Bytes.copy(recoveryLineageId);
        checkpointId = Bytes.copy(checkpointId);
    }

    @Override
    public byte[] recoveryLineageId() {
        return Bytes.copy(recoveryLineageId);
    }

    @Override
    public byte[] checkpointId() {
        return Bytes.copy(checkpointId);
    }

    private static void requireNonZeroFixed(final byte[] value, final String name) {
        Bytes.requireLength(value, ID_LENGTH, name);
        if (Arrays.stream(toIntArray(value)).allMatch(element -> element == 0)) {
            throw new IllegalArgumentException(name + " must be non-zero");
        }
    }

    private static int[] toIntArray(final byte[] value) {
        final int[] result = new int[value.length];
        for (int index = 0; index < value.length; index++) {
            result[index] = value[index];
        }
        return result;
    }
}
