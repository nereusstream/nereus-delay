package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.Bytes;

import java.util.Arrays;

/** Exact source-assignment and Oxia-session identities bound into a lease. */
public record OwnerLeaseContext(byte[] sourceAssignmentId, byte[] sessionIdentity) {
    public static final int ID_LENGTH = 32;

    public OwnerLeaseContext {
        requireNonZero(sourceAssignmentId, "sourceAssignmentId");
        requireNonZero(sessionIdentity, "sessionIdentity");
        sourceAssignmentId = Bytes.copy(sourceAssignmentId);
        sessionIdentity = Bytes.copy(sessionIdentity);
    }

    @Override
    public byte[] sourceAssignmentId() {
        return Bytes.copy(sourceAssignmentId);
    }

    @Override
    public byte[] sessionIdentity() {
        return Bytes.copy(sessionIdentity);
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof OwnerLeaseContext that
                && Arrays.equals(sourceAssignmentId, that.sourceAssignmentId)
                && Arrays.equals(sessionIdentity, that.sessionIdentity);
    }

    @Override
    public int hashCode() {
        return 31 * Arrays.hashCode(sourceAssignmentId) + Arrays.hashCode(sessionIdentity);
    }

    private static void requireNonZero(final byte[] value, final String name) {
        Bytes.requireLength(value, ID_LENGTH, name);
        for (byte item : value) {
            if (item != 0) {
                return;
            }
        }
        throw new IllegalArgumentException(name + " must be non-zero");
    }
}
