package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.Bytes;
import java.util.Arrays;
import java.util.Objects;

/** Exact source-assignment identity/epoch and Oxia-session identity bound into a lease. */
public record OwnerLeaseContext(byte[] sourceAssignmentId, long assignmentEpoch, byte[] sessionIdentity) {
    public static final int ID_LENGTH = 32;

    /** Compatibility constructor for legacy contexts that predate assignment epochs. */
    public OwnerLeaseContext(final byte[] sourceAssignmentId, final byte[] sessionIdentity) {
        this(sourceAssignmentId, 0, sessionIdentity);
    }

    public OwnerLeaseContext {
        requireNonZero(sourceAssignmentId, "sourceAssignmentId");
        requireNonZero(sessionIdentity, "sessionIdentity");
        if (assignmentEpoch < 0) {
            throw new IllegalArgumentException("assignmentEpoch must be non-negative");
        }
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
                && assignmentEpoch == that.assignmentEpoch
                && Arrays.equals(sessionIdentity, that.sessionIdentity);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(sourceAssignmentId), assignmentEpoch, Arrays.hashCode(sessionIdentity));
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
