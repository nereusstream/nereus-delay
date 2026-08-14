package io.nereusstream.delay.gateway;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CanonicalProtobuf;
import io.nereusstream.delay.transport.PhysicalEnqueueAttemptId;

import java.util.Objects;

/** Immutable persisted projection of one Gateway physical attempt. */
public final class GatewayPhysicalAttemptV1 {
    private final int attemptNo;
    private final PhysicalEnqueueAttemptId physicalAttemptId;
    private final GatewayPhysicalAttemptStateV1 state;
    private final byte[] outcomeBytes;
    private final long startedAtEpochMs;
    private final long uncertaintyAtEpochMs;
    private final PhysicalEnqueueAttemptId retryRequestId;
    private final io.nereusstream.delay.transport.Digest32 retryRequestHash;
    private final long revision;
    private final long ownershipNotAfterEpochMs;

    public GatewayPhysicalAttemptV1(final int attemptNo, final PhysicalEnqueueAttemptId physicalAttemptId,
                                    final GatewayPhysicalAttemptStateV1 state, final byte[] outcomeBytes,
                                    final long startedAtEpochMs, final long uncertaintyAtEpochMs,
                                    final long revision, final long ownershipNotAfterEpochMs) {
        this(attemptNo, physicalAttemptId, state, outcomeBytes, startedAtEpochMs, uncertaintyAtEpochMs,
                null, null, revision, ownershipNotAfterEpochMs);
    }

    public GatewayPhysicalAttemptV1(final int attemptNo, final PhysicalEnqueueAttemptId physicalAttemptId,
                                    final GatewayPhysicalAttemptStateV1 state, final byte[] outcomeBytes,
                                    final long startedAtEpochMs, final long uncertaintyAtEpochMs,
                                    final PhysicalEnqueueAttemptId retryRequestId,
                                    final io.nereusstream.delay.transport.Digest32 retryRequestHash,
                                    final long revision, final long ownershipNotAfterEpochMs) {
        if (attemptNo <= 0 || startedAtEpochMs < 0 || uncertaintyAtEpochMs < startedAtEpochMs || revision <= 0
                || ownershipNotAfterEpochMs < startedAtEpochMs || ownershipNotAfterEpochMs > uncertaintyAtEpochMs) {
            throw new IllegalArgumentException("invalid Gateway physical attempt bounds");
        }
        if ((retryRequestId == null) != (retryRequestHash == null)) {
            throw new IllegalArgumentException("retry request id/hash must be present together");
        }
        this.attemptNo = attemptNo;
        this.physicalAttemptId = Objects.requireNonNull(physicalAttemptId, "physicalAttemptId");
        this.state = Objects.requireNonNull(state, "state");
        this.outcomeBytes = outcomeBytes == null ? null : Bytes.copy(outcomeBytes);
        this.startedAtEpochMs = startedAtEpochMs;
        this.uncertaintyAtEpochMs = uncertaintyAtEpochMs;
        this.retryRequestId = retryRequestId;
        this.retryRequestHash = retryRequestHash;
        this.revision = revision;
        this.ownershipNotAfterEpochMs = ownershipNotAfterEpochMs;
    }

    public int attemptNo() {
        return attemptNo;
    }

    public PhysicalEnqueueAttemptId physicalAttemptId() {
        return physicalAttemptId;
    }

    public GatewayPhysicalAttemptStateV1 state() {
        return state;
    }

    public byte[] outcomeBytes() {
        return outcomeBytes == null ? null : Bytes.copy(outcomeBytes);
    }

    public long startedAtEpochMs() {
        return startedAtEpochMs;
    }

    public long uncertaintyAtEpochMs() {
        return uncertaintyAtEpochMs;
    }

    public PhysicalEnqueueAttemptId retryRequestId() {
        return retryRequestId;
    }

    public io.nereusstream.delay.transport.Digest32 retryRequestHash() {
        return retryRequestHash;
    }

    public long revision() {
        return revision;
    }

    public long ownershipNotAfterEpochMs() {
        return ownershipNotAfterEpochMs;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, attemptNo);
            CanonicalProtobuf.bytes(output, 2, physicalAttemptId.bytes());
            CanonicalProtobuf.uint32(output, 3, state.ordinal() + 1);
            if (outcomeBytes != null) {
                CanonicalProtobuf.bytes(output, 4, outcomeBytes);
            }
            CanonicalProtobuf.int64(output, 5, startedAtEpochMs);
            CanonicalProtobuf.int64(output, 6, uncertaintyAtEpochMs);
            if (retryRequestId != null) {
                CanonicalProtobuf.bytes(output, 7, retryRequestId.bytes());
                CanonicalProtobuf.bytes(output, 8, retryRequestHash.bytes());
            }
            CanonicalProtobuf.uint64(output, 9, revision);
            CanonicalProtobuf.int64(output, 10, ownershipNotAfterEpochMs);
        });
    }
}
