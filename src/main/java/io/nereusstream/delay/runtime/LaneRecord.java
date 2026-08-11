package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.SourcePosition;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

/** Persisted management and runtime projection for one Destination Lane. */
public record LaneRecord(
        DestinationLaneId laneId,
        byte[] laneIncarnation,
        long laneControlVersion,
        long laneVersion,
        AdmissionGate admissionGate,
        RuntimeReadiness runtimeReadiness,
        int weight,
        long nextEligibleAtEpochMs) {
    public LaneRecord {
        Objects.requireNonNull(laneId, "laneId");
        Objects.requireNonNull(admissionGate, "admissionGate");
        Objects.requireNonNull(runtimeReadiness, "runtimeReadiness");
        Bytes.requireLength(laneIncarnation, 16, "laneIncarnation");
        if (laneControlVersion <= 0 || laneVersion < 0 || weight <= 0 || nextEligibleAtEpochMs < 0) {
            throw new IllegalArgumentException("invalid lane record");
        }
        laneIncarnation = Bytes.copy(laneIncarnation);
    }

    @Override
    public byte[] laneIncarnation() {
        return Bytes.copy(laneIncarnation);
    }

    public static LaneRecord initial(final DestinationLaneId laneId, final SourcePosition sourcePosition) {
        final byte[] incarnationDigest = Bytes.sha256(Bytes.utf8("nereus-delay-lane-incarnation-v1\0"),
                laneId.bytes(), Bytes.lp32(sourcePosition.canonicalBytes()));
        return new LaneRecord(laneId, Arrays.copyOf(incarnationDigest, 16), 1, 0,
                AdmissionGate.OPEN, RuntimeReadiness.RECOVERING_EVIDENCE, 1, 0);
    }

    public boolean schedulable() {
        return admissionGate == AdmissionGate.OPEN && runtimeReadiness == RuntimeReadiness.READY;
    }

    public LaneRecord withReadiness(final RuntimeReadiness next) {
        if (admissionGate != AdmissionGate.OPEN && next == RuntimeReadiness.READY) {
            throw new IllegalStateException("non-open lane cannot become READY");
        }
        return new LaneRecord(laneId, laneIncarnation, laneControlVersion, nextVersion(laneVersion, "laneVersion"),
                admissionGate, next, weight, nextEligibleAtEpochMs);
    }

    /**
     * Updates the scheduler wake-up projection while retaining the management
     * and runtime gates.  READY keys carry this incremented runtime version so
     * a cursor cannot reuse a key from an older projection.
     */
    public LaneRecord withNextEligibleAt(final long next) {
        if (next < 0) {
            throw new IllegalArgumentException("next eligible time must be non-negative");
        }
        return new LaneRecord(laneId, laneIncarnation, laneControlVersion, nextVersion(laneVersion, "laneVersion"),
                admissionGate, runtimeReadiness, weight, next);
    }

    public LaneRecord withGate(final AdmissionGate nextGate) {
        Objects.requireNonNull(nextGate, "nextGate");
        switch (nextGate) {
            case OPEN -> {
                if (admissionGate != AdmissionGate.ADMIN_PAUSED) {
                    throw new IllegalStateException("only ADMIN_PAUSED can resume to OPEN");
                }
            }
            case ADMIN_PAUSED -> {
                if (admissionGate != AdmissionGate.OPEN) {
                    throw new IllegalStateException("only OPEN can become ADMIN_PAUSED");
                }
            }
            case ORDERING_BROKEN -> {
                if (admissionGate != AdmissionGate.OPEN && admissionGate != AdmissionGate.ADMIN_PAUSED) {
                    throw new IllegalStateException("only OPEN or ADMIN_PAUSED can break ordering");
                }
            }
            case CLOSED -> {
                if (admissionGate != AdmissionGate.OPEN && admissionGate != AdmissionGate.ADMIN_PAUSED
                        && admissionGate != AdmissionGate.ORDERING_BROKEN) {
                    throw new IllegalStateException("only an active or ordering-broken lane can close");
                }
            }
            case RETIRED -> {
                if (admissionGate != AdmissionGate.CLOSED) {
                    throw new IllegalStateException("only CLOSED can become RETIRED");
                }
            }
            case ABSENT -> throw new IllegalArgumentException("LaneRecord cannot use ABSENT gate");
        }
        return new LaneRecord(laneId, laneIncarnation, nextVersion(laneControlVersion, "laneControlVersion"),
                nextVersion(laneVersion, "laneVersion"),
                nextGate, nextGate == AdmissionGate.OPEN ? runtimeReadiness : RuntimeReadiness.BLOCKED,
                weight, nextEligibleAtEpochMs);
    }

    public LaneRecord pauseByAdmin() {
        if (admissionGate != AdmissionGate.OPEN) {
            throw new IllegalStateException("only an OPEN lane can be paused");
        }
        return withGate(AdmissionGate.ADMIN_PAUSED);
    }

    public LaneRecord resumeByAdmin() {
        if (admissionGate != AdmissionGate.ADMIN_PAUSED) {
            throw new IllegalStateException("only an ADMIN_PAUSED lane can resume");
        }
        return withGate(AdmissionGate.OPEN);
    }

    public LaneRecord breakOrdering() {
        if (admissionGate != AdmissionGate.OPEN && admissionGate != AdmissionGate.ADMIN_PAUSED) {
            throw new IllegalStateException("lane is already closed or ordering-broken");
        }
        return withGate(AdmissionGate.ORDERING_BROKEN);
    }

    public LaneRecord closeForNewAdmission() {
        if (admissionGate != AdmissionGate.OPEN && admissionGate != AdmissionGate.ADMIN_PAUSED
                && admissionGate != AdmissionGate.ORDERING_BROKEN) {
            throw new IllegalStateException("lane is already closed or retired");
        }
        return withGate(AdmissionGate.CLOSED);
    }

    public LaneRecord retire() {
        if (admissionGate != AdmissionGate.CLOSED) {
            throw new IllegalStateException("only a CLOSED lane can retire");
        }
        return withGate(AdmissionGate.RETIRED);
    }

    private static long nextVersion(final long value, final String name) {
        if (value == Long.MAX_VALUE) {
            throw new IllegalStateException(name + " exhausted");
        }
        return value + 1;
    }

    public byte[] encode() {
        return ByteBuffer.allocate(4 + 32 + 16 + 8 + 8 + 1 + 1 + 4 + 8)
                .putInt(1).put(laneId.bytes()).put(laneIncarnation).putLong(laneControlVersion).putLong(laneVersion)
                .put((byte) admissionGate.wireValue()).put((byte) runtimeReadiness.wireValue()).putInt(weight)
                .putLong(nextEligibleAtEpochMs).array();
    }

    public static LaneRecord decode(final byte[] encoded) {
        if (encoded.length != 4 + 32 + 16 + 8 + 8 + 1 + 1 + 4 + 8) {
            throw new IllegalArgumentException("invalid lane record length");
        }
        final ByteBuffer input = ByteBuffer.wrap(encoded);
        if (input.getInt() != 1) {
            throw new IllegalArgumentException("unsupported lane record version");
        }
        final byte[] id = new byte[32];
        final byte[] incarnation = new byte[16];
        input.get(id).get(incarnation);
        final LaneRecord result = new LaneRecord(new DestinationLaneId(id), incarnation, input.getLong(), input.getLong(),
                AdmissionGate.fromWire(input.get() & 0xff), RuntimeReadiness.fromWire(input.get() & 0xff),
                input.getInt(), input.getLong());
        if (!Arrays.equals(encoded, result.encode())) {
            throw new IllegalArgumentException("non-canonical lane record");
        }
        return result;
    }
}
