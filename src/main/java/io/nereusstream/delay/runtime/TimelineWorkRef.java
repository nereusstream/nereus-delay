package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CanonicalProtobuf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Canonical {@code TimelineWorkRefV1} projection.
 *
 * <p>The semantic digest intentionally excludes {@code runtimeRevision}; the instance
 * digest includes it and fences one local runtime snapshot.  Control references and
 * source positions are retained as canonical nested bytes until their dedicated V1
 * codecs are wired into the runtime state machine.</p>
 */
public final class TimelineWorkRef {
    public static final int HASH_LENGTH = 32;

    private final TimelineWorkKind workKind;
    private final byte[] encodedTimelineKey;
    private final byte[] timelineKeySha256;
    private final long actionAtEpochMs;
    private final long retryEligibilityAtEpochMs;
    private final int candidateAttemptNo;
    private final long runtimeRevision;
    private final boolean orderedHeadBlocking;
    private final UncertainRetryAuthority uncertainRetryAuthority;
    private final byte[] uncertainRetryControl;
    private final byte[] uncertainRetryControlPosition;
    private final byte[] semanticWorkDigest;
    private final byte[] workInstanceDigest;

    public TimelineWorkRef(final TimelineWorkKind workKind, final byte[] encodedTimelineKey,
                           final long actionAtEpochMs, final long retryEligibilityAtEpochMs,
                           final int candidateAttemptNo, final long runtimeRevision,
                           final boolean orderedHeadBlocking,
                           final UncertainRetryAuthority uncertainRetryAuthority,
                           final byte[] uncertainRetryControl, final byte[] uncertainRetryControlPosition) {
        this.workKind = Objects.requireNonNull(workKind, "workKind");
        this.encodedTimelineKey = requireTimelineKey(encodedTimelineKey);
        this.timelineKeySha256 = Bytes.sha256(this.encodedTimelineKey);
        if (actionAtEpochMs < 0 || retryEligibilityAtEpochMs < 0 || candidateAttemptNo <= 0
                || runtimeRevision <= 0) {
            throw new IllegalArgumentException("invalid timeline work numeric fields");
        }
        this.actionAtEpochMs = actionAtEpochMs;
        this.retryEligibilityAtEpochMs = retryEligibilityAtEpochMs;
        this.candidateAttemptNo = candidateAttemptNo;
        this.runtimeRevision = runtimeRevision;
        this.orderedHeadBlocking = orderedHeadBlocking;
        final boolean orderedKey = this.encodedTimelineKey[0] == 2;
        if (orderedHeadBlocking != orderedKey) {
            throw new IllegalArgumentException("timeline ordered-head flag does not match key namespace");
        }
        this.uncertainRetryAuthority = Objects.requireNonNull(uncertainRetryAuthority,
                "uncertainRetryAuthority");
        this.uncertainRetryControl = optional(uncertainRetryControl);
        this.uncertainRetryControlPosition = optional(uncertainRetryControlPosition);
        validateAuthority();
        if (workKind == TimelineWorkKind.INITIAL_SCHEDULE
                && (candidateAttemptNo != 1 || retryEligibilityAtEpochMs != actionAtEpochMs)) {
            throw new IllegalArgumentException("initial schedule has invalid attempt or retry eligibility");
        }
        this.semanticWorkDigest = computeSemanticDigest(this.workKind, this.encodedTimelineKey,
                this.actionAtEpochMs, this.retryEligibilityAtEpochMs, this.candidateAttemptNo,
                this.orderedHeadBlocking, this.uncertainRetryAuthority, this.uncertainRetryControl,
                this.uncertainRetryControlPosition);
        this.workInstanceDigest = computeInstanceDigest(this.semanticWorkDigest, this.workKind,
                this.encodedTimelineKey, this.actionAtEpochMs, this.retryEligibilityAtEpochMs,
                this.candidateAttemptNo, this.orderedHeadBlocking, this.uncertainRetryAuthority,
                this.uncertainRetryControl, this.uncertainRetryControlPosition, this.runtimeRevision);
    }

    private TimelineWorkRef(final TimelineWorkKind workKind, final byte[] encodedTimelineKey,
                            final byte[] timelineKeySha256, final long actionAtEpochMs,
                            final long retryEligibilityAtEpochMs, final int candidateAttemptNo,
                            final long runtimeRevision, final boolean orderedHeadBlocking,
                            final UncertainRetryAuthority uncertainRetryAuthority,
                            final byte[] uncertainRetryControl, final byte[] uncertainRetryControlPosition,
                            final byte[] semanticWorkDigest, final byte[] workInstanceDigest) {
        this(workKind, encodedTimelineKey, actionAtEpochMs, retryEligibilityAtEpochMs, candidateAttemptNo,
                runtimeRevision, orderedHeadBlocking, uncertainRetryAuthority, uncertainRetryControl,
                uncertainRetryControlPosition);
        if (!Bytes.constantTimeEquals(this.timelineKeySha256, timelineKeySha256)
                || !Bytes.constantTimeEquals(this.semanticWorkDigest, semanticWorkDigest)
                || !Bytes.constantTimeEquals(this.workInstanceDigest, workInstanceDigest)) {
            throw new IllegalArgumentException("timeline work digest mismatch");
        }
    }

    public static TimelineWorkRef initial(final byte[] encodedTimelineKey, final long actionAtEpochMs,
                                          final long runtimeRevision) {
        return new TimelineWorkRef(TimelineWorkKind.INITIAL_SCHEDULE, encodedTimelineKey, actionAtEpochMs,
                actionAtEpochMs, 1, runtimeRevision, encodedTimelineKey[0] == 2,
                UncertainRetryAuthority.NONE, null, null);
    }

    public TimelineWorkKind workKind() {
        return workKind;
    }

    public byte[] encodedTimelineKey() {
        return Bytes.copy(encodedTimelineKey);
    }

    public byte[] timelineKeySha256() {
        return Bytes.copy(timelineKeySha256);
    }

    public long actionAtEpochMs() {
        return actionAtEpochMs;
    }

    public long retryEligibilityAtEpochMs() {
        return retryEligibilityAtEpochMs;
    }

    public int candidateAttemptNo() {
        return candidateAttemptNo;
    }

    public long runtimeRevision() {
        return runtimeRevision;
    }

    public boolean orderedHeadBlocking() {
        return orderedHeadBlocking;
    }

    public UncertainRetryAuthority uncertainRetryAuthority() {
        return uncertainRetryAuthority;
    }

    public byte[] uncertainRetryControl() {
        return Bytes.copy(uncertainRetryControl);
    }

    public byte[] uncertainRetryControlPosition() {
        return Bytes.copy(uncertainRetryControlPosition);
    }

    public byte[] semanticWorkDigest() {
        return Bytes.copy(semanticWorkDigest);
    }

    public byte[] workInstanceDigest() {
        return Bytes.copy(workInstanceDigest);
    }

    public byte[] canonicalBytes() {
        return canonicalFields(semanticWorkDigest, workInstanceDigest, runtimeRevision);
    }

    public static TimelineWorkRef decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = readAll(new CanonicalProtobuf.Reader(encoded));
        if (fields.size() < 11 || fields.size() > 13) {
            throw new IllegalArgumentException("timeline work fields are incomplete or unknown");
        }
        int index = 0;
        final TimelineWorkKind workKind = TimelineWorkKind.fromWire(unsigned(fields.get(index++), 1));
        final byte[] key = bytes(fields.get(index++), 2);
        final byte[] keyHash = fixed(fields.get(index++), 3, HASH_LENGTH);
        final long actionAt = signedNonNegative(fields.get(index++), 4);
        final long retryAt = signedNonNegative(fields.get(index++), 5);
        final long candidateAttempt = unsigned(fields.get(index++), 6);
        if (candidateAttempt <= 0 || candidateAttempt > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("invalid timeline candidate attempt");
        }
        final long runtimeRevision = unsigned(fields.get(index++), 7);
        if (runtimeRevision <= 0) {
            throw new IllegalArgumentException("timeline runtime revision must be positive");
        }
        final boolean ordered = bool(fields.get(index++), 8);
        final UncertainRetryAuthority authority = UncertainRetryAuthority.fromWire(unsigned(fields.get(index++), 9));
        byte[] control = null;
        byte[] controlPosition = null;
        if (index < fields.size() && fields.get(index).number() == 10) {
            control = bytes(fields.get(index++), 10);
        }
        if (index < fields.size() && fields.get(index).number() == 11) {
            controlPosition = bytes(fields.get(index++), 11);
        }
        final byte[] semanticDigest = fixed(fields.get(index++), 12, HASH_LENGTH);
        final byte[] instanceDigest = fixed(fields.get(index++), 13, HASH_LENGTH);
        if (index != fields.size()) {
            throw new IllegalArgumentException("unknown timeline work field");
        }
        final TimelineWorkRef result = new TimelineWorkRef(workKind, key, keyHash, actionAt, retryAt,
                (int) candidateAttempt, runtimeRevision, ordered, authority, control, controlPosition,
                semanticDigest, instanceDigest);
        if (!Arrays.equals(encoded, result.canonicalBytes())) {
            throw new IllegalArgumentException("non-canonical timeline work reference");
        }
        return result;
    }

    private byte[] canonicalFields(final byte[] semanticDigest, final byte[] instanceDigest,
                                   final long revision) {
        return canonicalFields(semanticDigest, instanceDigest, revision, this);
    }

    private static byte[] canonicalFields(final byte[] semanticDigest, final byte[] instanceDigest,
                                          final long revision, final TimelineWorkRef ref) {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, ref.workKind.wireValue());
            CanonicalProtobuf.bytes(output, 2, ref.encodedTimelineKey);
            CanonicalProtobuf.bytes(output, 3, ref.timelineKeySha256);
            CanonicalProtobuf.int64(output, 4, ref.actionAtEpochMs);
            CanonicalProtobuf.int64(output, 5, ref.retryEligibilityAtEpochMs);
            CanonicalProtobuf.uint32(output, 6, ref.candidateAttemptNo);
            CanonicalProtobuf.int64(output, 7, revision);
            CanonicalProtobuf.uint32(output, 8, ref.orderedHeadBlocking ? 1 : 0);
            CanonicalProtobuf.uint32(output, 9, ref.uncertainRetryAuthority.wireValue());
            if (ref.uncertainRetryControl.length != 0) {
                CanonicalProtobuf.bytes(output, 10, ref.uncertainRetryControl);
            }
            if (ref.uncertainRetryControlPosition.length != 0) {
                CanonicalProtobuf.bytes(output, 11, ref.uncertainRetryControlPosition);
            }
            CanonicalProtobuf.bytes(output, 12, semanticDigest);
            CanonicalProtobuf.bytes(output, 13, instanceDigest);
        });
    }

    private static byte[] computeSemanticDigest(final TimelineWorkKind kind, final byte[] key,
                                                final long actionAt, final long retryAt,
                                                final int candidateAttempt, final boolean ordered,
                                                final UncertainRetryAuthority authority,
                                                final byte[] control, final byte[] controlPosition) {
        final byte[] fields = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, kind.wireValue());
            CanonicalProtobuf.bytes(output, 2, key);
            CanonicalProtobuf.bytes(output, 3, Bytes.sha256(key));
            CanonicalProtobuf.int64(output, 4, actionAt);
            CanonicalProtobuf.int64(output, 5, retryAt);
            CanonicalProtobuf.uint32(output, 6, candidateAttempt);
            CanonicalProtobuf.uint32(output, 8, ordered ? 1 : 0);
            CanonicalProtobuf.uint32(output, 9, authority.wireValue());
            if (control != null && control.length != 0) {
                CanonicalProtobuf.bytes(output, 10, control);
            }
            if (controlPosition != null && controlPosition.length != 0) {
                CanonicalProtobuf.bytes(output, 11, controlPosition);
            }
        });
        return Bytes.sha256(Bytes.utf8("nereus-delay-timeline-work-semantic-v1\0"), fields);
    }

    private static byte[] computeInstanceDigest(final byte[] semanticDigest, final TimelineWorkKind kind,
                                                final byte[] key, final long actionAt, final long retryAt,
                                                final int candidateAttempt, final boolean ordered,
                                                final UncertainRetryAuthority authority, final byte[] control,
                                                final byte[] controlPosition, final long revision) {
        final byte[] fields = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, kind.wireValue());
            CanonicalProtobuf.bytes(output, 2, key);
            CanonicalProtobuf.bytes(output, 3, Bytes.sha256(key));
            CanonicalProtobuf.int64(output, 4, actionAt);
            CanonicalProtobuf.int64(output, 5, retryAt);
            CanonicalProtobuf.uint32(output, 6, candidateAttempt);
            CanonicalProtobuf.int64(output, 7, revision);
            CanonicalProtobuf.uint32(output, 8, ordered ? 1 : 0);
            CanonicalProtobuf.uint32(output, 9, authority.wireValue());
            if (control != null && control.length != 0) {
                CanonicalProtobuf.bytes(output, 10, control);
            }
            if (controlPosition != null && controlPosition.length != 0) {
                CanonicalProtobuf.bytes(output, 11, controlPosition);
            }
            CanonicalProtobuf.bytes(output, 12, semanticDigest);
        });
        return Bytes.sha256(Bytes.utf8("nereus-delay-timeline-work-instance-v1\0"), fields);
    }

    private void validateAuthority() {
        if (workKind != TimelineWorkKind.UNCERTAIN_RETRY && uncertainRetryAuthority
                != UncertainRetryAuthority.NONE) {
            throw new IllegalArgumentException("non-uncertain timeline work has retry authority");
        }
        if (uncertainRetryAuthority == UncertainRetryAuthority.NONE
                && (uncertainRetryControl.length != 0 || uncertainRetryControlPosition.length != 0)) {
            throw new IllegalArgumentException("retry control requires a retry authority");
        }
        if (uncertainRetryAuthority == UncertainRetryAuthority.PINNED_POLICY
                && (uncertainRetryControl.length != 0 || uncertainRetryControlPosition.length != 0)) {
            throw new IllegalArgumentException("pinned retry cannot carry a control reference");
        }
        if (uncertainRetryAuthority == UncertainRetryAuthority.CONTROL_OVERRIDE
                && (uncertainRetryControl.length == 0 || uncertainRetryControlPosition.length == 0)) {
            throw new IllegalArgumentException("control retry requires control and source position");
        }
        if (workKind == TimelineWorkKind.UNCERTAIN_RETRY && uncertainRetryAuthority == UncertainRetryAuthority.NONE) {
            throw new IllegalArgumentException("uncertain retry requires an authority");
        }
    }

    private static byte[] requireTimelineKey(final byte[] value) {
        Objects.requireNonNull(value, "encodedTimelineKey");
        if (value.length < 2 || (value[0] != 1 && value[0] != 2) || value[1] != 1) {
            throw new IllegalArgumentException("timeline key is not a V1 DUE/ORDERED key");
        }
        return Bytes.copy(value);
    }

    private static byte[] optional(final byte[] value) {
        return value == null ? new byte[0] : Bytes.copy(value);
    }

    private static List<CanonicalProtobuf.Reader.Field> readAll(final CanonicalProtobuf.Reader reader) {
        final List<CanonicalProtobuf.Reader.Field> fields = new ArrayList<>();
        while (reader.hasRemaining()) {
            fields.add(reader.next());
        }
        return fields;
    }

    private static long unsigned(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 0 || field.unsignedValue() < 0) {
            throw new IllegalArgumentException("invalid timeline work varint field " + number);
        }
        return field.unsignedValue();
    }

    private static long signedNonNegative(final CanonicalProtobuf.Reader.Field field, final int number) {
        return unsigned(field, number);
    }

    private static boolean bool(final CanonicalProtobuf.Reader.Field field, final int number) {
        final long value = unsigned(field, number);
        if (value > 1) {
            throw new IllegalArgumentException("invalid timeline work bool field " + number);
        }
        return value == 1;
    }

    private static byte[] bytes(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 2) {
            throw new IllegalArgumentException("invalid timeline work bytes field " + number);
        }
        return field.rawValue();
    }

    private static byte[] fixed(final CanonicalProtobuf.Reader.Field field, final int number, final int length) {
        final byte[] value = bytes(field, number);
        Bytes.requireLength(value, length, "timeline work field " + number);
        return value;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof TimelineWorkRef that && Arrays.equals(canonicalBytes(), that.canonicalBytes());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(canonicalBytes());
    }
}
