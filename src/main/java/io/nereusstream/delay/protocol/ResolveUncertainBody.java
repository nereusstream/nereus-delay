package io.nereusstream.delay.protocol;

import java.util.List;
import java.util.Objects;

/** Semantic parser for the source-ordered {@code ResolveUncertainV1} body. */
public final class ResolveUncertainBody {
    private static final int LANE_INCARNATION_LENGTH = 16;

    private final ControlRef controlRef;
    private final DestinationLaneId laneId;
    private final byte[] laneIncarnation;
    private final DelayMessageId messageId;
    private final int generation;
    private final byte[] publishAttemptId;
    private final int resolutionKind;
    private final byte[] evidence;
    private final boolean allowPossibleDuplicate;
    private final boolean allowPossibleDeliveryTerminal;
    private final byte[] acknowledgementHash;

    private ResolveUncertainBody(final ControlRef controlRef, final DestinationLaneId laneId,
                                 final byte[] laneIncarnation, final DelayMessageId messageId,
                                 final int generation, final byte[] publishAttemptId, final int resolutionKind,
                                 final byte[] evidence, final boolean allowPossibleDuplicate,
                                 final boolean allowPossibleDeliveryTerminal, final byte[] acknowledgementHash) {
        this.controlRef = Objects.requireNonNull(controlRef, "controlRef");
        this.laneId = Objects.requireNonNull(laneId, "laneId");
        Bytes.requireLength(laneIncarnation, LANE_INCARNATION_LENGTH, "laneIncarnation");
        this.laneIncarnation = Bytes.copy(laneIncarnation);
        this.messageId = Objects.requireNonNull(messageId, "messageId");
        if (generation < 0) {
            throw new IllegalArgumentException("generation must be non-negative");
        }
        this.generation = generation;
        Bytes.requireLength(publishAttemptId, ControlRef.HASH_LENGTH, "publishAttemptId");
        this.publishAttemptId = Bytes.copy(publishAttemptId);
        if (resolutionKind < 1 || resolutionKind > 4) {
            throw new IllegalArgumentException("unknown uncertain resolution kind");
        }
        this.resolutionKind = resolutionKind;
        this.evidence = Bytes.copy(evidence);
        this.allowPossibleDuplicate = allowPossibleDuplicate;
        this.allowPossibleDeliveryTerminal = allowPossibleDeliveryTerminal;
        this.acknowledgementHash = Bytes.copy(acknowledgementHash);
        validateCombination();
        if (resolutionKind == 1 || resolutionKind == 2) {
            final PublishEvidenceV1 publishEvidence = PublishEvidenceV1.decode(this.evidence);
            publishEvidence.requireBusinessMutation(this.publishAttemptId, resolutionKind == 1);
        }
    }

    public static ResolveUncertainBody decode(final byte[] canonicalBody) {
        final List<CanonicalProtobuf.Reader.Field> fields =
                SystemMutationBodyCodec.fields(SystemMutationType.RESOLVE_UNCERTAIN, canonicalBody);
        final ControlRef controlRef = ControlRef.decode(nested(field(fields, 10), 10));
        final DestinationLaneId lane = new DestinationLaneId(fixed(field(fields, 11), 11,
                DestinationLaneId.LENGTH));
        final byte[] laneIncarnation = fixed(field(fields, 12), 12, LANE_INCARNATION_LENGTH);
        final DelayMessageId messageId = new DelayMessageId(fixed(field(fields, 13), 13, DelayMessageId.LENGTH));
        final int generation = intValue(field(fields, 14), 14);
        final byte[] publishAttemptId = fixed(field(fields, 15), 15, ControlRef.HASH_LENGTH);
        final int kind = intValue(field(fields, 16), 16);
        final byte[] evidence = optionalNested(fields, 17);
        final boolean allowDuplicate = bool(field(fields, 18), 18);
        final boolean allowTerminal = bool(field(fields, 19), 19);
        final byte[] acknowledgement = optionalFixed(fields, 20, ControlRef.HASH_LENGTH);
        return new ResolveUncertainBody(controlRef, lane, laneIncarnation, messageId, generation, publishAttemptId,
                kind, evidence, allowDuplicate, allowTerminal, acknowledgement);
    }

    public ControlRef controlRef() {
        return controlRef;
    }

    public DestinationLaneId laneId() {
        return laneId;
    }

    public byte[] laneIncarnation() {
        return Bytes.copy(laneIncarnation);
    }

    public DelayMessageId messageId() {
        return messageId;
    }

    public int generation() {
        return generation;
    }

    public byte[] publishAttemptId() {
        return Bytes.copy(publishAttemptId);
    }

    public int resolutionKind() {
        return resolutionKind;
    }

    public byte[] evidence() {
        return Bytes.copy(evidence);
    }

    public boolean allowPossibleDuplicate() {
        return allowPossibleDuplicate;
    }

    public boolean allowPossibleDeliveryTerminal() {
        return allowPossibleDeliveryTerminal;
    }

    public byte[] acknowledgementHash() {
        return Bytes.copy(acknowledgementHash);
    }

    private void validateCombination() {
        switch (resolutionKind) {
            case 1, 2 -> {
                if (evidence.length == 0 || allowPossibleDuplicate || allowPossibleDeliveryTerminal
                        || acknowledgementHash.length != 0) {
                    throw new IllegalArgumentException("evidence resolution presence is invalid");
                }
            }
            case 3 -> {
                if (evidence.length != 0 || !allowPossibleDuplicate || allowPossibleDeliveryTerminal
                        || acknowledgementHash.length != ControlRef.HASH_LENGTH) {
                    throw new IllegalArgumentException("uncertain retry acknowledgement is invalid");
                }
            }
            case 4 -> {
                if (evidence.length != 0 || allowPossibleDuplicate || !allowPossibleDeliveryTerminal
                        || acknowledgementHash.length != ControlRef.HASH_LENGTH) {
                    throw new IllegalArgumentException("possible-delivery terminal acknowledgement is invalid");
                }
            }
            default -> throw new IllegalArgumentException("unknown uncertain resolution kind");
        }
    }

    private static CanonicalProtobuf.Reader.Field field(
            final List<CanonicalProtobuf.Reader.Field> fields, final int number) {
        for (int index = 3; index < fields.size(); index++) {
            if (fields.get(index).number() == number) {
                return fields.get(index);
            }
        }
        throw new IllegalArgumentException("missing ResolveUncertain field " + number);
    }

    private static int intValue(final CanonicalProtobuf.Reader.Field field, final int number) {
        final long value = unsigned(field, number);
        if (value > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("ResolveUncertain field exceeds Java int range " + number);
        }
        return (int) value;
    }

    private static long unsigned(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 0 || field.unsignedValue() < 0) {
            throw new IllegalArgumentException("invalid ResolveUncertain scalar field " + number);
        }
        return field.unsignedValue();
    }

    private static boolean bool(final CanonicalProtobuf.Reader.Field field, final int number) {
        final long value = unsigned(field, number);
        if (value > 1) {
            throw new IllegalArgumentException("invalid ResolveUncertain boolean field " + number);
        }
        return value == 1;
    }

    private static byte[] nested(final CanonicalProtobuf.Reader.Field field, final int number) {
        final byte[] value = bytes(field, number);
        if (value.length == 0) {
            throw new IllegalArgumentException("ResolveUncertain nested field must not be empty: " + number);
        }
        return value;
    }

    private static byte[] optionalNested(final List<CanonicalProtobuf.Reader.Field> fields, final int number) {
        for (int index = 3; index < fields.size(); index++) {
            if (fields.get(index).number() == number) {
                return nested(fields.get(index), number);
            }
        }
        return new byte[0];
    }

    private static byte[] optionalFixed(final List<CanonicalProtobuf.Reader.Field> fields, final int number,
                                        final int length) {
        for (int index = 3; index < fields.size(); index++) {
            if (fields.get(index).number() == number) {
                return fixed(fields.get(index), number, length);
            }
        }
        return new byte[0];
    }

    private static byte[] fixed(final CanonicalProtobuf.Reader.Field field, final int number, final int length) {
        final byte[] value = bytes(field, number);
        Bytes.requireLength(value, length, "ResolveUncertain field " + number);
        return value;
    }

    private static byte[] bytes(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 2) {
            throw new IllegalArgumentException("invalid ResolveUncertain bytes field " + number);
        }
        return field.rawValue();
    }
}
