package io.nereusstream.delay.protocol;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Semantic parser for the lane-gate subset of {@code ApplyShardControlV1}. */
public final class ApplyShardControlBody {
    private final ControlRef controlRef;
    private final int controlKind;
    private final long semanticVersion;
    private final byte[] semanticHash;
    private final Long expectedPriorControlVersion;
    private final byte[] payload;

    private ApplyShardControlBody(final ControlRef controlRef, final int controlKind, final long semanticVersion,
                                  final byte[] semanticHash, final Long expectedPriorControlVersion,
                                  final byte[] payload) {
        this.controlRef = Objects.requireNonNull(controlRef, "controlRef");
        if (controlKind < 1 || controlKind > 14 || semanticVersion <= 0) {
            throw new IllegalArgumentException("invalid shard control kind/version");
        }
        this.controlKind = controlKind;
        this.semanticVersion = semanticVersion;
        Bytes.requireLength(semanticHash, ControlRef.HASH_LENGTH, "semanticHash");
        this.semanticHash = Bytes.copy(semanticHash);
        this.expectedPriorControlVersion = expectedPriorControlVersion;
        this.payload = copyNested(payload, "controlPayload");
    }

    public static ApplyShardControlBody decode(final byte[] canonicalBody) {
        final List<CanonicalProtobuf.Reader.Field> fields =
                SystemMutationBodyCodec.fields(SystemMutationType.APPLY_SHARD_CONTROL, canonicalBody);
        final ControlRef controlRef = ControlRef.decode(nested(field(fields, 10), 10));
        final int controlKind = intValue(field(fields, 11), 11);
        final long semanticVersion = unsigned(field(fields, 12), 12);
        final byte[] semanticHash = fixed(field(fields, 13), 13, ControlRef.HASH_LENGTH);
        final Long expectedPrior = optionalUnsigned(fields, 14);
        final byte[] payload = nested(field(fields, 15), 15);
        final ApplyShardControlBody result = new ApplyShardControlBody(controlRef, controlKind, semanticVersion,
                semanticHash, expectedPrior, payload);
        result.validatePayloadBranch();
        return result;
    }

    public ControlRef controlRef() {
        return controlRef;
    }

    public int controlKind() {
        return controlKind;
    }

    public long semanticVersion() {
        return semanticVersion;
    }

    public byte[] semanticHash() {
        return Bytes.copy(semanticHash);
    }

    public Long expectedPriorControlVersion() {
        return expectedPriorControlVersion;
    }

    public byte[] payload() {
        return Bytes.copy(payload);
    }

    public LaneTarget laneTarget() {
        if (controlKind < 8 || controlKind > 11) {
            throw new IllegalArgumentException("control kind is not a lane operation");
        }
        final List<CanonicalProtobuf.Reader.Field> branches = readAll(new CanonicalProtobuf.Reader(payload));
        final byte[] branch = bytes(branches, controlKind);
        final List<CanonicalProtobuf.Reader.Field> fields = readAll(new CanonicalProtobuf.Reader(branch));
        final int expectedFieldCount = controlKind == 11 ? 5 : 2;
        if (fields.size() != expectedFieldCount || fields.get(0).number() != 1 || fields.get(0).wireType() != 2
                || fields.get(1).number() != 2 || fields.get(1).wireType() != 2) {
            throw new IllegalArgumentException("lane control payload is incomplete");
        }
        final List<CanonicalProtobuf.Reader.Field> target = readAll(
                new CanonicalProtobuf.Reader(fields.get(0).rawValue()));
        if (target.size() != 3) {
            throw new IllegalArgumentException("LaneControlTarget fields are incomplete or unknown");
        }
        final byte[] laneId = fixed(target.get(0), 1, DestinationLaneId.LENGTH);
        final byte[] laneIncarnation = fixed(target.get(1), 2, 16);
        final long expectedVersion = unsigned(target.get(2), 3);
        if (controlKind == 8 || controlKind == 9) {
            validateControlReason(fields.get(1).rawValue());
        }
        return new LaneTarget(new DestinationLaneId(laneId), laneIncarnation, expectedVersion);
    }

    private static void validateControlReason(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = readAll(new CanonicalProtobuf.Reader(encoded));
        if (fields.size() < 1 || fields.size() > 3 || fields.get(0).number() != 1
                || fields.get(0).wireType() != 0 || fields.get(0).unsignedValue() <= 0) {
            throw new IllegalArgumentException("invalid ControlReason");
        }
        for (int index = 1; index < fields.size(); index++) {
            final CanonicalProtobuf.Reader.Field field = fields.get(index);
            if ((field.number() != 2 && field.number() != 3) || field.wireType() != 2) {
                throw new IllegalArgumentException("invalid ControlReason field");
            }
            Bytes.requireLength(field.rawValue(), ControlRef.HASH_LENGTH, "ControlReason hash");
        }
    }

    private void validatePayloadBranch() {
        final List<CanonicalProtobuf.Reader.Field> branches = readAll(new CanonicalProtobuf.Reader(payload));
        if (branches.size() != 1 || branches.get(0).number() != controlKind || branches.get(0).wireType() != 2) {
            throw new IllegalArgumentException("ControlPayload branch does not match control kind");
        }
        readAll(new CanonicalProtobuf.Reader(branches.get(0).rawValue()));
    }

    private static List<CanonicalProtobuf.Reader.Field> readAll(final CanonicalProtobuf.Reader reader) {
        final List<CanonicalProtobuf.Reader.Field> fields = new ArrayList<>();
        while (reader.hasRemaining()) {
            fields.add(reader.next());
        }
        return fields;
    }

    private static CanonicalProtobuf.Reader.Field field(
            final List<CanonicalProtobuf.Reader.Field> fields, final int number) {
        for (int index = 3; index < fields.size(); index++) {
            if (fields.get(index).number() == number) {
                return fields.get(index);
            }
        }
        throw new IllegalArgumentException("missing ApplyShardControl field " + number);
    }

    private static Long optionalUnsigned(final List<CanonicalProtobuf.Reader.Field> fields, final int number) {
        for (int index = 3; index < fields.size(); index++) {
            if (fields.get(index).number() == number) {
                return unsigned(fields.get(index), number);
            }
        }
        return null;
    }

    private static int intValue(final CanonicalProtobuf.Reader.Field field, final int number) {
        final long value = unsigned(field, number);
        if (value > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("ApplyShardControl field exceeds Java int range " + number);
        }
        return (int) value;
    }

    private static long unsigned(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 0 || field.unsignedValue() < 0) {
            throw new IllegalArgumentException("invalid ApplyShardControl scalar field " + number);
        }
        return field.unsignedValue();
    }

    private static byte[] nested(final CanonicalProtobuf.Reader.Field field, final int number) {
        final byte[] value = bytes(field, number);
        if (value.length == 0) {
            throw new IllegalArgumentException("ApplyShardControl nested field must not be empty: " + number);
        }
        readAll(new CanonicalProtobuf.Reader(value));
        return value;
    }

    private static byte[] copyNested(final byte[] value, final String name) {
        if (value.length == 0) {
            throw new IllegalArgumentException(name + " must not be empty");
        }
        return Bytes.copy(value);
    }

    private static byte[] bytes(final List<CanonicalProtobuf.Reader.Field> fields, final int number) {
        for (CanonicalProtobuf.Reader.Field field : fields) {
            if (field.number() == number) {
                if (field.wireType() != 2 || field.rawValue().length == 0) {
                    throw new IllegalArgumentException("invalid ControlPayload branch " + number);
                }
                return field.rawValue();
            }
        }
        throw new IllegalArgumentException("missing ControlPayload branch " + number);
    }

    private static byte[] bytes(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 2) {
            throw new IllegalArgumentException("invalid ApplyShardControl bytes field " + number);
        }
        return field.rawValue();
    }

    private static byte[] fixed(final CanonicalProtobuf.Reader.Field field, final int number, final int length) {
        final byte[] value = bytes(field, number);
        Bytes.requireLength(value, length, "ApplyShardControl field " + number);
        return value;
    }

    public record LaneTarget(DestinationLaneId laneId, byte[] laneIncarnation, long expectedControlVersion) {
        public LaneTarget {
            Objects.requireNonNull(laneId, "laneId");
            Bytes.requireLength(laneIncarnation, 16, "laneIncarnation");
            if (expectedControlVersion <= 0) {
                throw new IllegalArgumentException("expected lane control version must be positive");
            }
            laneIncarnation = Bytes.copy(laneIncarnation);
        }

        @Override
        public byte[] laneIncarnation() {
            return Bytes.copy(laneIncarnation);
        }
    }
}
