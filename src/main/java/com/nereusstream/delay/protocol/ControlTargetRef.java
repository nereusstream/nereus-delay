package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Immutable target reference used by a prepared Control Operation.
 *
 * <p>The target digest covers fields 1--21 exactly. Operation-specific
 * target-presence and mutation-authority rules are intentionally evaluated by
 * the preparation layer, because this value does not carry an operation kind.</p>
 */
public final class ControlTargetRef {
    public static final int HASH_LENGTH = 32;
    public static final int ROUTE_UUID_LENGTH = RouteIncarnation.LENGTH;

    private final long targetIndex;
    private final ControlTargetKind targetKind;
    private final Object target;
    private final byte[] expectedMutationId;
    private final byte[] expectedMutationHash;
    private final byte[] targetDigest;

    public ControlTargetRef(
            final long targetIndex,
            final ControlTargetKind targetKind,
            final Object target,
            final byte[] expectedMutationId,
            final byte[] expectedMutationHash) {
        this(targetIndex, targetKind, target, expectedMutationId, expectedMutationHash, null);
    }

    private ControlTargetRef(
            final long targetIndex,
            final ControlTargetKind targetKind,
            final Object target,
            final byte[] expectedMutationId,
            final byte[] expectedMutationHash,
            final byte[] encodedDigest) {
        if (targetIndex < 0 || targetIndex > 0xffff_ffffL) {
            throw new IllegalArgumentException("targetIndex must be an unsigned uint32");
        }
        this.targetIndex = targetIndex;
        this.targetKind = Objects.requireNonNull(targetKind, "targetKind");
        this.target = copyTarget(requireTargetType(targetKind, target));
        if ((expectedMutationId == null) != (expectedMutationHash == null)) {
            throw new IllegalArgumentException("expected mutation identity fields must be both present or absent");
        }
        if (expectedMutationId != null) {
            Bytes.requireLength(expectedMutationId, HASH_LENGTH, "expectedMutationId");
            Bytes.requireLength(expectedMutationHash, HASH_LENGTH, "expectedMutationHash");
        }
        this.expectedMutationId = expectedMutationId == null ? null : Bytes.copy(expectedMutationId);
        this.expectedMutationHash = expectedMutationHash == null ? null : Bytes.copy(expectedMutationHash);
        final byte[] calculated = Bytes.sha256(canonicalWithoutDigest());
        if (encodedDigest != null) {
            Bytes.requireLength(encodedDigest, HASH_LENGTH, "targetDigest");
            if (!Bytes.constantTimeEquals(encodedDigest, calculated)) {
                throw new IllegalArgumentException("ControlTargetRef targetDigest mismatch");
            }
            this.targetDigest = Bytes.copy(encodedDigest);
        } else {
            this.targetDigest = calculated;
        }
    }

    public long targetIndex() {
        return targetIndex;
    }

    public ControlTargetKind targetKind() {
        return targetKind;
    }

    /** Returns the typed branch value, or the 16-byte route UUID for ROUTE. */
    public Object target() {
        return copyTarget(target);
    }

    public ShardSubject shard() {
        return target instanceof ShardSubject value ? value : null;
    }

    public LaneControlTarget lane() {
        return target instanceof LaneControlTarget value ? value : null;
    }

    public ControlMessageTarget message() {
        return target instanceof ControlMessageTarget value ? value : null;
    }

    public byte[] routeUuid() {
        return target instanceof byte[] value ? Bytes.copy(value) : null;
    }

    public ProfileControlTarget profile() {
        return target instanceof ProfileControlTarget value ? value : null;
    }

    public QuotaGrantRef quotaGrant() {
        return target instanceof QuotaGrantRef value ? value : null;
    }

    public byte[] expectedMutationId() {
        return expectedMutationId == null ? null : Bytes.copy(expectedMutationId);
    }

    public byte[] expectedMutationHash() {
        return expectedMutationHash == null ? null : Bytes.copy(expectedMutationHash);
    }

    public byte[] targetDigest() {
        return Bytes.copy(targetDigest);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            writeWithoutDigest(output);
            CanonicalProtobuf.bytes(output, 22, targetDigest);
        });
    }

    public static ControlTargetRef decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "ControlTargetRef");
        if (fields.size() != 4 && fields.size() != 6) {
            throw new IllegalArgumentException("invalid ControlTargetRef field count");
        }
        if (fields.get(0).number() != 1
                || fields.get(1).number() != 2
                || fields.get(fields.size() - 1).number() != 22) {
            throw new IllegalArgumentException("invalid ControlTargetRef field order");
        }
        final ControlTargetKind kind = ControlTargetKind.fromWire(QueryCodecSupport.uint(fields.get(1), 2));
        final int branchIndex = 2;
        final int branchField = fields.get(branchIndex).number();
        if (branchField != branchField(kind)) {
            throw new IllegalArgumentException("ControlTargetRef branch does not match target kind");
        }
        final Object target = decodeTarget(kind, fields.get(branchIndex));
        final boolean hasMutation = fields.size() == 6;
        if (hasMutation && (fields.get(3).number() != 20 || fields.get(4).number() != 21)) {
            throw new IllegalArgumentException("invalid ControlTargetRef mutation identity order");
        }
        final byte[] mutationId = hasMutation ? QueryCodecSupport.fixed(fields.get(3), 20, HASH_LENGTH) : null;
        final byte[] mutationHash = hasMutation ? QueryCodecSupport.fixed(fields.get(4), 21, HASH_LENGTH) : null;
        final byte[] digest = QueryCodecSupport.fixed(fields.get(fields.size() - 1), 22, HASH_LENGTH);
        final ControlTargetRef result = new ControlTargetRef(
                QueryCodecSupport.uint(fields.get(0), 1), kind, target, mutationId, mutationHash, digest);
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "ControlTargetRef");
        return result;
    }

    private byte[] canonicalWithoutDigest() {
        return CanonicalProtobuf.message(this::writeWithoutDigest);
    }

    private void writeWithoutDigest(final java.io.ByteArrayOutputStream output) {
        CanonicalProtobuf.uint32(output, 1, targetIndex);
        CanonicalProtobuf.uint32(output, 2, targetKind.wireValue());
        final int field = branchField(targetKind);
        if (targetKind == ControlTargetKind.ROUTE) {
            CanonicalProtobuf.bytes(output, field, (byte[]) target);
        } else {
            CanonicalProtobuf.bytes(output, field, targetCanonical(target));
        }
        if (expectedMutationId != null) {
            CanonicalProtobuf.bytes(output, 20, expectedMutationId);
            CanonicalProtobuf.bytes(output, 21, expectedMutationHash);
        }
    }

    private static Object decodeTarget(final ControlTargetKind kind, final CanonicalProtobuf.Reader.Field field) {
        return switch (kind) {
            case SHARD -> ShardSubject.decode(QueryCodecSupport.nested(field, 10));
            case LANE -> LaneControlTarget.decode(QueryCodecSupport.nested(field, 11));
            case MESSAGE -> ControlMessageTarget.decode(QueryCodecSupport.nested(field, 12));
            case ROUTE -> QueryCodecSupport.fixed(field, 13, ROUTE_UUID_LENGTH);
            case PROFILE -> ProfileControlTarget.decode(QueryCodecSupport.nested(field, 14));
            case QUOTA_GRANT -> QuotaGrantRef.decode(QueryCodecSupport.nested(field, 15));
        };
    }

    private static int branchField(final ControlTargetKind kind) {
        return 9 + kind.wireValue();
    }

    private static Object requireTargetType(final ControlTargetKind kind, final Object value) {
        Objects.requireNonNull(value, "target");
        final boolean valid =
                switch (kind) {
                    case SHARD -> value instanceof ShardSubject;
                    case LANE -> value instanceof LaneControlTarget;
                    case MESSAGE -> value instanceof ControlMessageTarget;
                    case ROUTE -> value instanceof byte[] && ((byte[]) value).length == ROUTE_UUID_LENGTH;
                    case PROFILE -> value instanceof ProfileControlTarget;
                    case QUOTA_GRANT -> value instanceof QuotaGrantRef;
                };
        if (!valid) {
            throw new IllegalArgumentException("ControlTargetRef target does not match target kind");
        }
        return value;
    }

    private static Object copyTarget(final Object value) {
        return value instanceof byte[] bytes ? Bytes.copy(bytes) : value;
    }

    private static byte[] targetCanonical(final Object value) {
        return switch (value) {
            case ShardSubject shard -> shard.canonicalBytes();
            case LaneControlTarget lane -> lane.canonicalBytes();
            case ControlMessageTarget message -> message.canonicalBytes();
            case ProfileControlTarget profile -> profile.canonicalBytes();
            case QuotaGrantRef quota -> quota.canonicalBytes();
            case byte[] ignored -> throw new IllegalArgumentException("route target is not nested");
            default -> throw new IllegalArgumentException("unknown ControlTargetRef target");
        };
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof ControlTargetRef that
                && targetIndex == that.targetIndex
                && targetKind == that.targetKind
                && targetEquals(target, that.target)
                && Arrays.equals(expectedMutationId, that.expectedMutationId)
                && Arrays.equals(expectedMutationHash, that.expectedMutationHash)
                && Arrays.equals(targetDigest, that.targetDigest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                targetIndex,
                targetKind,
                targetHash(target),
                Arrays.hashCode(expectedMutationId),
                Arrays.hashCode(expectedMutationHash),
                Arrays.hashCode(targetDigest));
    }

    private static boolean targetEquals(final Object left, final Object right) {
        return left instanceof byte[] leftBytes && right instanceof byte[] rightBytes
                ? Arrays.equals(leftBytes, rightBytes)
                : Objects.equals(left, right);
    }

    private static Object targetHash(final Object value) {
        return value instanceof byte[] bytes ? Arrays.hashCode(bytes) : value;
    }
}
