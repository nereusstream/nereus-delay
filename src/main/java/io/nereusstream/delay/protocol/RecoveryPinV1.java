package io.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Canonical session-bound recovery pin projection. */
public final class RecoveryPinV1 {
    private static final int ID_LENGTH = 16;
    private static final int HASH_LENGTH = 32;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-recovery-pin-v1\0");

    private final byte[] pinId;
    private final ShardSubjectV1 shard;
    private final OwnerIdentityV1 owner;
    private final RecoveryCandidateRefV1 candidate;
    private final RecoveryFloorRefV1 observedFloor;
    private final long observedCatalogGeneration;
    private final byte[] oxiaSessionIdentityDigest;
    private final byte[] pinDigest;

    public RecoveryPinV1(final byte[] pinId, final ShardSubjectV1 shard, final OwnerIdentityV1 owner,
                         final RecoveryCandidateRefV1 candidate, final RecoveryFloorRefV1 observedFloor,
                         final long observedCatalogGeneration, final byte[] oxiaSessionIdentityDigest) {
        this.pinId = nonZeroFixed(pinId, ID_LENGTH, "pinId");
        this.shard = Objects.requireNonNull(shard, "shard");
        this.owner = Objects.requireNonNull(owner, "owner");
        this.candidate = Objects.requireNonNull(candidate, "candidate");
        this.observedFloor = Objects.requireNonNull(observedFloor, "observedFloor");
        if (observedCatalogGeneration <= 0
                || observedCatalogGeneration != observedFloor.catalogGeneration()) {
            throw new IllegalArgumentException("pin catalog generation must equal observed Floor");
        }
        this.observedCatalogGeneration = observedCatalogGeneration;
        this.oxiaSessionIdentityDigest = fixed(oxiaSessionIdentityDigest, HASH_LENGTH,
                "oxiaSessionIdentityDigest");
        if (!Arrays.equals(candidate.recoveryLineageId(), observedFloor.recoveryLineageId())) {
            throw new IllegalArgumentException("candidate and Floor lineage differ");
        }
        this.pinDigest = Bytes.sha256(DIGEST_DOMAIN, fieldsOneToEight());
    }

    public byte[] pinId() {
        return Bytes.copy(pinId);
    }

    public ShardSubjectV1 shard() {
        return shard;
    }

    public OwnerIdentityV1 owner() {
        return owner;
    }

    public RecoveryCandidateRefV1 candidate() {
        return candidate;
    }

    public RecoveryFloorRefV1 observedFloor() {
        return observedFloor;
    }

    public long observedCatalogGeneration() {
        return observedCatalogGeneration;
    }

    public byte[] oxiaSessionIdentityDigest() {
        return Bytes.copy(oxiaSessionIdentityDigest);
    }

    public byte[] pinDigest() {
        return Bytes.copy(pinDigest);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            output.writeBytes(fieldsOneToEight());
            CanonicalProtobuf.bytes(output, 9, pinDigest);
        });
    }

    public static RecoveryPinV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "RecoveryPinV1");
        QueryCodecSupport.requireNumbers(fields, new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9}, "RecoveryPinV1");
        if (QueryCodecSupport.uint32(fields.get(0), 1) != 1) {
            throw new IllegalArgumentException("unsupported RecoveryPinV1 version");
        }
        final RecoveryPinV1 result = new RecoveryPinV1(
                QueryCodecSupport.fixed(fields.get(1), 2, ID_LENGTH),
                ShardSubjectV1.decode(QueryCodecSupport.nested(fields.get(2), 3)),
                OwnerIdentityV1.decode(QueryCodecSupport.nested(fields.get(3), 4)),
                RecoveryCandidateRefV1.decode(QueryCodecSupport.nested(fields.get(4), 5)),
                RecoveryFloorRefV1.decode(QueryCodecSupport.nested(fields.get(5), 6)),
                QueryCodecSupport.uint(fields.get(6), 7),
                QueryCodecSupport.fixed(fields.get(7), 8, HASH_LENGTH));
        final byte[] digest = QueryCodecSupport.fixed(fields.get(8), 9, HASH_LENGTH);
        if (!Bytes.constantTimeEquals(digest, result.pinDigest)) {
            throw new IllegalArgumentException("RecoveryPinV1 digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "RecoveryPinV1");
        return result;
    }

    private byte[] fieldsOneToEight() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, 1);
            CanonicalProtobuf.bytes(output, 2, pinId);
            CanonicalProtobuf.bytes(output, 3, shard.canonicalBytes());
            CanonicalProtobuf.bytes(output, 4, owner.canonicalBytes());
            CanonicalProtobuf.bytes(output, 5, candidate.canonicalBytes());
            CanonicalProtobuf.bytes(output, 6, observedFloor.canonicalBytes());
            CanonicalProtobuf.uint64(output, 7, observedCatalogGeneration);
            CanonicalProtobuf.bytes(output, 8, oxiaSessionIdentityDigest);
        });
    }

    private static byte[] fixed(final byte[] value, final int length, final String name) {
        Bytes.requireLength(value, length, name);
        return Bytes.copy(value);
    }

    private static byte[] nonZeroFixed(final byte[] value, final int length, final String name) {
        final byte[] result = fixed(value, length, name);
        if (Arrays.stream(toIntArray(result)).allMatch(item -> item == 0)) {
            throw new IllegalArgumentException(name + " must be non-zero");
        }
        return result;
    }

    private static int[] toIntArray(final byte[] value) {
        final int[] result = new int[value.length];
        for (int index = 0; index < value.length; index++) {
            result[index] = value[index];
        }
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof RecoveryPinV1 that
                && Arrays.equals(canonicalBytes(), that.canonicalBytes());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(canonicalBytes());
    }
}
