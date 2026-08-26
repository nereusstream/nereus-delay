package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Canonical typed value for the Registry LocalStoreResource branch. */
public final class LocalStoreResource {
    private static final int INCARNATION_LENGTH = 16;
    private static final int HASH_LENGTH = 32;

    private final ShardSubject shard;
    private final byte[] storeIncarnation;
    private final byte[] dbIdentity;
    private final byte[] absoluteRootPolicyDigest;

    public LocalStoreResource(
            final ShardSubject shard,
            final byte[] storeIncarnation,
            final byte[] dbIdentity,
            final byte[] absoluteRootPolicyDigest) {
        this.shard = Objects.requireNonNull(shard, "shard");
        this.storeIncarnation = fixed(storeIncarnation, INCARNATION_LENGTH, "storeIncarnation");
        this.dbIdentity = fixed(dbIdentity, HASH_LENGTH, "dbIdentity");
        this.absoluteRootPolicyDigest = fixed(absoluteRootPolicyDigest, HASH_LENGTH, "absoluteRootPolicyDigest");
    }

    public ShardSubject shard() {
        return shard;
    }

    public byte[] storeIncarnation() {
        return Bytes.copy(storeIncarnation);
    }

    public byte[] dbIdentity() {
        return Bytes.copy(dbIdentity);
    }

    public byte[] absoluteRootPolicyDigest() {
        return Bytes.copy(absoluteRootPolicyDigest);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, shard.canonicalBytes());
            CanonicalProtobuf.bytes(output, 2, storeIncarnation);
            CanonicalProtobuf.bytes(output, 3, dbIdentity);
            CanonicalProtobuf.bytes(output, 4, absoluteRootPolicyDigest);
        });
    }

    public byte[] exactResourceCanonicalBytes() {
        return CanonicalProtobuf.message(
                output -> CanonicalProtobuf.bytes(output, ResourceKind.LOCAL_STORE.wireValue(), canonicalBytes()));
    }

    public static LocalStoreResource decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "LocalStoreResource");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3, 4}, "LocalStoreResource");
        final LocalStoreResource result = new LocalStoreResource(
                ShardSubject.decode(QueryCodecSupport.nested(fields.get(0), 1)),
                QueryCodecSupport.fixed(fields.get(1), 2, INCARNATION_LENGTH),
                QueryCodecSupport.fixed(fields.get(2), 3, HASH_LENGTH),
                QueryCodecSupport.fixed(fields.get(3), 4, HASH_LENGTH));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "LocalStoreResource");
        return result;
    }

    private static byte[] fixed(final byte[] value, final int length, final String name) {
        Bytes.requireLength(value, length, name);
        return Bytes.copy(value);
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof LocalStoreResource that
                && shard.equals(that.shard)
                && Arrays.equals(storeIncarnation, that.storeIncarnation)
                && Arrays.equals(dbIdentity, that.dbIdentity)
                && Arrays.equals(absoluteRootPolicyDigest, that.absoluteRootPolicyDigest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                shard,
                Arrays.hashCode(storeIncarnation),
                Arrays.hashCode(dbIdentity),
                Arrays.hashCode(absoluteRootPolicyDigest));
    }
}
