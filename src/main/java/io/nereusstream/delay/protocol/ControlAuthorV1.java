package io.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Authenticated actor/role/resource-scope projection for a control operation. */
public final class ControlAuthorV1 {
    public static final int HASH_LENGTH = 32;

    private final byte[] operationActorIdHash;
    private final byte[] authenticatedRoleSetHash;
    private final byte[] tenantResourceScopeHash;

    public ControlAuthorV1(final byte[] operationActorIdHash, final byte[] authenticatedRoleSetHash,
                           final byte[] tenantResourceScopeHash) {
        this.operationActorIdHash = fixed(operationActorIdHash, "operationActorIdHash");
        this.authenticatedRoleSetHash = fixed(authenticatedRoleSetHash, "authenticatedRoleSetHash");
        this.tenantResourceScopeHash = fixed(tenantResourceScopeHash, "tenantResourceScopeHash");
    }

    public byte[] operationActorIdHash() {
        return Bytes.copy(operationActorIdHash);
    }

    public byte[] authenticatedRoleSetHash() {
        return Bytes.copy(authenticatedRoleSetHash);
    }

    public byte[] tenantResourceScopeHash() {
        return Bytes.copy(tenantResourceScopeHash);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, operationActorIdHash);
            CanonicalProtobuf.bytes(output, 2, authenticatedRoleSetHash);
            CanonicalProtobuf.bytes(output, 3, tenantResourceScopeHash);
        });
    }

    public static ControlAuthorV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "ControlAuthorV1");
        QueryCodecSupport.requireNumbers(fields, new int[]{1, 2, 3}, "ControlAuthorV1");
        final ControlAuthorV1 result = new ControlAuthorV1(
                QueryCodecSupport.fixed(fields.get(0), 1, HASH_LENGTH),
                QueryCodecSupport.fixed(fields.get(1), 2, HASH_LENGTH),
                QueryCodecSupport.fixed(fields.get(2), 3, HASH_LENGTH));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "ControlAuthorV1");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof ControlAuthorV1 that
                && Arrays.equals(operationActorIdHash, that.operationActorIdHash)
                && Arrays.equals(authenticatedRoleSetHash, that.authenticatedRoleSetHash)
                && Arrays.equals(tenantResourceScopeHash, that.tenantResourceScopeHash);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(operationActorIdHash), Arrays.hashCode(authenticatedRoleSetHash),
                Arrays.hashCode(tenantResourceScopeHash));
    }

    private static byte[] fixed(final byte[] value, final String name) {
        Bytes.requireLength(value, HASH_LENGTH, name);
        return Bytes.copy(value);
    }
}
