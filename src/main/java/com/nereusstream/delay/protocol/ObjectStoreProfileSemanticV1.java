package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Immutable Object Store Profile semantic body from Registry §5.1.1. */
public final class ObjectStoreProfileSemanticV1 implements ProfileSemanticBodyV1 {
    public static final int SCHEMA_VERSION = 1;
    public static final int CREDENTIAL_BINDING_PROTOCOL_VERSION = 1;
    public static final int SINGLE_PUT = 0x01;
    public static final int MULTIPART = 0x02;
    public static final int ALLOWED_UPLOAD_HANDLE_BITS = SINGLE_PUT | MULTIPART;
    public static final int HASH_LENGTH = 32;

    private final ObjectStoreProviderKindV1 providerKind;
    private final byte[] endpointConfigDigest;
    private final byte[] credentialAuthorizationScopeDigest;
    private final int credentialBindingProtocolVersion;
    private final boolean requireIfAbsentCreate;
    private final boolean requireImmutableVersion;
    private final boolean requireExactVersionDelete;
    private final boolean requireSha256Verification;
    private final byte[] encryptionPolicyDigest;
    private final long maxObjectBytes;
    private final int allowedUploadHandleBits;
    private final int adapterConformanceVersion;
    private final byte[] lifecyclePolicyDigest;

    public ObjectStoreProfileSemanticV1(
            final ObjectStoreProviderKindV1 providerKind,
            final byte[] endpointConfigDigest,
            final byte[] credentialAuthorizationScopeDigest,
            final int credentialBindingProtocolVersion,
            final boolean requireIfAbsentCreate,
            final boolean requireImmutableVersion,
            final boolean requireExactVersionDelete,
            final boolean requireSha256Verification,
            final byte[] encryptionPolicyDigest,
            final long maxObjectBytes,
            final int allowedUploadHandleBits,
            final int adapterConformanceVersion,
            final byte[] lifecyclePolicyDigest) {
        this.providerKind = Objects.requireNonNull(providerKind, "providerKind");
        this.endpointConfigDigest = fixed(endpointConfigDigest, "endpointConfigDigest");
        this.credentialAuthorizationScopeDigest =
                fixed(credentialAuthorizationScopeDigest, "credentialAuthorizationScopeDigest");
        if (credentialBindingProtocolVersion != CREDENTIAL_BINDING_PROTOCOL_VERSION) {
            throw new IllegalArgumentException("unsupported Object Store credential binding protocol");
        }
        this.credentialBindingProtocolVersion = credentialBindingProtocolVersion;
        if (!requireIfAbsentCreate
                || !requireImmutableVersion
                || !requireExactVersionDelete
                || !requireSha256Verification) {
            throw new IllegalArgumentException("V1 Object Store safety requirements are mandatory");
        }
        this.requireIfAbsentCreate = requireIfAbsentCreate;
        this.requireImmutableVersion = requireImmutableVersion;
        this.requireExactVersionDelete = requireExactVersionDelete;
        this.requireSha256Verification = requireSha256Verification;
        this.encryptionPolicyDigest = fixed(encryptionPolicyDigest, "encryptionPolicyDigest");
        if (maxObjectBytes <= 0) {
            throw new IllegalArgumentException("max object bytes must be positive");
        }
        this.maxObjectBytes = maxObjectBytes;
        if (allowedUploadHandleBits <= 0 || (allowedUploadHandleBits & ~ALLOWED_UPLOAD_HANDLE_BITS) != 0) {
            throw new IllegalArgumentException("invalid Object Store upload handle bits");
        }
        this.allowedUploadHandleBits = allowedUploadHandleBits;
        if (adapterConformanceVersion <= 0) {
            throw new IllegalArgumentException("Object Store adapter conformance version must be positive");
        }
        this.adapterConformanceVersion = adapterConformanceVersion;
        this.lifecyclePolicyDigest = fixed(lifecyclePolicyDigest, "lifecyclePolicyDigest");
    }

    @Override
    public ProfileKindV1 profileKind() {
        return ProfileKindV1.OBJECT_STORE;
    }

    @Override
    public int schemaVersion() {
        return SCHEMA_VERSION;
    }

    public ObjectStoreProviderKindV1 providerKind() {
        return providerKind;
    }

    public byte[] endpointConfigDigest() {
        return Bytes.copy(endpointConfigDigest);
    }

    public byte[] credentialAuthorizationScopeDigest() {
        return Bytes.copy(credentialAuthorizationScopeDigest);
    }

    public int credentialBindingProtocolVersion() {
        return credentialBindingProtocolVersion;
    }

    public boolean requireIfAbsentCreate() {
        return requireIfAbsentCreate;
    }

    public boolean requireImmutableVersion() {
        return requireImmutableVersion;
    }

    public boolean requireExactVersionDelete() {
        return requireExactVersionDelete;
    }

    public boolean requireSha256Verification() {
        return requireSha256Verification;
    }

    public byte[] encryptionPolicyDigest() {
        return Bytes.copy(encryptionPolicyDigest);
    }

    public long maxObjectBytes() {
        return maxObjectBytes;
    }

    public int allowedUploadHandleBits() {
        return allowedUploadHandleBits;
    }

    public int adapterConformanceVersion() {
        return adapterConformanceVersion;
    }

    public byte[] lifecyclePolicyDigest() {
        return Bytes.copy(lifecyclePolicyDigest);
    }

    @Override
    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, providerKind.wireValue());
            CanonicalProtobuf.bytes(output, 2, endpointConfigDigest);
            CanonicalProtobuf.bytes(output, 3, credentialAuthorizationScopeDigest);
            CanonicalProtobuf.uint32(output, 4, credentialBindingProtocolVersion);
            CanonicalProtobuf.uint32(output, 5, requireIfAbsentCreate ? 1 : 0);
            CanonicalProtobuf.uint32(output, 6, requireImmutableVersion ? 1 : 0);
            CanonicalProtobuf.uint32(output, 7, requireExactVersionDelete ? 1 : 0);
            CanonicalProtobuf.uint32(output, 8, requireSha256Verification ? 1 : 0);
            CanonicalProtobuf.bytes(output, 9, encryptionPolicyDigest);
            CanonicalProtobuf.uint64(output, 10, maxObjectBytes);
            CanonicalProtobuf.uint32(output, 11, allowedUploadHandleBits);
            CanonicalProtobuf.uint32(output, 12, adapterConformanceVersion);
            CanonicalProtobuf.bytes(output, 13, lifecyclePolicyDigest);
        });
    }

    public static ObjectStoreProfileSemanticV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields =
                QueryCodecSupport.read(encoded, "ObjectStoreProfileSemanticV1");
        QueryCodecSupport.requireNumbers(
                fields, new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13}, "ObjectStoreProfileSemanticV1");
        final ObjectStoreProfileSemanticV1 result = new ObjectStoreProfileSemanticV1(
                ObjectStoreProviderKindV1.fromWire(QueryCodecSupport.uint(fields.get(0), 1)),
                QueryCodecSupport.fixed(fields.get(1), 2, HASH_LENGTH),
                QueryCodecSupport.fixed(fields.get(2), 3, HASH_LENGTH),
                QueryCodecSupport.uint32(fields.get(3), 4),
                QueryCodecSupport.bool(fields.get(4), 5),
                QueryCodecSupport.bool(fields.get(5), 6),
                QueryCodecSupport.bool(fields.get(6), 7),
                QueryCodecSupport.bool(fields.get(7), 8),
                QueryCodecSupport.fixed(fields.get(8), 9, HASH_LENGTH),
                QueryCodecSupport.uint(fields.get(9), 10),
                QueryCodecSupport.uint32(fields.get(10), 11),
                QueryCodecSupport.uint32(fields.get(11), 12),
                QueryCodecSupport.fixed(fields.get(12), 13, HASH_LENGTH));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "ObjectStoreProfileSemanticV1");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof ObjectStoreProfileSemanticV1 that
                && providerKind == that.providerKind
                && Arrays.equals(endpointConfigDigest, that.endpointConfigDigest)
                && Arrays.equals(credentialAuthorizationScopeDigest, that.credentialAuthorizationScopeDigest)
                && credentialBindingProtocolVersion == that.credentialBindingProtocolVersion
                && requireIfAbsentCreate == that.requireIfAbsentCreate
                && requireImmutableVersion == that.requireImmutableVersion
                && requireExactVersionDelete == that.requireExactVersionDelete
                && requireSha256Verification == that.requireSha256Verification
                && Arrays.equals(encryptionPolicyDigest, that.encryptionPolicyDigest)
                && maxObjectBytes == that.maxObjectBytes
                && allowedUploadHandleBits == that.allowedUploadHandleBits
                && adapterConformanceVersion == that.adapterConformanceVersion
                && Arrays.equals(lifecyclePolicyDigest, that.lifecyclePolicyDigest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                providerKind,
                Arrays.hashCode(endpointConfigDigest),
                Arrays.hashCode(credentialAuthorizationScopeDigest),
                credentialBindingProtocolVersion,
                requireIfAbsentCreate,
                requireImmutableVersion,
                requireExactVersionDelete,
                requireSha256Verification,
                Arrays.hashCode(encryptionPolicyDigest),
                maxObjectBytes,
                allowedUploadHandleBits,
                adapterConformanceVersion,
                Arrays.hashCode(lifecyclePolicyDigest));
    }

    private static byte[] fixed(final byte[] value, final String name) {
        Bytes.requireLength(value, HASH_LENGTH, name);
        if (allZero(value)) {
            throw new IllegalArgumentException(name + " must be non-zero");
        }
        return Bytes.copy(value);
    }

    private static boolean allZero(final byte[] value) {
        for (byte element : value) {
            if (element != 0) {
                return false;
            }
        }
        return true;
    }
}
