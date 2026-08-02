package io.nereusstream.delay.protocol;

import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Canonical Registry proof that a reserved immutable Object Store payload was attested. */
public final class PayloadCommitProofV1 implements PayloadCommitProofView {
    private static final int PROOF_VERSION = 1;
    private static final int HASH_LENGTH = 32;
    private static final int ROUTE_LENGTH = 16;
    private static final int SIGNATURE_LENGTH = 64;

    private final byte[] reservationId;
    private final byte[] tenantRoutingScope;
    private final byte[] routeIncarnationUuid;
    private final int partition;
    private final DelayMessageId delayMessageId;
    private final ProfileRefV1 objectStoreProfile;
    private final long trustSetVersion;
    private final int proofKeyVersion;
    private final byte[] container;
    private final byte[] objectKey;
    private final byte[] immutableObjectVersion;
    private final byte[] etag;
    private final long length;
    private final byte[] payloadSha256;
    private final long notAfterEpochMs;
    private final byte[] proofId;
    private final byte[] signature;

    public PayloadCommitProofV1(final byte[] reservationId, final byte[] tenantRoutingScope,
                                final byte[] routeIncarnationUuid, final int partition,
                                final DelayMessageId delayMessageId, final ProfileRefV1 objectStoreProfile,
                                final long trustSetVersion, final int proofKeyVersion, final byte[] container,
                                final byte[] objectKey, final byte[] immutableObjectVersion, final byte[] etag,
                                final long length, final byte[] payloadSha256, final long notAfterEpochMs,
                                final byte[] proofId, final byte[] signature) {
        this.reservationId = fixedNonZero(reservationId, "reservationId");
        Bytes.requireLength(tenantRoutingScope, HASH_LENGTH, "tenantRoutingScope");
        this.tenantRoutingScope = Bytes.copy(tenantRoutingScope);
        Bytes.requireLength(routeIncarnationUuid, ROUTE_LENGTH, "routeIncarnationUuid");
        this.routeIncarnationUuid = Bytes.copy(routeIncarnationUuid);
        if (partition < 0 || trustSetVersion <= 0 || proofKeyVersion <= 0 || length < 0
                || notAfterEpochMs < 0) {
            throw new IllegalArgumentException("invalid payload commit proof");
        }
        this.partition = partition;
        this.delayMessageId = Objects.requireNonNull(delayMessageId, "delayMessageId");
        if (!Arrays.equals(this.routeIncarnationUuid, delayMessageId.routingId().shardId().routeIncarnation().bytes())
                || partition != delayMessageId.routingId().shardId().partition()) {
            throw new IllegalArgumentException("payload proof shard identity mismatch");
        }
        this.objectStoreProfile = Objects.requireNonNull(objectStoreProfile, "objectStoreProfile");
        if (objectStoreProfile.profileKind() != ProfileKindV1.OBJECT_STORE) {
            throw new IllegalArgumentException("objectStoreProfile must have OBJECT_STORE kind");
        }
        this.trustSetVersion = trustSetVersion;
        this.proofKeyVersion = proofKeyVersion;
        this.container = nonEmpty(container, "container");
        this.objectKey = nonEmpty(objectKey, "objectKey");
        this.immutableObjectVersion = nonEmpty(immutableObjectVersion, "immutableObjectVersion");
        this.etag = etag == null ? null : Bytes.copy(etag);
        this.length = length;
        Bytes.requireLength(payloadSha256, HASH_LENGTH, "payloadSha256");
        this.payloadSha256 = Bytes.copy(payloadSha256);
        this.notAfterEpochMs = notAfterEpochMs;
        this.proofId = fixedNonZero(proofId, "proofId");
        Bytes.requireLength(signature, SIGNATURE_LENGTH, "signature");
        this.signature = Bytes.copy(signature);
        if (!Bytes.constantTimeEquals(this.proofId, computeProofId())) {
            throw new IllegalArgumentException("payload proof id mismatch");
        }
    }

    public byte[] reservationId() {
        return Bytes.copy(reservationId);
    }

    public byte[] tenantRoutingScope() {
        return Bytes.copy(tenantRoutingScope);
    }

    public byte[] routeIncarnationUuid() {
        return Bytes.copy(routeIncarnationUuid);
    }

    public int partition() {
        return partition;
    }

    public DelayMessageId delayMessageId() {
        return delayMessageId;
    }

    public ProfileRefV1 objectStoreProfile() {
        return objectStoreProfile;
    }

    public long trustSetVersion() {
        return trustSetVersion;
    }

    public int proofKeyVersion() {
        return proofKeyVersion;
    }

    public byte[] container() {
        return Bytes.copy(container);
    }

    public byte[] objectKey() {
        return Bytes.copy(objectKey);
    }

    public byte[] immutableObjectVersion() {
        return Bytes.copy(immutableObjectVersion);
    }

    public byte[] etag() {
        return etag == null ? null : Bytes.copy(etag);
    }

    public long length() {
        return length;
    }

    public byte[] payloadSha256() {
        return Bytes.copy(payloadSha256);
    }

    public long notAfterEpochMs() {
        return notAfterEpochMs;
    }

    public byte[] proofId() {
        return Bytes.copy(proofId);
    }

    public byte[] signature() {
        return Bytes.copy(signature);
    }

    @Override
    public byte[] objectStoreProfileHash() {
        return objectStoreProfile.semanticHash();
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, PROOF_VERSION);
            CanonicalProtobuf.bytes(output, 2, reservationId);
            CanonicalProtobuf.bytes(output, 3, tenantRoutingScope);
            CanonicalProtobuf.bytes(output, 4, routeIncarnationUuid);
            CanonicalProtobuf.uint32(output, 5, partition);
            CanonicalProtobuf.bytes(output, 6, delayMessageId.bytes());
            CanonicalProtobuf.bytes(output, 7, objectStoreProfile.canonicalBytes());
            CanonicalProtobuf.uint64(output, 8, trustSetVersion);
            CanonicalProtobuf.uint32(output, 9, proofKeyVersion);
            CanonicalProtobuf.bytes(output, 10, container);
            CanonicalProtobuf.bytes(output, 11, objectKey);
            CanonicalProtobuf.bytes(output, 12, immutableObjectVersion);
            if (etag != null) {
                CanonicalProtobuf.bytes(output, 13, etag);
            }
            CanonicalProtobuf.uint64(output, 14, length);
            CanonicalProtobuf.bytes(output, 15, payloadSha256);
            CanonicalProtobuf.int64(output, 16, notAfterEpochMs);
            CanonicalProtobuf.bytes(output, 17, proofId);
            CanonicalProtobuf.bytes(output, 18, signature);
        });
    }

    public boolean verifySignature(final PublicKey publicKey) {
        Objects.requireNonNull(publicKey, "publicKey");
        try {
            final Signature verifier = Signature.getInstance("Ed25519");
            verifier.initVerify(publicKey);
            verifier.update(signatureDigest());
            return verifier.verify(signature);
        } catch (GeneralSecurityException exception) {
            throw new IllegalStateException("Ed25519 verification is unavailable", exception);
        }
    }

    public static PayloadCommitProofV1 signed(final byte[] reservationId, final byte[] tenantRoutingScope,
                                              final byte[] routeIncarnationUuid, final int partition,
                                              final DelayMessageId delayMessageId,
                                              final ProfileRefV1 objectStoreProfile, final long trustSetVersion,
                                              final int proofKeyVersion, final byte[] container,
                                              final byte[] objectKey, final byte[] immutableObjectVersion,
                                              final byte[] etag, final long length, final byte[] payloadSha256,
                                              final long notAfterEpochMs, final PrivateKey privateKey) {
        Objects.requireNonNull(privateKey, "privateKey");
        final PayloadCommitProofV1 unsigned = new PayloadCommitProofV1(reservationId, tenantRoutingScope,
                routeIncarnationUuid, partition, delayMessageId, objectStoreProfile, trustSetVersion,
                proofKeyVersion, container, objectKey, immutableObjectVersion, etag, length, payloadSha256,
                notAfterEpochMs, computeProofId(reservationId, tenantRoutingScope, routeIncarnationUuid, partition,
                        delayMessageId, objectStoreProfile, trustSetVersion, container, objectKey,
                        immutableObjectVersion, etag, length, payloadSha256, notAfterEpochMs), new byte[SIGNATURE_LENGTH]);
        final byte[] signature;
        try {
            final Signature signer = Signature.getInstance("Ed25519");
            signer.initSign(privateKey);
            signer.update(unsigned.signatureDigest());
            signature = signer.sign();
        } catch (GeneralSecurityException exception) {
            throw new IllegalStateException("Ed25519 signing is unavailable", exception);
        }
        return new PayloadCommitProofV1(reservationId, tenantRoutingScope, routeIncarnationUuid, partition,
                delayMessageId, objectStoreProfile, trustSetVersion, proofKeyVersion, container, objectKey,
                immutableObjectVersion, etag, length, payloadSha256, notAfterEpochMs, unsigned.proofId, signature);
    }

    public static PayloadCommitProofV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded,
                "PayloadCommitProofV1");
        if (fields.size() != 17 && fields.size() != 18) {
            throw new IllegalArgumentException("PayloadCommitProofV1 has an invalid field count");
        }
        int index = 0;
        if (QueryCodecSupport.uint32(fields.get(index++), 1) != PROOF_VERSION) {
            throw new IllegalArgumentException("unsupported PayloadCommitProofV1 version");
        }
        final byte[] reservationId = QueryCodecSupport.fixed(fields.get(index++), 2, HASH_LENGTH);
        final byte[] tenantRoutingScope = QueryCodecSupport.fixed(fields.get(index++), 3, HASH_LENGTH);
        final byte[] route = QueryCodecSupport.fixed(fields.get(index++), 4, ROUTE_LENGTH);
        final int partition = QueryCodecSupport.uint32(fields.get(index++), 5);
        final DelayMessageId messageId = new DelayMessageId(QueryCodecSupport.fixed(fields.get(index++), 6,
                DelayMessageId.LENGTH));
        final ProfileRefV1 profile = ProfileRefV1.decode(QueryCodecSupport.nested(fields.get(index++), 7));
        final long trustSetVersion = QueryCodecSupport.uint(fields.get(index++), 8);
        final int proofKeyVersion = QueryCodecSupport.uint32(fields.get(index++), 9);
        final byte[] container = QueryCodecSupport.bytes(fields.get(index++), 10);
        final byte[] objectKey = QueryCodecSupport.bytes(fields.get(index++), 11);
        final byte[] objectVersion = QueryCodecSupport.bytes(fields.get(index++), 12);
        byte[] etag = null;
        if (fields.get(index).number() == 13) {
            etag = QueryCodecSupport.bytes(fields.get(index++), 13);
        }
        final long length = QueryCodecSupport.uint(fields.get(index++), 14);
        final byte[] payloadSha = QueryCodecSupport.fixed(fields.get(index++), 15, HASH_LENGTH);
        final long notAfter = QueryCodecSupport.uint(fields.get(index++), 16);
        final byte[] proofId = QueryCodecSupport.fixed(fields.get(index++), 17, HASH_LENGTH);
        final byte[] signature = QueryCodecSupport.fixed(fields.get(index), 18, SIGNATURE_LENGTH);
        final PayloadCommitProofV1 result = new PayloadCommitProofV1(reservationId, tenantRoutingScope, route,
                partition, messageId, profile, trustSetVersion, proofKeyVersion, container, objectKey,
                objectVersion, etag, length, payloadSha, notAfter, proofId, signature);
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "PayloadCommitProofV1");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof PayloadCommitProofV1 that)) {
            return false;
        }
        return trustSetVersion == that.trustSetVersion && proofKeyVersion == that.proofKeyVersion
                && partition == that.partition && length == that.length && notAfterEpochMs == that.notAfterEpochMs
                && delayMessageId.equals(that.delayMessageId) && objectStoreProfile.equals(that.objectStoreProfile)
                && Arrays.equals(reservationId, that.reservationId)
                && Arrays.equals(tenantRoutingScope, that.tenantRoutingScope)
                && Arrays.equals(routeIncarnationUuid, that.routeIncarnationUuid)
                && Arrays.equals(container, that.container) && Arrays.equals(objectKey, that.objectKey)
                && Arrays.equals(immutableObjectVersion, that.immutableObjectVersion)
                && Arrays.equals(etag, that.etag) && Arrays.equals(payloadSha256, that.payloadSha256)
                && Arrays.equals(proofId, that.proofId) && Arrays.equals(signature, that.signature);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(reservationId), Arrays.hashCode(tenantRoutingScope),
                Arrays.hashCode(routeIncarnationUuid), partition, delayMessageId, objectStoreProfile,
                trustSetVersion, proofKeyVersion, Arrays.hashCode(container), Arrays.hashCode(objectKey),
                Arrays.hashCode(immutableObjectVersion), Arrays.hashCode(etag), length,
                Arrays.hashCode(payloadSha256), notAfterEpochMs, Arrays.hashCode(proofId),
                Arrays.hashCode(signature));
    }

    private byte[] computeProofId() {
        return computeProofId(reservationId, tenantRoutingScope, routeIncarnationUuid, partition, delayMessageId,
                objectStoreProfile, trustSetVersion, container, objectKey, immutableObjectVersion, etag, length,
                payloadSha256, notAfterEpochMs);
    }

    private static byte[] computeProofId(final byte[] reservationId, final byte[] tenantRoutingScope,
                                         final byte[] routeIncarnationUuid, final int partition,
                                         final DelayMessageId delayMessageId, final ProfileRefV1 objectStoreProfile,
                                         final long trustSetVersion, final byte[] container, final byte[] objectKey,
                                         final byte[] immutableObjectVersion, final byte[] etag, final long length,
                                         final byte[] payloadSha256, final long notAfterEpochMs) {
        return Bytes.sha256(Bytes.utf8("nereus-delay-payload-proof-id-v1\0"), proofIdFields(reservationId,
                tenantRoutingScope, routeIncarnationUuid, partition, delayMessageId, objectStoreProfile,
                trustSetVersion, container, objectKey, immutableObjectVersion, etag, length, payloadSha256,
                notAfterEpochMs));
    }

    private static byte[] proofIdFields(final byte[] reservationId, final byte[] tenantRoutingScope,
                                        final byte[] routeIncarnationUuid, final int partition,
                                        final DelayMessageId delayMessageId, final ProfileRefV1 objectStoreProfile,
                                        final long trustSetVersion, final byte[] container, final byte[] objectKey,
                                        final byte[] immutableObjectVersion, final byte[] etag, final long length,
                                        final byte[] payloadSha256, final long notAfterEpochMs) {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, PROOF_VERSION);
            CanonicalProtobuf.bytes(output, 2, reservationId);
            CanonicalProtobuf.bytes(output, 3, tenantRoutingScope);
            CanonicalProtobuf.bytes(output, 4, routeIncarnationUuid);
            CanonicalProtobuf.uint32(output, 5, partition);
            CanonicalProtobuf.bytes(output, 6, delayMessageId.bytes());
            CanonicalProtobuf.bytes(output, 7, objectStoreProfile.canonicalBytes());
            CanonicalProtobuf.uint64(output, 8, trustSetVersion);
            CanonicalProtobuf.bytes(output, 10, container);
            CanonicalProtobuf.bytes(output, 11, objectKey);
            CanonicalProtobuf.bytes(output, 12, immutableObjectVersion);
            if (etag != null) {
                CanonicalProtobuf.bytes(output, 13, etag);
            }
            CanonicalProtobuf.uint64(output, 14, length);
            CanonicalProtobuf.bytes(output, 15, payloadSha256);
            CanonicalProtobuf.int64(output, 16, notAfterEpochMs);
        });
    }

    private byte[] signatureDigest() {
        return Bytes.sha256(Bytes.utf8("nereus-delay-payload-proof-signature-v1\0"), signatureFields());
    }

    private byte[] signatureFields() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, PROOF_VERSION);
            CanonicalProtobuf.bytes(output, 2, reservationId);
            CanonicalProtobuf.bytes(output, 3, tenantRoutingScope);
            CanonicalProtobuf.bytes(output, 4, routeIncarnationUuid);
            CanonicalProtobuf.uint32(output, 5, partition);
            CanonicalProtobuf.bytes(output, 6, delayMessageId.bytes());
            CanonicalProtobuf.bytes(output, 7, objectStoreProfile.canonicalBytes());
            CanonicalProtobuf.uint64(output, 8, trustSetVersion);
            CanonicalProtobuf.uint32(output, 9, proofKeyVersion);
            CanonicalProtobuf.bytes(output, 10, container);
            CanonicalProtobuf.bytes(output, 11, objectKey);
            CanonicalProtobuf.bytes(output, 12, immutableObjectVersion);
            if (etag != null) {
                CanonicalProtobuf.bytes(output, 13, etag);
            }
            CanonicalProtobuf.uint64(output, 14, length);
            CanonicalProtobuf.bytes(output, 15, payloadSha256);
            CanonicalProtobuf.int64(output, 16, notAfterEpochMs);
            CanonicalProtobuf.bytes(output, 17, proofId);
        });
    }

    private static byte[] nonEmpty(final byte[] value, final String name) {
        Objects.requireNonNull(value, name);
        if (value.length == 0) {
            throw new IllegalArgumentException(name + " must not be empty");
        }
        return Bytes.copy(value);
    }

    private static byte[] fixedNonZero(final byte[] value, final String name) {
        Bytes.requireLength(value, HASH_LENGTH, name);
        if (Bytes.constantTimeEquals(value, new byte[HASH_LENGTH])) {
            throw new IllegalArgumentException(name + " must not be all zero");
        }
        return Bytes.copy(value);
    }
}
