package io.nereusstream.delay.protocol;

import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.util.Arrays;
import java.util.Objects;

/**
 * Non-secret proof that an immutable object-store payload was uploaded.
 * Object-store access and proof issuance live outside the shard state machine;
 * the shard only validates this closed proof projection.
 */
public record PayloadCommitProof(
        long trustSetVersion,
        int proofKeyVersion,
        byte[] routeIncarnationUuid,
        int partition,
        DelayMessageId delayMessageId,
        byte[] reservationId,
        byte[] objectStoreProfileHash,
        byte[] container,
        byte[] objectKey,
        byte[] immutableObjectVersion,
        byte[] etag,
        long length,
        byte[] payloadSha256,
        long notAfterEpochMs,
        byte[] proofId,
        byte[] signature) implements PayloadCommitProofView {
    private static final int PROOF_ID_LENGTH = 32;
    private static final int SIGNATURE_LENGTH = 64;

    public PayloadCommitProof {
        Bytes.requireLength(routeIncarnationUuid, 16, "routeIncarnationUuid");
        if (partition < 0 || trustSetVersion <= 0 || proofKeyVersion <= 0 || length < 0
                || notAfterEpochMs < 0) {
            throw new IllegalArgumentException("invalid payload commit proof");
        }
        Objects.requireNonNull(delayMessageId, "delayMessageId");
        if (!Arrays.equals(routeIncarnationUuid, delayMessageId.routingId().shardId().routeIncarnation().bytes())
                || partition != delayMessageId.routingId().shardId().partition()) {
            throw new IllegalArgumentException("payload proof shard identity mismatch");
        }
        Bytes.requireLength(reservationId, 32, "reservationId");
        Bytes.requireLength(objectStoreProfileHash, 32, "objectStoreProfileHash");
        requireNonEmpty(container, "container");
        requireNonEmpty(objectKey, "objectKey");
        requireNonEmpty(immutableObjectVersion, "immutableObjectVersion");
        Objects.requireNonNull(etag, "etag");
        Bytes.requireLength(payloadSha256, 32, "payloadSha256");
        Bytes.requireLength(proofId, PROOF_ID_LENGTH, "proofId");
        Bytes.requireLength(signature, SIGNATURE_LENGTH, "signature");
        routeIncarnationUuid = Bytes.copy(routeIncarnationUuid);
        reservationId = Bytes.copy(reservationId);
        objectStoreProfileHash = Bytes.copy(objectStoreProfileHash);
        container = Bytes.copy(container);
        objectKey = Bytes.copy(objectKey);
        immutableObjectVersion = Bytes.copy(immutableObjectVersion);
        etag = Bytes.copy(etag);
        payloadSha256 = Bytes.copy(payloadSha256);
        proofId = Bytes.copy(proofId);
        signature = Bytes.copy(signature);
        if (!Bytes.constantTimeEquals(proofId, computeProofId(trustSetVersion, routeIncarnationUuid, partition,
                delayMessageId, reservationId, objectStoreProfileHash, container, objectKey,
                immutableObjectVersion, etag, length, payloadSha256, notAfterEpochMs))) {
            throw new IllegalArgumentException("payload proof id mismatch");
        }
    }

    private static void requireNonEmpty(final byte[] value, final String name) {
        Objects.requireNonNull(value, name);
        if (value.length == 0) {
            throw new IllegalArgumentException(name + " must not be empty");
        }
    }

    public byte[] routeIncarnationUuid() {
        return Bytes.copy(routeIncarnationUuid);
    }

    public byte[] reservationId() {
        return Bytes.copy(reservationId);
    }

    public byte[] objectStoreProfileHash() {
        return Bytes.copy(objectStoreProfileHash);
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
        return Bytes.copy(etag);
    }

    public byte[] payloadSha256() {
        return Bytes.copy(payloadSha256);
    }

    public byte[] proofId() {
        return Bytes.copy(proofId);
    }

    public byte[] signature() {
        return Bytes.copy(signature);
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof PayloadCommitProof that)) {
            return false;
        }
        return trustSetVersion == that.trustSetVersion && proofKeyVersion == that.proofKeyVersion
                && partition == that.partition && length == that.length && notAfterEpochMs == that.notAfterEpochMs
                && delayMessageId.equals(that.delayMessageId)
                && Arrays.equals(routeIncarnationUuid, that.routeIncarnationUuid)
                && Arrays.equals(reservationId, that.reservationId)
                && Arrays.equals(objectStoreProfileHash, that.objectStoreProfileHash)
                && Arrays.equals(container, that.container) && Arrays.equals(objectKey, that.objectKey)
                && Arrays.equals(immutableObjectVersion, that.immutableObjectVersion)
                && Arrays.equals(etag, that.etag) && Arrays.equals(payloadSha256, that.payloadSha256)
                && Arrays.equals(proofId, that.proofId) && Arrays.equals(signature, that.signature);
    }

    @Override
    public int hashCode() {
        return Objects.hash(trustSetVersion, proofKeyVersion, Arrays.hashCode(routeIncarnationUuid), partition,
                delayMessageId, Arrays.hashCode(reservationId), Arrays.hashCode(objectStoreProfileHash),
                Arrays.hashCode(container), Arrays.hashCode(objectKey), Arrays.hashCode(immutableObjectVersion),
                Arrays.hashCode(etag), length, Arrays.hashCode(payloadSha256), notAfterEpochMs,
                Arrays.hashCode(proofId), Arrays.hashCode(signature));
    }

    /** Encodes all fields in a strict versioned binary projection. */
    public byte[] canonicalBytes() {
        return Bytes.concat(Bytes.u32be(1), Bytes.u64be(trustSetVersion), Bytes.u32be(proofKeyVersion),
                routeIncarnationUuid, Bytes.u32be(partition), delayMessageId.bytes(), reservationId,
                objectStoreProfileHash, Bytes.lp32(container), Bytes.lp32(objectKey),
                Bytes.lp32(immutableObjectVersion), Bytes.lp32(etag), Bytes.u64be(length), payloadSha256,
                Bytes.i64be(notAfterEpochMs), proofId, signature);
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

    public static PayloadCommitProof signed(final long trustSetVersion, final int proofKeyVersion,
                                            final byte[] routeIncarnationUuid, final int partition,
                                            final DelayMessageId delayMessageId, final byte[] reservationId,
                                            final byte[] objectStoreProfileHash, final byte[] container,
                                            final byte[] objectKey, final byte[] immutableObjectVersion,
                                            final byte[] etag, final long length, final byte[] payloadSha256,
                                            final long notAfterEpochMs, final PrivateKey privateKey) {
        final byte[] proofId = computeProofId(trustSetVersion, routeIncarnationUuid, partition, delayMessageId,
                reservationId, objectStoreProfileHash, container, objectKey, immutableObjectVersion, etag, length,
                payloadSha256, notAfterEpochMs);
        final byte[] signature;
        try {
            final Signature signer = Signature.getInstance("Ed25519");
            signer.initSign(privateKey);
            signer.update(signatureDigest(trustSetVersion, proofKeyVersion, routeIncarnationUuid, partition,
                    delayMessageId, reservationId, objectStoreProfileHash, container, objectKey,
                    immutableObjectVersion, etag, length, payloadSha256, notAfterEpochMs, proofId));
            signature = signer.sign();
        } catch (GeneralSecurityException exception) {
            throw new IllegalStateException("Ed25519 signing is unavailable", exception);
        }
        return new PayloadCommitProof(trustSetVersion, proofKeyVersion, routeIncarnationUuid, partition,
                delayMessageId, reservationId, objectStoreProfileHash, container, objectKey,
                immutableObjectVersion, etag, length, payloadSha256, notAfterEpochMs, proofId, signature);
    }

    public static PayloadCommitProof decode(final byte[] encoded) {
        final ByteBuffer input = ByteBuffer.wrap(encoded);
        if (input.remaining() < 4 + 8 + 4 + 16 + 4 + DelayMessageId.LENGTH + 32 + 32 + 4 * 3 + 8 + 32 + 8
                + PROOF_ID_LENGTH + SIGNATURE_LENGTH) {
            throw new IllegalArgumentException("payload commit proof is truncated");
        }
        if (input.getInt() != 1) {
            throw new IllegalArgumentException("unsupported payload commit proof version");
        }
        final long trustSetVersion = input.getLong();
        final int proofKeyVersion = input.getInt();
        final byte[] route = readFixed(input, 16);
        final int partition = input.getInt();
        final DelayMessageId messageId = new DelayMessageId(readFixed(input, DelayMessageId.LENGTH));
        final byte[] reservation = readFixed(input, 32);
        final byte[] profile = readFixed(input, 32);
        final byte[] container = readLp32(input);
        final byte[] objectKey = readLp32(input);
        final byte[] objectVersion = readLp32(input);
        final byte[] etag = readLp32(input);
        final long length = input.getLong();
        final byte[] sha = readFixed(input, 32);
        final long notAfter = input.getLong();
        final byte[] proofId = readFixed(input, PROOF_ID_LENGTH);
        final byte[] signature = readFixed(input, SIGNATURE_LENGTH);
        if (input.hasRemaining()) {
            throw new IllegalArgumentException("payload commit proof has trailing bytes");
        }
        final PayloadCommitProof decoded = new PayloadCommitProof(trustSetVersion, proofKeyVersion, route,
                partition, messageId, reservation, profile, container, objectKey, objectVersion, etag, length, sha,
                notAfter, proofId, signature);
        if (!Arrays.equals(encoded, decoded.canonicalBytes())) {
            throw new IllegalArgumentException("non-canonical payload commit proof");
        }
        return decoded;
    }

    private byte[] signatureDigest() {
        return signatureDigest(trustSetVersion, proofKeyVersion, routeIncarnationUuid, partition, delayMessageId,
                reservationId, objectStoreProfileHash, container, objectKey, immutableObjectVersion, etag, length,
                payloadSha256, notAfterEpochMs, proofId);
    }

    private static byte[] computeProofId(final long trustSetVersion, final byte[] route, final int partition,
                                         final DelayMessageId messageId, final byte[] reservation,
                                         final byte[] profile, final byte[] container, final byte[] objectKey,
                                         final byte[] objectVersion, final byte[] etag, final long length,
                                         final byte[] sha, final long notAfter) {
        return Bytes.sha256(Bytes.utf8("nereus-delay-payload-proof-id-v1\0"), Bytes.u32be(1),
                Bytes.u64be(trustSetVersion), route, Bytes.u32be(partition), messageId.bytes(), reservation,
                profile, Bytes.lp32(container), Bytes.lp32(objectKey), Bytes.lp32(objectVersion), Bytes.lp32(etag),
                Bytes.u64be(length), sha, Bytes.i64be(notAfter));
    }

    private static byte[] signatureDigest(final long trustSetVersion, final int proofKeyVersion,
                                           final byte[] route, final int partition, final DelayMessageId messageId,
                                           final byte[] reservation, final byte[] profile, final byte[] container,
                                           final byte[] objectKey, final byte[] objectVersion, final byte[] etag,
                                           final long length, final byte[] sha, final long notAfter,
                                           final byte[] proofId) {
        return Bytes.sha256(Bytes.utf8("nereus-delay-payload-proof-signature-v1\0"), Bytes.u32be(1),
                Bytes.u64be(trustSetVersion), Bytes.u32be(proofKeyVersion), route, Bytes.u32be(partition),
                messageId.bytes(), reservation, profile, Bytes.lp32(container), Bytes.lp32(objectKey),
                Bytes.lp32(objectVersion), Bytes.lp32(etag), Bytes.u64be(length), sha, Bytes.i64be(notAfter),
                proofId);
    }

    private static byte[] readFixed(final ByteBuffer input, final int length) {
        if (input.remaining() < length) {
            throw new IllegalArgumentException("truncated payload commit proof");
        }
        final byte[] result = new byte[length];
        input.get(result);
        return result;
    }

    private static byte[] readLp32(final ByteBuffer input) {
        if (input.remaining() < 4) {
            throw new IllegalArgumentException("truncated payload commit proof length");
        }
        final long length = Integer.toUnsignedLong(input.getInt());
        if (length > input.remaining()) {
            throw new IllegalArgumentException("payload commit proof length outside body");
        }
        return readFixed(input, Math.toIntExact(length));
    }
}
