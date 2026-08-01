package io.nereusstream.delay.protocol;

import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Canonical, signed Shard Log System Mutation envelope.
 *
 * <p>The operation body is intentionally kept opaque here. Operation-specific body codecs can be added without
 * changing the outer envelope, while this class already enforces the registry's identity, subject, hash and
 * signature boundaries.</p>
 */
public final class SystemMutation {
    public static final int LOG_ENVELOPE_VERSION = 1;
    public static final int ENVELOPE_VERSION = 1;
    public static final int BODY_VERSION = 1;
    public static final int FRAMING_VERSION = 1;
    public static final int HASH_LENGTH = 32;
    public static final int SIGNATURE_LENGTH = 64;

    private final ShardId shardId;
    private final SystemMutationType type;
    private final long retryUntilEpochMs;
    private final byte[] logicalOperationIdentity;
    private final byte[] canonicalBody;
    private final byte[] authorIdentity;
    private final int signingKeyVersion;
    private final byte[] systemMutationId;
    private final byte[] mutationHash;
    private final byte[] signature;

    private SystemMutation(final ShardId shardId, final SystemMutationType type, final long retryUntilEpochMs,
                           final byte[] logicalOperationIdentity, final byte[] canonicalBody,
                           final byte[] authorIdentity, final int signingKeyVersion, final byte[] systemMutationId,
                           final byte[] mutationHash, final byte[] signature) {
        this.shardId = Objects.requireNonNull(shardId, "shardId");
        this.type = Objects.requireNonNull(type, "type");
        if (retryUntilEpochMs < 0) {
            throw new IllegalArgumentException("retryUntil must be non-negative");
        }
        this.retryUntilEpochMs = retryUntilEpochMs;
        this.logicalOperationIdentity = fixed(logicalOperationIdentity, HASH_LENGTH, "logicalOperationIdentity");
        this.canonicalBody = Bytes.copy(canonicalBody);
        final AuthorIdentity decodedAuthor = AuthorIdentity.decode(authorIdentity);
        decodedAuthor.requireFor(type);
        this.authorIdentity = decodedAuthor.canonicalBytes();
        if (signingKeyVersion <= 0) {
            throw new IllegalArgumentException("signingKeyVersion must be positive");
        }
        this.signingKeyVersion = signingKeyVersion;
        this.systemMutationId = fixed(systemMutationId, HASH_LENGTH, "systemMutationId");
        this.mutationHash = fixed(mutationHash, HASH_LENGTH, "mutationHash");
        this.signature = fixed(signature, SIGNATURE_LENGTH, "signature");

        if (!Bytes.constantTimeEquals(this.mutationHash, computeMutationHash(shardId, type, retryUntilEpochMs,
                this.canonicalBody))) {
            throw new IllegalArgumentException("mutationHash does not match the canonical mutation");
        }
        if (!Bytes.constantTimeEquals(this.systemMutationId, computeSystemMutationId(shardId, type,
                this.logicalOperationIdentity, this.mutationHash))) {
            throw new IllegalArgumentException("systemMutationId does not match the logical operation");
        }
    }

    /** Creates and signs a mutation using the exact registry preimages. */
    public static SystemMutation signed(final ShardId shardId, final SystemMutationType type,
                                        final long retryUntilEpochMs, final byte[] logicalOperationIdentity,
                                        final byte[] canonicalBody, final byte[] authorIdentity,
                                        final int signingKeyVersion, final PrivateKey privateKey) {
        Objects.requireNonNull(privateKey, "privateKey");
        final byte[] logical = fixed(logicalOperationIdentity, HASH_LENGTH, "logicalOperationIdentity");
        final byte[] body = Bytes.copy(canonicalBody);
        final byte[] author = AuthorIdentity.decode(authorIdentity).canonicalBytes();
        AuthorIdentity.decode(author).requireFor(type);
        final byte[] hash = computeMutationHash(shardId, type, retryUntilEpochMs, body);
        final byte[] id = computeSystemMutationId(shardId, type, logical, hash);
        final byte[] signature = sign(signatureDigest(shardId, type, retryUntilEpochMs, id, body, hash, author,
                signingKeyVersion), privateKey);
        return new SystemMutation(shardId, type, retryUntilEpochMs, logical, body, author, signingKeyVersion, id,
                hash, signature);
    }

    public ShardId shardId() {
        return shardId;
    }

    public SystemMutationType type() {
        return type;
    }

    public long retryUntilEpochMs() {
        return retryUntilEpochMs;
    }

    public byte[] logicalOperationIdentity() {
        return Bytes.copy(logicalOperationIdentity);
    }

    public byte[] canonicalBody() {
        return Bytes.copy(canonicalBody);
    }

    public byte[] authorIdentity() {
        return Bytes.copy(authorIdentity);
    }

    public int signingKeyVersion() {
        return signingKeyVersion;
    }

    public byte[] systemMutationId() {
        return Bytes.copy(systemMutationId);
    }

    public byte[] mutationHash() {
        return Bytes.copy(mutationHash);
    }

    public byte[] signature() {
        return Bytes.copy(signature);
    }

    /** Encodes the canonical ShardLogEnvelopeV1, including the system-mutation oneof branch. */
    public byte[] canonicalEnvelope() {
        final byte[] subject = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, shardId.routeIncarnation().bytes());
            CanonicalProtobuf.uint32(output, 2, shardId.partition());
        });
        final byte[] mutation = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, ENVELOPE_VERSION);
            CanonicalProtobuf.bytes(output, 2, systemMutationId);
            CanonicalProtobuf.bytes(output, 3, subject);
            CanonicalProtobuf.uint32(output, 4, type.wireValue());
            CanonicalProtobuf.int64(output, 5, retryUntilEpochMs);
            CanonicalProtobuf.bytes(output, 6, canonicalBody);
            CanonicalProtobuf.bytes(output, 7, mutationHash);
            CanonicalProtobuf.uint32(output, 8, BODY_VERSION);
            CanonicalProtobuf.bytes(output, 9, authorIdentity);
            CanonicalProtobuf.uint32(output, 10, signingKeyVersion);
            CanonicalProtobuf.bytes(output, 11, signature);
        });
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, LOG_ENVELOPE_VERSION);
            CanonicalProtobuf.bytes(output, 3, mutation);
        });
    }

    public byte[] encodeFrame() {
        return ShardLogFrame.encode(ShardLogFrame.SYSTEM_MUTATION_KIND, canonicalEnvelope());
    }

    /**
     * Decodes a frame and validates the logical operation identity supplied by the operation-specific caller.
     * The identity is deliberately not duplicated in the wire envelope; the registry derives it from the body
     * operation's stable identity.
     */
    public static SystemMutation decodeFrame(final byte[] frame, final byte[] logicalOperationIdentity) {
        final ShardLogFrame.Decoded decoded = ShardLogFrame.decode(frame);
        if (decoded.recordKind() != ShardLogFrame.SYSTEM_MUTATION_KIND) {
            throw new IllegalArgumentException("frame is not a System Mutation");
        }
        return decodeEnvelope(decoded.canonicalEnvelope(), logicalOperationIdentity);
    }

    public static SystemMutation decodeEnvelope(final byte[] envelope, final byte[] logicalOperationIdentity) {
        final List<CanonicalProtobuf.Reader.Field> outer = readAll(new CanonicalProtobuf.Reader(envelope));
        if (outer.size() != 2) {
            throw new IllegalArgumentException("Shard Log envelope fields are incomplete or unknown");
        }
        requireVarint(outer.get(0), 1, LOG_ENVELOPE_VERSION);
        requireWire(outer.get(1), 3, 2);

        final List<CanonicalProtobuf.Reader.Field> fields = readAll(
                new CanonicalProtobuf.Reader(outer.get(1).rawValue()));
        if (fields.size() != 11) {
            throw new IllegalArgumentException("System Mutation envelope fields are incomplete or unknown");
        }
        requireVarint(fields.get(0), 1, ENVELOPE_VERSION);
        final byte[] id = requireFixed(fields.get(1), 2, HASH_LENGTH);
        final List<CanonicalProtobuf.Reader.Field> subjectFields = readAll(
                new CanonicalProtobuf.Reader(requireBytes(fields.get(2), 3)));
        if (subjectFields.size() != 2) {
            throw new IllegalArgumentException("System Mutation shard subject is incomplete or unknown");
        }
        final RouteIncarnation route = new RouteIncarnation(requireFixed(subjectFields.get(0), 1,
                RouteIncarnation.LENGTH));
        final long partition = requireVarint(subjectFields.get(1), 2);
        if (partition > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("shard partition exceeds Java int range");
        }
        final ShardId shard = new ShardId(route, (int) partition);
        final SystemMutationType type = SystemMutationType.fromWire(requireVarint(fields.get(3), 4));
        final long retryUntil = requireVarint(fields.get(4), 5);
        final byte[] body = requireBytes(fields.get(5), 6);
        final byte[] hash = requireFixed(fields.get(6), 7, HASH_LENGTH);
        requireVarint(fields.get(7), 8, BODY_VERSION);
        final byte[] author = requireBytes(fields.get(8), 9);
        final long keyVersion = requireVarint(fields.get(9), 10);
        if (keyVersion > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("signingKeyVersion exceeds Java int range");
        }
        final byte[] signature = requireFixed(fields.get(10), 11, SIGNATURE_LENGTH);
        final SystemMutation decoded = new SystemMutation(shard, type, retryUntil, logicalOperationIdentity, body,
                author, (int) keyVersion, id, hash, signature);
        if (!Arrays.equals(envelope, decoded.canonicalEnvelope())) {
            throw new IllegalArgumentException("non-canonical System Mutation envelope");
        }
        return decoded;
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

    public static byte[] computeMutationHash(final ShardId shardId, final SystemMutationType type,
                                              final long retryUntilEpochMs, final byte[] canonicalBody) {
        if (retryUntilEpochMs < 0) {
            throw new IllegalArgumentException("retryUntil must be non-negative");
        }
        return Bytes.sha256(Bytes.utf8("nereus-delay-system-mutation-hash-v1\0"), Bytes.u8(FRAMING_VERSION),
                Bytes.u32be(LOG_ENVELOPE_VERSION), Bytes.u32be(ENVELOPE_VERSION), Bytes.u32be(BODY_VERSION),
                Bytes.u16be(Objects.requireNonNull(type, "type").wireValue()), shardId.routeIncarnation().bytes(),
                Bytes.u32be(shardId.partition()), Bytes.i64be(retryUntilEpochMs), Bytes.lp32(canonicalBody));
    }

    public static byte[] computeSystemMutationId(final ShardId shardId, final SystemMutationType type,
                                                   final byte[] logicalOperationIdentity, final byte[] mutationHash) {
        return Bytes.sha256(Bytes.utf8("nereus-delay-system-mutation-id-v1"),
                Bytes.u16be(Objects.requireNonNull(type, "type").wireValue()),
                Bytes.lp32(fixed(logicalOperationIdentity, HASH_LENGTH, "logicalOperationIdentity")),
                shardId.routeIncarnation().bytes(), Bytes.u32be(shardId.partition()),
                fixed(mutationHash, HASH_LENGTH, "mutationHash"));
    }

    private byte[] signatureDigest() {
        return signatureDigest(shardId, type, retryUntilEpochMs, systemMutationId, canonicalBody, mutationHash,
                authorIdentity, signingKeyVersion);
    }

    private static byte[] signatureDigest(final ShardId shardId, final SystemMutationType type,
                                          final long retryUntilEpochMs, final byte[] systemMutationId,
                                          final byte[] canonicalBody, final byte[] mutationHash,
                                          final byte[] authorIdentity, final int signingKeyVersion) {
        return Bytes.sha256(Bytes.utf8("nereus-delay-system-mutation-signature-v1\0"),
                Bytes.u32be(ShardLogFrame.MAGIC), Bytes.u8(FRAMING_VERSION),
                Bytes.u8(ShardLogFrame.SYSTEM_MUTATION_KIND), Bytes.u32be(LOG_ENVELOPE_VERSION),
                Bytes.u32be(ENVELOPE_VERSION), Bytes.u32be(BODY_VERSION),
                Bytes.u16be(Objects.requireNonNull(type, "type").wireValue()),
                Bytes.lp32(fixed(systemMutationId, HASH_LENGTH, "systemMutationId")),
                shardId.routeIncarnation().bytes(), Bytes.u32be(shardId.partition()), Bytes.i64be(retryUntilEpochMs),
                Bytes.lp32(canonicalBody), Bytes.lp32(fixed(mutationHash, HASH_LENGTH, "mutationHash")),
                Bytes.lp32(authorIdentity), Bytes.u32be(signingKeyVersion));
    }

    private static byte[] sign(final byte[] digest, final PrivateKey privateKey) {
        try {
            final Signature signer = Signature.getInstance("Ed25519");
            signer.initSign(privateKey);
            signer.update(digest);
            return signer.sign();
        } catch (GeneralSecurityException exception) {
            throw new IllegalStateException("Ed25519 signing is unavailable", exception);
        }
    }

    private static List<CanonicalProtobuf.Reader.Field> readAll(final CanonicalProtobuf.Reader reader) {
        final List<CanonicalProtobuf.Reader.Field> fields = new ArrayList<>();
        while (reader.hasRemaining()) {
            fields.add(reader.next());
        }
        return fields;
    }

    private static void requireWire(final CanonicalProtobuf.Reader.Field field, final int number, final int wire) {
        if (field.number() != number || field.wireType() != wire) {
            throw new IllegalArgumentException("unexpected protobuf field " + field.number());
        }
    }

    private static void requireVarint(final CanonicalProtobuf.Reader.Field field, final int number,
                                      final long expected) {
        requireWire(field, number, 0);
        if (field.unsignedValue() != expected) {
            throw new IllegalArgumentException("unexpected value for protobuf field " + number);
        }
    }

    private static long requireVarint(final CanonicalProtobuf.Reader.Field field, final int number) {
        requireWire(field, number, 0);
        final long value = field.unsignedValue();
        if (value < 0) {
            throw new IllegalArgumentException("protobuf int64 value exceeds signed range");
        }
        return value;
    }

    private static byte[] requireBytes(final CanonicalProtobuf.Reader.Field field, final int number) {
        requireWire(field, number, 2);
        return field.rawValue();
    }

    private static byte[] requireFixed(final CanonicalProtobuf.Reader.Field field, final int number,
                                       final int length) {
        final byte[] value = requireBytes(field, number);
        Bytes.requireLength(value, length, "protobuf field " + number);
        return value;
    }

    private static byte[] fixed(final byte[] value, final int length, final String name) {
        Bytes.requireLength(value, length, name);
        return Bytes.copy(value);
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof SystemMutation mutation)) {
            return false;
        }
        return shardId.equals(mutation.shardId) && type == mutation.type
                && retryUntilEpochMs == mutation.retryUntilEpochMs
                && Arrays.equals(logicalOperationIdentity, mutation.logicalOperationIdentity)
                && Arrays.equals(canonicalBody, mutation.canonicalBody)
                && Arrays.equals(authorIdentity, mutation.authorIdentity)
                && signingKeyVersion == mutation.signingKeyVersion
                && Arrays.equals(systemMutationId, mutation.systemMutationId)
                && Arrays.equals(mutationHash, mutation.mutationHash)
                && Arrays.equals(signature, mutation.signature);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(shardId, type, retryUntilEpochMs, signingKeyVersion);
        result = 31 * result + Arrays.hashCode(logicalOperationIdentity);
        result = 31 * result + Arrays.hashCode(canonicalBody);
        result = 31 * result + Arrays.hashCode(authorIdentity);
        result = 31 * result + Arrays.hashCode(systemMutationId);
        result = 31 * result + Arrays.hashCode(mutationHash);
        result = 31 * result + Arrays.hashCode(signature);
        return result;
    }
}
