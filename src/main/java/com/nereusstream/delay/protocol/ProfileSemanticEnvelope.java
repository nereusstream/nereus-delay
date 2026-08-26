package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Closed immutable Profile semantic envelope and domain-separated semantic hash.
 *
 * <p>Runtime health, endpoint discovery and mutable credential state are
 * deliberately outside this value. Publication/catalog authority remains an
 * external control-plane concern.</p>
 */
public final class ProfileSemanticEnvelope {
    public static final int ENVELOPE_VERSION = 1;
    public static final int BODY_SCHEMA_VERSION = 1;
    public static final int HASH_LENGTH = 32;
    public static final int MAX_PROFILE_ID_BYTES = 256;
    private static final String HASH_DOMAIN = "nereus-delay-profile-semantic\0";

    private final ProfileKind profileKind;
    private final byte[] profileId;
    private final long version;
    private final ProfileSemanticBody body;
    private final byte[] semanticHash;

    public ProfileSemanticEnvelope(
            final ProfileKind profileKind, final byte[] profileId, final long version, final ProfileSemanticBody body) {
        this.profileKind = Objects.requireNonNull(profileKind, "profileKind");
        this.profileId = boundedId(profileId);
        if (version == 0) {
            throw new IllegalArgumentException("Profile version must be nonzero");
        }
        this.version = version;
        this.body = Objects.requireNonNull(body, "body");
        if (body.profileKind() != profileKind || body.schemaVersion() != BODY_SCHEMA_VERSION) {
            throw new IllegalArgumentException("Profile kind/body schema branch mismatch");
        }
        this.semanticHash = computeSemanticHash();
    }

    private ProfileSemanticEnvelope(
            final ProfileKind profileKind,
            final byte[] profileId,
            final long version,
            final ProfileSemanticBody body,
            final byte[] semanticHash) {
        this(profileKind, profileId, version, body);
        Bytes.requireLength(semanticHash, HASH_LENGTH, "semanticHash");
        if (!Bytes.constantTimeEquals(this.semanticHash, semanticHash)) {
            throw new IllegalArgumentException("Profile semantic hash mismatch");
        }
    }

    public ProfileKind profileKind() {
        return profileKind;
    }

    public byte[] profileId() {
        return Bytes.copy(profileId);
    }

    public long version() {
        return version;
    }

    public ProfileSemanticBody body() {
        return body;
    }

    public byte[] semanticHash() {
        return Bytes.copy(semanticHash);
    }

    public ProfileRef ref() {
        return new ProfileRef(profileId, version, semanticHash, profileKind);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, ENVELOPE_VERSION);
            CanonicalProtobuf.uint32(output, 2, profileKind.wireValue());
            CanonicalProtobuf.bytes(output, 3, profileId);
            CanonicalProtobuf.uint64Bits(output, 4, version);
            CanonicalProtobuf.uint32(output, 5, BODY_SCHEMA_VERSION);
            CanonicalProtobuf.bytes(output, branchNumber(profileKind), body.canonicalBytes());
            CanonicalProtobuf.bytes(output, 20, semanticHash);
        });
    }

    public static ProfileSemanticEnvelope decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "ProfileSemanticEnvelope");
        if (fields.size() != 7) {
            throw new IllegalArgumentException("ProfileSemanticEnvelope has an unexpected field count");
        }
        for (int index = 0; index < 5; index++) {
            if (fields.get(index).number() != index + 1) {
                throw new IllegalArgumentException("ProfileSemanticEnvelope field order mismatch");
            }
        }
        if (fields.get(6).number() != 20) {
            throw new IllegalArgumentException("ProfileSemanticEnvelope semantic hash field is missing");
        }
        if (QueryCodecSupport.uint(fields.get(0), 1) != ENVELOPE_VERSION
                || QueryCodecSupport.uint(fields.get(4), 5) != BODY_SCHEMA_VERSION) {
            throw new IllegalArgumentException("unsupported ProfileSemanticEnvelope version");
        }
        final ProfileKind kind = ProfileKind.fromWire(QueryCodecSupport.uint(fields.get(1), 2));
        final int branch = branchNumber(kind);
        if (fields.get(5).number() != branch) {
            throw new IllegalArgumentException("Profile semantic branch does not match kind");
        }
        final ProfileSemanticBody body = decodeBody(kind, QueryCodecSupport.nested(fields.get(5), branch));
        final ProfileSemanticEnvelope result = new ProfileSemanticEnvelope(
                kind,
                QueryCodecSupport.bytes(fields.get(2), 3),
                QueryCodecSupport.uint64Bits(fields.get(3), 4),
                body,
                QueryCodecSupport.fixed(fields.get(6), 20, HASH_LENGTH));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "ProfileSemanticEnvelope");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof ProfileSemanticEnvelope that
                && profileKind == that.profileKind
                && version == that.version
                && Arrays.equals(profileId, that.profileId)
                && body.equals(that.body)
                && Arrays.equals(semanticHash, that.semanticHash);
    }

    @Override
    public int hashCode() {
        return Objects.hash(profileKind, Arrays.hashCode(profileId), version, body, Arrays.hashCode(semanticHash));
    }

    private byte[] computeSemanticHash() {
        return Bytes.sha256(
                Bytes.utf8(HASH_DOMAIN),
                Bytes.u16be(profileKind.wireValue()),
                Bytes.lp32(profileId),
                Bytes.u64beBits(version),
                Bytes.u32be(BODY_SCHEMA_VERSION),
                Bytes.lp32(body.canonicalBytes()));
    }

    private static ProfileSemanticBody decodeBody(final ProfileKind kind, final byte[] encoded) {
        return switch (kind) {
            case DESTINATION -> DestinationProfileSemantic.decode(encoded);
            case DELIVERY_CAPABILITY -> DeliveryCapabilitySemantic.decode(encoded);
            case OBJECT_STORE -> ObjectStoreProfileSemantic.decode(encoded);
            case EVIDENCE_VERIFIER -> EvidenceVerifierProfileSemantic.decode(encoded);
        };
    }

    private static int branchNumber(final ProfileKind kind) {
        return switch (kind) {
            case DESTINATION -> 10;
            case DELIVERY_CAPABILITY -> 11;
            case OBJECT_STORE -> 12;
            case EVIDENCE_VERIFIER -> 13;
        };
    }

    private static byte[] boundedId(final byte[] value) {
        Objects.requireNonNull(value, "profileId");
        if (value.length == 0 || value.length > MAX_PROFILE_ID_BYTES) {
            throw new IllegalArgumentException("profileId is outside the bound");
        }
        return Bytes.copy(value);
    }
}
