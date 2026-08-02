package io.nereusstream.delay.protocol;

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
public final class ProfileSemanticEnvelopeV1 {
    public static final int ENVELOPE_VERSION = 1;
    public static final int BODY_SCHEMA_VERSION = 1;
    public static final int HASH_LENGTH = 32;
    public static final int MAX_PROFILE_ID_BYTES = 256;
    private static final String HASH_DOMAIN = "nereus-delay-profile-semantic-v1\0";

    private final ProfileKindV1 profileKind;
    private final byte[] profileId;
    private final long version;
    private final ProfileSemanticBodyV1 body;
    private final byte[] semanticHash;

    public ProfileSemanticEnvelopeV1(final ProfileKindV1 profileKind, final byte[] profileId, final long version,
                                     final ProfileSemanticBodyV1 body) {
        this.profileKind = Objects.requireNonNull(profileKind, "profileKind");
        this.profileId = boundedId(profileId);
        if (version <= 0) {
            throw new IllegalArgumentException("Profile version must be positive");
        }
        this.version = version;
        this.body = Objects.requireNonNull(body, "body");
        if (body.profileKind() != profileKind || body.schemaVersion() != BODY_SCHEMA_VERSION) {
            throw new IllegalArgumentException("Profile kind/body schema branch mismatch");
        }
        this.semanticHash = computeSemanticHash();
    }

    private ProfileSemanticEnvelopeV1(final ProfileKindV1 profileKind, final byte[] profileId, final long version,
                                      final ProfileSemanticBodyV1 body, final byte[] semanticHash) {
        this(profileKind, profileId, version, body);
        Bytes.requireLength(semanticHash, HASH_LENGTH, "semanticHash");
        if (!Bytes.constantTimeEquals(this.semanticHash, semanticHash)) {
            throw new IllegalArgumentException("Profile semantic hash mismatch");
        }
    }

    public ProfileKindV1 profileKind() {
        return profileKind;
    }

    public byte[] profileId() {
        return Bytes.copy(profileId);
    }

    public long version() {
        return version;
    }

    public ProfileSemanticBodyV1 body() {
        return body;
    }

    public byte[] semanticHash() {
        return Bytes.copy(semanticHash);
    }

    public ProfileRefV1 ref() {
        return new ProfileRefV1(profileId, version, semanticHash, profileKind);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, ENVELOPE_VERSION);
            CanonicalProtobuf.uint32(output, 2, profileKind.wireValue());
            CanonicalProtobuf.bytes(output, 3, profileId);
            CanonicalProtobuf.uint64(output, 4, version);
            CanonicalProtobuf.uint32(output, 5, BODY_SCHEMA_VERSION);
            CanonicalProtobuf.bytes(output, branchNumber(profileKind), body.canonicalBytes());
            CanonicalProtobuf.bytes(output, 20, semanticHash);
        });
    }

    public static ProfileSemanticEnvelopeV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(
                encoded, "ProfileSemanticEnvelopeV1");
        if (fields.size() != 7) {
            throw new IllegalArgumentException("ProfileSemanticEnvelopeV1 has an unexpected field count");
        }
        for (int index = 0; index < 5; index++) {
            if (fields.get(index).number() != index + 1) {
                throw new IllegalArgumentException("ProfileSemanticEnvelopeV1 field order mismatch");
            }
        }
        if (fields.get(6).number() != 20) {
            throw new IllegalArgumentException("ProfileSemanticEnvelopeV1 semantic hash field is missing");
        }
        if (QueryCodecSupport.uint(fields.get(0), 1) != ENVELOPE_VERSION
                || QueryCodecSupport.uint(fields.get(4), 5) != BODY_SCHEMA_VERSION) {
            throw new IllegalArgumentException("unsupported ProfileSemanticEnvelopeV1 version");
        }
        final ProfileKindV1 kind = ProfileKindV1.fromWire(QueryCodecSupport.uint(fields.get(1), 2));
        final int branch = branchNumber(kind);
        if (fields.get(5).number() != branch) {
            throw new IllegalArgumentException("Profile semantic branch does not match kind");
        }
        final ProfileSemanticBodyV1 body = decodeBody(kind, QueryCodecSupport.nested(fields.get(5), branch));
        final ProfileSemanticEnvelopeV1 result = new ProfileSemanticEnvelopeV1(kind,
                QueryCodecSupport.bytes(fields.get(2), 3), QueryCodecSupport.uint(fields.get(3), 4), body,
                QueryCodecSupport.fixed(fields.get(6), 20, HASH_LENGTH));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "ProfileSemanticEnvelopeV1");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof ProfileSemanticEnvelopeV1 that && profileKind == that.profileKind
                && version == that.version && Arrays.equals(profileId, that.profileId)
                && body.equals(that.body) && Arrays.equals(semanticHash, that.semanticHash);
    }

    @Override
    public int hashCode() {
        return Objects.hash(profileKind, Arrays.hashCode(profileId), version, body,
                Arrays.hashCode(semanticHash));
    }

    private byte[] computeSemanticHash() {
        return Bytes.sha256(Bytes.utf8(HASH_DOMAIN), Bytes.u16be(profileKind.wireValue()), Bytes.lp32(profileId),
                Bytes.u64be(version), Bytes.u32be(BODY_SCHEMA_VERSION), Bytes.lp32(body.canonicalBytes()));
    }

    private static ProfileSemanticBodyV1 decodeBody(final ProfileKindV1 kind, final byte[] encoded) {
        return switch (kind) {
            case DESTINATION -> DestinationProfileSemanticV1.decode(encoded);
            case DELIVERY_CAPABILITY -> DeliveryCapabilitySemanticV1.decode(encoded);
            case OBJECT_STORE -> ObjectStoreProfileSemanticV1.decode(encoded);
            case EVIDENCE_VERIFIER -> EvidenceVerifierProfileSemanticV1.decode(encoded);
        };
    }

    private static int branchNumber(final ProfileKindV1 kind) {
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
            throw new IllegalArgumentException("profileId is outside the V1 bound");
        }
        return Bytes.copy(value);
    }
}
