package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Immutable private control-plane binding for one exact credential generation. */
public final class CredentialBinding {
    public static final int HASH_LENGTH = 32;
    public static final int BINDING_PROTOCOL_VERSION = 1;
    public static final int MAX_SECRET_REFERENCE_BYTES = 4096;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-credential-binding\0");

    private final ProfileRef profile;
    private final long secretGeneration;
    private final byte[] secretReference;
    private final byte[] secretReferenceSha256;
    private final CredentialEquivalenceAttestation equivalenceAttestation;
    private final byte[] bindingDigest;

    public CredentialBinding(
            final ProfileRef profile,
            final long secretGeneration,
            final byte[] secretReference,
            final byte[] secretReferenceSha256,
            final CredentialEquivalenceAttestation equivalenceAttestation,
            final int bindingProtocolVersion,
            final byte[] bindingDigest) {
        this.profile = requireBindableProfile(profile);
        this.secretGeneration = nonZero(secretGeneration, "secretGeneration");
        this.secretReference = boundedNonEmpty(secretReference, MAX_SECRET_REFERENCE_BYTES, "secretReference");
        this.secretReferenceSha256 = fixed(secretReferenceSha256, "secretReferenceSha256");
        if (!Bytes.constantTimeEquals(this.secretReferenceSha256, Bytes.sha256(this.secretReference))) {
            throw new IllegalArgumentException("secretReferenceSha256 does not match secretReference");
        }
        this.equivalenceAttestation = Objects.requireNonNull(equivalenceAttestation, "equivalenceAttestation");
        this.equivalenceAttestation.requireCandidate(this.profile, this.secretGeneration, this.secretReferenceSha256);
        if (bindingProtocolVersion != BINDING_PROTOCOL_VERSION) {
            throw new IllegalArgumentException("unsupported credential binding protocol version");
        }
        this.bindingDigest = fixed(bindingDigest, "bindingDigest");
        if (!Bytes.constantTimeEquals(this.bindingDigest, digestForFields())) {
            throw new IllegalArgumentException("CredentialBinding digest mismatch");
        }
    }

    /** Creates an immutable binding and derives its reference hash and binding digest. */
    public static CredentialBinding create(
            final ProfileRef profile,
            final long secretGeneration,
            final byte[] secretReference,
            final CredentialEquivalenceAttestation equivalenceAttestation) {
        final byte[] reference = boundedNonEmpty(secretReference, MAX_SECRET_REFERENCE_BYTES, "secretReference");
        final byte[] referenceHash = Bytes.sha256(reference);
        final byte[] digest =
                digestForFields(profile, secretGeneration, reference, referenceHash, equivalenceAttestation);
        return new CredentialBinding(
                profile,
                secretGeneration,
                reference,
                referenceHash,
                equivalenceAttestation,
                BINDING_PROTOCOL_VERSION,
                digest);
    }

    public static CredentialBinding decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "CredentialBinding");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3, 4, 5, 6, 7}, "CredentialBinding");
        final ProfileRef profile = ProfileRef.decode(QueryCodecSupport.nested(fields.get(0), 1));
        final long generation = nonZero(QueryCodecSupport.uint(fields.get(1), 2), "secretGeneration");
        final byte[] reference = QueryCodecSupport.bytes(fields.get(2), 3);
        final byte[] referenceHash = QueryCodecSupport.fixed(fields.get(3), 4, HASH_LENGTH);
        final CredentialEquivalenceAttestation attestation =
                CredentialEquivalenceAttestation.decode(QueryCodecSupport.nested(fields.get(4), 5));
        final int protocolVersion = QueryCodecSupport.uint32(fields.get(5), 6);
        final byte[] digest = QueryCodecSupport.fixed(fields.get(6), 7, HASH_LENGTH);
        final CredentialBinding result = new CredentialBinding(
                profile, generation, reference, referenceHash, attestation, protocolVersion, digest);
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "CredentialBinding");
        return result;
    }

    public ProfileRef profile() {
        return profile;
    }

    public long secretGeneration() {
        return secretGeneration;
    }

    /** Private control-plane reference; callers must not project this to public surfaces. */
    public byte[] secretReference() {
        return Bytes.copy(secretReference);
    }

    public byte[] secretReferenceSha256() {
        return Bytes.copy(secretReferenceSha256);
    }

    public CredentialEquivalenceAttestation equivalenceAttestation() {
        return equivalenceAttestation;
    }

    public int bindingProtocolVersion() {
        return BINDING_PROTOCOL_VERSION;
    }

    public byte[] bindingDigest() {
        return Bytes.copy(bindingDigest);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, profile.canonicalBytes());
            CanonicalProtobuf.uint64Bits(output, 2, secretGeneration);
            CanonicalProtobuf.bytes(output, 3, secretReference);
            CanonicalProtobuf.bytes(output, 4, secretReferenceSha256);
            CanonicalProtobuf.bytes(output, 5, equivalenceAttestation.canonicalBytes());
            CanonicalProtobuf.uint32(output, 6, BINDING_PROTOCOL_VERSION);
            CanonicalProtobuf.bytes(output, 7, bindingDigest);
        });
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof CredentialBinding that
                && secretGeneration == that.secretGeneration
                && profile.equals(that.profile)
                && Arrays.equals(secretReference, that.secretReference)
                && Arrays.equals(secretReferenceSha256, that.secretReferenceSha256)
                && equivalenceAttestation.equals(that.equivalenceAttestation)
                && Arrays.equals(bindingDigest, that.bindingDigest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                profile,
                secretGeneration,
                Arrays.hashCode(secretReference),
                Arrays.hashCode(secretReferenceSha256),
                equivalenceAttestation,
                Arrays.hashCode(bindingDigest));
    }

    private byte[] digestForFields() {
        return digestForFields(
                profile, secretGeneration, secretReference, secretReferenceSha256, equivalenceAttestation);
    }

    private static byte[] digestForFields(
            final ProfileRef profile,
            final long secretGeneration,
            final byte[] secretReference,
            final byte[] secretReferenceSha256,
            final CredentialEquivalenceAttestation equivalenceAttestation) {
        final byte[] fields = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, profile.canonicalBytes());
            CanonicalProtobuf.uint64Bits(output, 2, secretGeneration);
            CanonicalProtobuf.bytes(output, 3, secretReference);
            CanonicalProtobuf.bytes(output, 4, secretReferenceSha256);
            CanonicalProtobuf.bytes(output, 5, equivalenceAttestation.canonicalBytes());
            CanonicalProtobuf.uint32(output, 6, BINDING_PROTOCOL_VERSION);
        });
        return Bytes.sha256(DIGEST_DOMAIN, Bytes.lp32(fields));
    }

    private static ProfileRef requireBindableProfile(final ProfileRef value) {
        final ProfileRef profile = Objects.requireNonNull(value, "profile");
        if (profile.profileKind() != ProfileKind.DESTINATION && profile.profileKind() != ProfileKind.OBJECT_STORE) {
            throw new IllegalArgumentException("credential binding profile must be DESTINATION or OBJECT_STORE");
        }
        return profile;
    }

    private static byte[] fixed(final byte[] value, final String name) {
        Bytes.requireLength(value, HASH_LENGTH, name);
        return Bytes.copy(value);
    }

    private static byte[] boundedNonEmpty(final byte[] value, final int maximum, final String name) {
        Objects.requireNonNull(value, name);
        if (value.length == 0 || value.length > maximum) {
            throw new IllegalArgumentException(name + " must be non-empty and at most " + maximum + " bytes");
        }
        return Bytes.copy(value);
    }

    private static long nonZero(final long value, final String name) {
        if (value == 0) {
            throw new IllegalArgumentException(name + " must be non-zero");
        }
        return value;
    }
}
