package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Registry §6.3 request for checked rotation of an equivalent credential reference. */
public final class RotateEquivalentSecretRequestV1 implements ControlOperationRequestBranchV1 {
    public static final int ROTATION_PROTOCOL_VERSION = CredentialBindingV1.BINDING_PROTOCOL_VERSION;
    public static final int HASH_LENGTH = CredentialBindingV1.HASH_LENGTH;

    private final ProfileRefV1 profile;
    private final long expectedSecretGeneration;
    private final long newSecretGeneration;
    private final byte[] newSecretReference;
    private final byte[] newSecretReferenceSha256;
    private final CredentialEquivalenceAttestationV1 equivalenceAttestation;
    private final byte[] expectedBindingDigest;
    private final long expectedBindingHeadRevision;

    public RotateEquivalentSecretRequestV1(
            final ProfileRefV1 profile,
            final long expectedSecretGeneration,
            final long newSecretGeneration,
            final byte[] newSecretReference,
            final byte[] newSecretReferenceSha256,
            final CredentialEquivalenceAttestationV1 equivalenceAttestation,
            final byte[] expectedBindingDigest,
            final long expectedBindingHeadRevision) {
        this.profile = requireBindableProfile(profile);
        this.expectedSecretGeneration = nonZero(expectedSecretGeneration, "expectedSecretGeneration");
        this.newSecretGeneration = nonZero(newSecretGeneration, "newSecretGeneration");
        if (this.expectedSecretGeneration == -1L) {
            throw new IllegalArgumentException("secret generation cannot be incremented");
        }
        final long expectedSuccessor = this.expectedSecretGeneration + 1;
        if (this.newSecretGeneration != expectedSuccessor) {
            throw new IllegalArgumentException("newSecretGeneration must increment expectedSecretGeneration by one");
        }
        this.newSecretReference = boundedNonEmpty(
                newSecretReference, CredentialBindingV1.MAX_SECRET_REFERENCE_BYTES, "newSecretReference");
        this.newSecretReferenceSha256 = fixed(newSecretReferenceSha256, "newSecretReferenceSha256");
        if (!Bytes.constantTimeEquals(this.newSecretReferenceSha256, Bytes.sha256(this.newSecretReference))) {
            throw new IllegalArgumentException("newSecretReferenceSha256 does not match newSecretReference");
        }
        this.equivalenceAttestation = Objects.requireNonNull(equivalenceAttestation, "equivalenceAttestation");
        this.equivalenceAttestation.requireCandidate(
                this.profile, this.newSecretGeneration, this.newSecretReferenceSha256);
        this.expectedBindingDigest = fixed(expectedBindingDigest, "expectedBindingDigest");
        this.expectedBindingHeadRevision = nonZero(expectedBindingHeadRevision, "expectedBindingHeadRevision");
    }

    public ProfileRefV1 profile() {
        return profile;
    }

    public long expectedSecretGeneration() {
        return expectedSecretGeneration;
    }

    public long newSecretGeneration() {
        return newSecretGeneration;
    }

    /** Private control-plane reference; callers must not project it publicly. */
    public byte[] newSecretReference() {
        return Bytes.copy(newSecretReference);
    }

    public byte[] newSecretReferenceSha256() {
        return Bytes.copy(newSecretReferenceSha256);
    }

    public CredentialEquivalenceAttestationV1 equivalenceAttestation() {
        return equivalenceAttestation;
    }

    public byte[] expectedBindingDigest() {
        return Bytes.copy(expectedBindingDigest);
    }

    public long expectedBindingHeadRevision() {
        return expectedBindingHeadRevision;
    }

    /** Derives the exact immutable binding that the control operation will create. */
    public CredentialBindingV1 newBinding() {
        return CredentialBindingV1.create(profile, newSecretGeneration, newSecretReference, equivalenceAttestation);
    }

    @Override
    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, profile.canonicalBytes());
            CanonicalProtobuf.uint64Bits(output, 2, expectedSecretGeneration);
            CanonicalProtobuf.uint64Bits(output, 3, newSecretGeneration);
            CanonicalProtobuf.bytes(output, 4, newSecretReference);
            CanonicalProtobuf.bytes(output, 5, newSecretReferenceSha256);
            CanonicalProtobuf.bytes(output, 6, equivalenceAttestation.canonicalBytes());
            CanonicalProtobuf.bytes(output, 7, expectedBindingDigest);
            CanonicalProtobuf.uint64Bits(output, 8, expectedBindingHeadRevision);
        });
    }

    public static RotateEquivalentSecretRequestV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields =
                QueryCodecSupport.read(encoded, "RotateEquivalentSecretRequestV1");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3, 4, 5, 6, 7, 8}, "RotateEquivalentSecretRequestV1");
        final RotateEquivalentSecretRequestV1 result = new RotateEquivalentSecretRequestV1(
                ProfileRefV1.decode(QueryCodecSupport.nested(fields.get(0), 1)),
                QueryCodecSupport.uint(fields.get(1), 2),
                QueryCodecSupport.uint(fields.get(2), 3),
                QueryCodecSupport.bytes(fields.get(3), 4),
                QueryCodecSupport.fixed(fields.get(4), 5, HASH_LENGTH),
                CredentialEquivalenceAttestationV1.decode(QueryCodecSupport.nested(fields.get(5), 6)),
                QueryCodecSupport.fixed(fields.get(6), 7, HASH_LENGTH),
                QueryCodecSupport.uint64Bits(fields.get(7), 8));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "RotateEquivalentSecretRequestV1");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof RotateEquivalentSecretRequestV1 that
                && expectedSecretGeneration == that.expectedSecretGeneration
                && newSecretGeneration == that.newSecretGeneration
                && expectedBindingHeadRevision == that.expectedBindingHeadRevision
                && profile.equals(that.profile)
                && Arrays.equals(newSecretReference, that.newSecretReference)
                && Arrays.equals(newSecretReferenceSha256, that.newSecretReferenceSha256)
                && equivalenceAttestation.equals(that.equivalenceAttestation)
                && Arrays.equals(expectedBindingDigest, that.expectedBindingDigest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                profile,
                expectedSecretGeneration,
                newSecretGeneration,
                Arrays.hashCode(newSecretReference),
                Arrays.hashCode(newSecretReferenceSha256),
                equivalenceAttestation,
                Arrays.hashCode(expectedBindingDigest),
                expectedBindingHeadRevision);
    }

    private static ProfileRefV1 requireBindableProfile(final ProfileRefV1 value) {
        final ProfileRefV1 profile = Objects.requireNonNull(value, "profile");
        if (profile.profileKind() != ProfileKindV1.DESTINATION && profile.profileKind() != ProfileKindV1.OBJECT_STORE) {
            throw new IllegalArgumentException("secret rotation profile must be DESTINATION or OBJECT_STORE");
        }
        return profile;
    }

    private static byte[] boundedNonEmpty(final byte[] value, final int maximum, final String name) {
        Objects.requireNonNull(value, name);
        if (value.length == 0 || value.length > maximum) {
            throw new IllegalArgumentException(name + " must be non-empty and at most " + maximum + " bytes");
        }
        return Bytes.copy(value);
    }

    private static byte[] fixed(final byte[] value, final String name) {
        Bytes.requireLength(value, HASH_LENGTH, name);
        return Bytes.copy(value);
    }

    private static long nonZero(final long value, final String name) {
        if (value == 0) {
            throw new IllegalArgumentException(name + " must be non-zero");
        }
        return value;
    }
}
