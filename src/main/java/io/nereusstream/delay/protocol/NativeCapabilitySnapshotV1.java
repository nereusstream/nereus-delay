package io.nereusstream.delay.protocol;

import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Signed, immutable authorization snapshot for the Pulsar AUTO_FAST branch.
 * Issuance-side guard and credential protection remain outside this codec.
 */
public final class NativeCapabilitySnapshotV1 {
    public static final int SNAPSHOT_VERSION = 1;
    public static final int HASH_LENGTH = 32;
    public static final int SIGNATURE_LENGTH = 64;

    private final ProfileRefV1 destination;
    private final ProfileRefV1 capability;
    private final PulsarBrokerResourceIdentityV1 target;
    private final int physicalPartition;
    private final byte[] resourceGuardAttestationSha256;
    private final long resourceGuardConfigGeneration;
    private final long credentialBindingGeneration;
    private final byte[] credentialBindingDigest;
    private final byte[] resolvedCredentialFingerprintDigest;
    private final byte[] sdkPrincipalScopeDigest;
    private final TrustedUtcIntervalEvidence issuedAt;
    private final long notAfterEpochMs;
    private final int issuerSigningKeyVersion;
    private final byte[] snapshotDigest;
    private final byte[] signature;

    private NativeCapabilitySnapshotV1(final ProfileRefV1 destination, final ProfileRefV1 capability,
                                       final PulsarBrokerResourceIdentityV1 target, final int physicalPartition,
                                       final byte[] resourceGuardAttestationSha256,
                                       final long resourceGuardConfigGeneration,
                                       final long credentialBindingGeneration, final byte[] credentialBindingDigest,
                                       final byte[] resolvedCredentialFingerprintDigest,
                                       final byte[] sdkPrincipalScopeDigest, final TrustedUtcIntervalEvidence issuedAt,
                                       final long notAfterEpochMs, final int issuerSigningKeyVersion,
                                       final byte[] snapshotDigest, final byte[] signature) {
        this.destination = Objects.requireNonNull(destination, "destination");
        if (destination.profileKind() != ProfileKindV1.DESTINATION) {
            throw new IllegalArgumentException("native snapshot destination must be a DESTINATION profile");
        }
        this.capability = Objects.requireNonNull(capability, "capability");
        if (capability.profileKind() != ProfileKindV1.DELIVERY_CAPABILITY) {
            throw new IllegalArgumentException("native snapshot capability must be a DELIVERY_CAPABILITY profile");
        }
        this.target = Objects.requireNonNull(target, "target");
        if (resourceGuardConfigGeneration <= 0 || credentialBindingGeneration == 0 || notAfterEpochMs < 0
                || notAfterEpochMs <= issuedAt(issuedAt).latestEpochMs()
                || issuerSigningKeyVersion == 0) {
            throw new IllegalArgumentException("invalid native capability snapshot numbers");
        }
        this.physicalPartition = physicalPartition;
        this.resourceGuardAttestationSha256 = fixed(resourceGuardAttestationSha256,
                "resourceGuardAttestationSha256");
        this.resourceGuardConfigGeneration = resourceGuardConfigGeneration;
        this.credentialBindingGeneration = credentialBindingGeneration;
        this.credentialBindingDigest = fixed(credentialBindingDigest, "credentialBindingDigest");
        this.resolvedCredentialFingerprintDigest = fixed(resolvedCredentialFingerprintDigest,
                "resolvedCredentialFingerprintDigest");
        this.sdkPrincipalScopeDigest = fixed(sdkPrincipalScopeDigest, "sdkPrincipalScopeDigest");
        this.issuedAt = issuedAt;
        this.notAfterEpochMs = notAfterEpochMs;
        this.issuerSigningKeyVersion = issuerSigningKeyVersion;
        this.snapshotDigest = fixed(snapshotDigest, "snapshotDigest");
        Bytes.requireLength(signature, SIGNATURE_LENGTH, "signature");
        this.signature = Bytes.copy(signature);
    }

    public static NativeCapabilitySnapshotV1 create(final ProfileRefV1 destination,
                                                     final ProfileRefV1 capability,
                                                     final PulsarBrokerResourceIdentityV1 target,
                                                     final int physicalPartition,
                                                     final byte[] resourceGuardAttestationSha256,
                                                     final long resourceGuardConfigGeneration,
                                                     final long credentialBindingGeneration,
                                                     final byte[] credentialBindingDigest,
                                                     final byte[] resolvedCredentialFingerprintDigest,
                                                     final byte[] sdkPrincipalScopeDigest,
                                                     final TrustedUtcIntervalEvidence issuedAt,
                                                     final long notAfterEpochMs,
                                                     final int issuerSigningKeyVersion,
                                                     final PrivateKey issuerKey) {
        Objects.requireNonNull(issuedAt, "issuedAt");
        final byte[] fields = canonicalFields(destination, capability, target, physicalPartition,
                resourceGuardAttestationSha256, resourceGuardConfigGeneration, credentialBindingGeneration,
                credentialBindingDigest, resolvedCredentialFingerprintDigest, sdkPrincipalScopeDigest, issuedAt,
                notAfterEpochMs, issuerSigningKeyVersion);
        final byte[] digest = Bytes.sha256(Bytes.utf8("nereus-delay-native-capability-snapshot-v1\0"), fields);
        final byte[] signature = sign(signatureDigest(digest, issuerSigningKeyVersion), issuerKey);
        return new NativeCapabilitySnapshotV1(destination, capability, target, physicalPartition,
                resourceGuardAttestationSha256, resourceGuardConfigGeneration, credentialBindingGeneration,
                credentialBindingDigest, resolvedCredentialFingerprintDigest, sdkPrincipalScopeDigest, issuedAt,
                notAfterEpochMs, issuerSigningKeyVersion, digest, signature);
    }

    public static NativeCapabilitySnapshotV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded,
                "NativeCapabilitySnapshotV1");
        QueryCodecSupport.requireNumbers(fields, new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
                "NativeCapabilitySnapshotV1");
        if (QueryCodecSupport.uint(fields.get(0), 1) != SNAPSHOT_VERSION) {
            throw new IllegalArgumentException("unsupported NativeCapabilitySnapshotV1 version");
        }
        final ProfileRefV1 destination = ProfileRefV1.decode(QueryCodecSupport.nested(fields.get(1), 2));
        final ProfileRefV1 capability = ProfileRefV1.decode(QueryCodecSupport.nested(fields.get(2), 3));
        final PulsarBrokerResourceIdentityV1 target = PulsarBrokerResourceIdentityV1.decode(
                QueryCodecSupport.nested(fields.get(3), 4));
        final int partition = QueryCodecSupport.uint32Bits(fields.get(4), 5);
        final byte[] guardAttestation = QueryCodecSupport.fixed(fields.get(5), 6, HASH_LENGTH);
        final long guardGeneration = QueryCodecSupport.uint(fields.get(6), 7);
        final long bindingGeneration = QueryCodecSupport.uint(fields.get(7), 8);
        final byte[] bindingDigest = QueryCodecSupport.fixed(fields.get(8), 9, HASH_LENGTH);
        final byte[] fingerprint = QueryCodecSupport.fixed(fields.get(9), 10, HASH_LENGTH);
        final byte[] principalScope = QueryCodecSupport.fixed(fields.get(10), 11, HASH_LENGTH);
        final TrustedUtcIntervalEvidence issuedAt = TrustedUtcIntervalEvidence.decode(
                QueryCodecSupport.nested(fields.get(11), 12));
        final long notAfter = QueryCodecSupport.uint(fields.get(12), 13);
        final int keyVersion = QueryCodecSupport.uint32Bits(fields.get(13), 14);
        final byte[] digest = QueryCodecSupport.fixed(fields.get(14), 15, HASH_LENGTH);
        final byte[] signature = QueryCodecSupport.fixed(fields.get(15), 16, SIGNATURE_LENGTH);
        final byte[] expectedDigest = Bytes.sha256(Bytes.utf8("nereus-delay-native-capability-snapshot-v1\0"),
                canonicalFields(destination, capability, target, partition, guardAttestation, guardGeneration,
                        bindingGeneration, bindingDigest, fingerprint, principalScope, issuedAt, notAfter,
                        keyVersion));
        if (!Bytes.constantTimeEquals(digest, expectedDigest)) {
            throw new IllegalArgumentException("NativeCapabilitySnapshot digest mismatch");
        }
        final NativeCapabilitySnapshotV1 result = new NativeCapabilitySnapshotV1(destination, capability, target,
                partition, guardAttestation, guardGeneration, bindingGeneration, bindingDigest, fingerprint,
                principalScope, issuedAt, notAfter, keyVersion, digest, signature);
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "NativeCapabilitySnapshotV1");
        return result;
    }

    public ProfileRefV1 destination() {
        return destination;
    }

    public ProfileRefV1 capability() {
        return capability;
    }

    public PulsarBrokerResourceIdentityV1 target() {
        return target;
    }

    public int physicalPartition() {
        return physicalPartition;
    }

    public byte[] resourceGuardAttestationSha256() {
        return Bytes.copy(resourceGuardAttestationSha256);
    }

    public long resourceGuardConfigGeneration() {
        return resourceGuardConfigGeneration;
    }

    public long credentialBindingGeneration() {
        return credentialBindingGeneration;
    }

    public byte[] credentialBindingDigest() {
        return Bytes.copy(credentialBindingDigest);
    }

    public byte[] resolvedCredentialFingerprintDigest() {
        return Bytes.copy(resolvedCredentialFingerprintDigest);
    }

    public byte[] sdkPrincipalScopeDigest() {
        return Bytes.copy(sdkPrincipalScopeDigest);
    }

    public TrustedUtcIntervalEvidence issuedAt() {
        return issuedAt;
    }

    public long notAfterEpochMs() {
        return notAfterEpochMs;
    }

    public int issuerSigningKeyVersion() {
        return issuerSigningKeyVersion;
    }

    public byte[] snapshotDigest() {
        return Bytes.copy(snapshotDigest);
    }

    public byte[] signature() {
        return Bytes.copy(signature);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            writeFields(output);
            CanonicalProtobuf.bytes(output, 15, snapshotDigest);
            CanonicalProtobuf.bytes(output, 16, signature);
        });
    }

    public boolean verifySignature(final PublicKey issuerKey) {
        Objects.requireNonNull(issuerKey, "issuerKey");
        try {
            final Signature verifier = Signature.getInstance("Ed25519");
            verifier.initVerify(issuerKey);
            verifier.update(signatureDigest(snapshotDigest, issuerSigningKeyVersion));
            return verifier.verify(signature);
        } catch (GeneralSecurityException exception) {
            throw new IllegalStateException("Ed25519 verification is unavailable", exception);
        }
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof NativeCapabilitySnapshotV1 that)) {
            return false;
        }
        return physicalPartition == that.physicalPartition
                && resourceGuardConfigGeneration == that.resourceGuardConfigGeneration
                && credentialBindingGeneration == that.credentialBindingGeneration
                && notAfterEpochMs == that.notAfterEpochMs
                && issuerSigningKeyVersion == that.issuerSigningKeyVersion
                && destination.equals(that.destination) && capability.equals(that.capability)
                && target.equals(that.target)
                && Arrays.equals(resourceGuardAttestationSha256, that.resourceGuardAttestationSha256)
                && Arrays.equals(credentialBindingDigest, that.credentialBindingDigest)
                && Arrays.equals(resolvedCredentialFingerprintDigest, that.resolvedCredentialFingerprintDigest)
                && Arrays.equals(sdkPrincipalScopeDigest, that.sdkPrincipalScopeDigest)
                && Arrays.equals(issuedAt.canonicalBytes(), that.issuedAt.canonicalBytes())
                && Arrays.equals(snapshotDigest, that.snapshotDigest) && Arrays.equals(signature, that.signature);
    }

    @Override
    public int hashCode() {
        return Objects.hash(destination, capability, target, physicalPartition,
                Arrays.hashCode(resourceGuardAttestationSha256), resourceGuardConfigGeneration,
                credentialBindingGeneration, Arrays.hashCode(credentialBindingDigest),
                Arrays.hashCode(resolvedCredentialFingerprintDigest), Arrays.hashCode(sdkPrincipalScopeDigest),
                Arrays.hashCode(issuedAt.canonicalBytes()), notAfterEpochMs, issuerSigningKeyVersion,
                Arrays.hashCode(snapshotDigest), Arrays.hashCode(signature));
    }

    private void writeFields(final java.io.ByteArrayOutputStream output) {
        CanonicalProtobuf.uint32(output, 1, SNAPSHOT_VERSION);
        CanonicalProtobuf.bytes(output, 2, destination.canonicalBytes());
        CanonicalProtobuf.bytes(output, 3, capability.canonicalBytes());
        CanonicalProtobuf.bytes(output, 4, target.canonicalBytes());
        CanonicalProtobuf.uint32Bits(output, 5, physicalPartition);
        CanonicalProtobuf.bytes(output, 6, resourceGuardAttestationSha256);
        CanonicalProtobuf.uint64(output, 7, resourceGuardConfigGeneration);
        CanonicalProtobuf.uint64Bits(output, 8, credentialBindingGeneration);
        CanonicalProtobuf.bytes(output, 9, credentialBindingDigest);
        CanonicalProtobuf.bytes(output, 10, resolvedCredentialFingerprintDigest);
        CanonicalProtobuf.bytes(output, 11, sdkPrincipalScopeDigest);
        CanonicalProtobuf.bytes(output, 12, issuedAt.canonicalBytes());
        CanonicalProtobuf.int64(output, 13, notAfterEpochMs);
        CanonicalProtobuf.uint32Bits(output, 14, issuerSigningKeyVersion);
    }

    private static byte[] canonicalFields(final ProfileRefV1 destination, final ProfileRefV1 capability,
                                           final PulsarBrokerResourceIdentityV1 target, final int physicalPartition,
                                           final byte[] resourceGuardAttestationSha256,
                                           final long resourceGuardConfigGeneration,
                                           final long credentialBindingGeneration, final byte[] credentialBindingDigest,
                                           final byte[] resolvedCredentialFingerprintDigest,
                                           final byte[] sdkPrincipalScopeDigest,
                                           final TrustedUtcIntervalEvidence issuedAt, final long notAfterEpochMs,
                                           final int issuerSigningKeyVersion) {
        final NativeCapabilitySnapshotV1 fields = new NativeCapabilitySnapshotV1(destination, capability, target,
                physicalPartition, resourceGuardAttestationSha256, resourceGuardConfigGeneration,
                credentialBindingGeneration, credentialBindingDigest, resolvedCredentialFingerprintDigest,
                sdkPrincipalScopeDigest, issuedAt, notAfterEpochMs, issuerSigningKeyVersion, new byte[HASH_LENGTH],
                new byte[SIGNATURE_LENGTH]);
        return CanonicalProtobuf.message(fields::writeFields);
    }

    private static byte[] signatureDigest(final byte[] snapshotDigest, final int signingKeyVersion) {
        return Bytes.sha256(Bytes.utf8("nereus-delay-native-capability-snapshot-signature-v1\0"), snapshotDigest,
                Bytes.u32beBits(signingKeyVersion));
    }

    private static byte[] sign(final byte[] digest, final PrivateKey issuerKey) {
        Objects.requireNonNull(issuerKey, "issuerKey");
        try {
            final Signature signer = Signature.getInstance("Ed25519");
            signer.initSign(issuerKey);
            signer.update(digest);
            return signer.sign();
        } catch (GeneralSecurityException exception) {
            throw new IllegalStateException("Ed25519 signing is unavailable", exception);
        }
    }

    private static byte[] fixed(final byte[] value, final String name) {
        Bytes.requireLength(value, HASH_LENGTH, name);
        return Bytes.copy(value);
    }

    private static TrustedUtcIntervalEvidence issuedAt(final TrustedUtcIntervalEvidence value) {
        return Objects.requireNonNull(value, "issuedAt");
    }
}
